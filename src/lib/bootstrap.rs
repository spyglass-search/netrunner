use async_recursion::async_recursion;
use bytes::Buf;
use dashmap::DashSet;
use feedfinder::FeedType;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use governor::Quota;
use governor::RateLimiter;
use nonzero_ext::nonzero;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use reqwest::Error;
use reqwest::Response;
use rss::Channel;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, io::Read};
use tar::Archive;
use tar::Builder;
use tar::Header;
use tokio::task::JoinSet;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use url::Url;

use super::cdx;
use super::crawler::RateLimit;
use crate::{cache::CrawlCache, crawler::http_client, site::SiteInfo};

// Sitemap Cache directory
const SITE_CACHE_DIR: &str = "sitemaps/";

#[derive(Clone)]
pub struct Bootstrapper {
    client: Client,
    // Urls that need to be processed through a cdx index.
    cdx_queue: HashSet<String>,
}

impl Default for Bootstrapper {
    fn default() -> Self {
        Self::new(&http_client())
    }
}

impl Bootstrapper {
    pub fn new(client: &Client) -> Self {
        Self {
            client: client.clone(),
            cdx_queue: HashSet::new(),
        }
    }

    /// Generates a sitemap cache for the specified domain
    pub async fn cache_sitemaps(&mut self, domain: &str) -> anyhow::Result<PathBuf> {
        let mut cache = CrawlCache::new();
        let domain_url = format!("http://{domain}/");
        if !cache.process_url(&domain_url).await {
            self.cdx_queue.insert(domain_url);
        }

        let site_cache = get_cache_location(domain);
        let tar_gz = std::fs::File::create(site_cache.clone()).unwrap();

        let tar = GzEncoder::new(tar_gz, Compression::default());
        let mut builder = Builder::new(tar);

        self.cache_all_sitemaps(&cache, &mut builder).await;
        if let Err(error) = builder.finish() {
            log::error!("Error closing archive {:?}", error);
        }
        Ok(site_cache)
    }

    pub async fn find_urls(&mut self, lens: &LensConfig) -> anyhow::Result<Vec<String>> {
        // Urls gathered from sitemaps + cdx processing.
        let mut to_crawl: DashSet<String> = DashSet::new();
        let mut cache = CrawlCache::new();

        log::info!("Loading lens rules");
        let filters = lens.into_regexes();
        let allowed = RegexSetBuilder::new(filters.allowed)
            .size_limit(100_000_000)
            .build()?;

        let skipped = RegexSetBuilder::new(filters.skipped)
            .size_limit(100_000_000)
            .build()?;

        // ------------------------------------------------------------------------
        // Second, we fetch robots & sitemaps from the domains/urls represented by the lens
        // ------------------------------------------------------------------------
        log::info!("Fetching robots.txt & sitemaps.xml");
        for domain in lens.domains.iter() {
            let domain_url = format!("http://{domain}/");
            to_crawl.insert(domain_url.to_string());
            // If there are no sitemaps, add to CDX queue
            if !cache.process_url(&domain_url).await {
                self.cdx_queue.insert(domain_url);
            }
        }

        for prefix in lens.urls.iter() {
            let url = if prefix.ends_with('$') {
                // Remove the '$' suffix and add to the crawl queue
                let url = prefix.trim_end_matches('$');
                to_crawl.insert(url.to_string());
                continue;
            } else {
                to_crawl.insert(prefix.clone());
                prefix
            };

            // If there is no sitemaps in the robots, add to CDX queue
            if !cache.process_url(url).await {
                self.cdx_queue.insert(url.to_owned());
            }
        }

        // ------------------------------------------------------------------------
        // Third, either read the sitemaps or pull data from a CDX to determine which
        // urls to crawl.
        // ------------------------------------------------------------------------
        self.process_sitemaps_and_cdx(&cache, &mut to_crawl, &allowed, &skipped)
            .await;

        // Clear CDX queue after fetching URLs.
        self.cdx_queue.clear();
        // Ignore invalid URLs and remove fragments from URLs (e.g. http://example.com#Title
        // is considered the same as http://example.com)
        let cleaned: HashSet<String> = to_crawl
            .iter()
            .filter_map(|url| {
                if let Ok(mut url) = Url::parse(&url) {
                    url.set_fragment(None);
                    Some(url.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(cleaned.into_iter().collect())
    }

    // Helper method used to cache all the sitemaps for a single domain
    async fn cache_all_sitemaps(
        &self,
        cache: &CrawlCache,
        builder: &mut Builder<GzEncoder<std::fs::File>>,
    ) {
        let mut sitemaps = Vec::new();

        for info in cache.cache.values().flatten() {
            // Grab list of sitemaps
            if let Some(robot) = &info.robot {
                if !robot.sitemaps.is_empty() {
                    for sitemap in &robot.sitemaps {
                        sitemaps.push(sitemap.clone());
                    }
                }
            }
        }

        if !sitemaps.is_empty() {
            log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
            let quota = Quota::per_second(nonzero!(2u32));
            let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

            for sitemap in sitemaps {
                let lim = lim.clone();
                if let Ok(url) = Url::parse(&sitemap) {
                    cache_sitemap(lim.clone(), &url, builder).await
                }
            }
        }
    }

    async fn process_sitemaps_and_cdx(
        &self,
        cache: &CrawlCache,
        to_crawl: &mut DashSet<String>,
        allowed: &RegexSet,
        skipped: &RegexSet,
    ) {
        // Crawl sitemaps & rss feeds
        let mut handles = JoinSet::new();
        let mut sitemaps = Vec::new();

        for info in cache.cache.values().flatten() {
            // Fetch links from RSS feeds
            to_crawl.extend(fetch_rss(info).await);
            // Grab list of sitemaps
            if let Some(robot) = &info.robot {
                if !robot.sitemaps.is_empty() {
                    for sitemap in &robot.sitemaps {
                        sitemaps.push(sitemap.clone());
                    }
                }
            }
        }

        if !sitemaps.is_empty() {
            log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
            let quota = Quota::per_second(nonzero!(2u32));
            let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

            for sitemap in sitemaps {
                let allowed = allowed.clone();
                let skipped = skipped.clone();
                let lim = lim.clone();
                if let Ok(url) = Url::parse(&sitemap) {
                    handles.spawn(async move {
                        fetch_sitemap(lim.clone(), &url, &allowed, &skipped).await
                    });
                }
            }

            while let Some(Ok(urls)) = handles.join_next().await {
                to_crawl.extend(urls);
            }
        }

        // Process any URLs in the cdx queue
        for prefix in self.cdx_queue.iter() {
            let mut resume_key = None;
            log::debug!("fetching cdx for: {}", prefix);
            while let Ok((urls, resume)) =
                cdx::fetch_cdx(&self.client, prefix, 1000, resume_key.clone()).await
            {
                let filtered = urls
                    .into_iter()
                    .filter(|url| {
                        if allowed.is_match(url) && !skipped.is_match(url) {
                            return true;
                        }

                        false
                    })
                    .collect::<Vec<String>>();

                log::info!("found {} urls", filtered.len());
                to_crawl.extend(filtered);

                if resume.is_none() {
                    break;
                }

                resume_key = resume;
            }
        }
    }
}

async fn fetch_rss(info: &SiteInfo) -> Vec<String> {
    let mut feed_urls: Vec<String> = Vec::new();

    for feed in &info.feeds {
        match feed.feed_type() {
            FeedType::Atom | FeedType::Rss => {
                if let Ok(resp) = reqwest::get(feed.url().to_string()).await {
                    if let Ok(content) = resp.bytes().await {
                        if let Ok(channel) = Channel::read_from(&content[..]) {
                            for item in channel.items {
                                if let Some(link) = item.link {
                                    feed_urls.push(link);
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }

    feed_urls
}

/// Fetch, parse and cache a sitemap file
#[async_recursion]
async fn cache_sitemap(
    limiter: Arc<RateLimit>,
    sitemap_url: &Url,
    builder: &mut Builder<GzEncoder<std::fs::File>>,
) {
    let response = get_sitemap(sitemap_url, &limiter).await;
    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                let sitemap_url_str = sitemap_url.to_string();
                let mut buf = String::new();
                // Decode gzipped files. Doesn't work automatically if they were
                // gzipped before uploading it to their destination.
                if sitemap_url_str.ends_with(".gz") {
                    if let Ok(text) = resp.bytes().await {
                        let mut decoder = GzDecoder::new(text.reader());
                        decoder.read_to_string(&mut buf).unwrap();
                    }
                } else if let Ok(text) = resp.text().await {
                    buf = text.replace('\u{feff}', "");
                }

                log::debug!("Adding sitemap to cache {:?}", sitemap_url);
                let mut header = Header::new_gnu();
                header.set_size(buf.as_bytes().len().try_into().unwrap());
                header.set_cksum();
                if let Err(error) = builder.append_data(
                    &mut header,
                    sitemap_url.path().strip_prefix('/').unwrap(),
                    buf.as_bytes(),
                ) {
                    log::error!("Error {:?}", error);
                }

                let parser = SiteMapReader::new(buf.as_bytes());
                let mut sitemaps = Vec::new();
                for entity in parser {
                    match entity {
                        SiteMapEntity::Url(_url_entry) => {}
                        SiteMapEntity::SiteMap(sitemap_entry) => {
                            if let Some(loc) = sitemap_entry.loc.get_url() {
                                sitemaps.push(loc.to_string());
                            }
                        }
                        _ => {}
                    }
                }

                if !sitemaps.is_empty() {
                    log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
                    for sitemap in sitemaps {
                        let limiter = limiter.clone();
                        if let Ok(url) = Url::parse(&sitemap) {
                            cache_sitemap(limiter.clone(), &url, builder).await;
                        }
                    }
                }
            } else {
                log::debug!("error fetching sitemap: {:?}", resp.error_for_status_ref());
            }
        }
        Err(err) => log::error!("{:?}", err),
    }
}

/// Fetch and parse a sitemap file
#[async_recursion]
async fn fetch_sitemap(
    limiter: Arc<RateLimit>,
    sitemap_url: &Url,
    allowed: &RegexSet,
    skipped: &RegexSet,
) -> HashSet<String> {
    let mut urls: HashSet<String> = HashSet::new();
    match get_cached_sitemap(sitemap_url) {
        Some(sitemap_str) => {
            process_site_map(&limiter, sitemap_str, allowed, skipped, &mut urls).await;
        }
        None => {
            let response = get_sitemap(sitemap_url, &limiter).await;
            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        let sitemap_url_str = sitemap_url.to_string();
                        let mut buf = String::new();
                        // Decode gzipped files. Doesn't work automatically if they were
                        // gzipped before uploading it to their destination.
                        if sitemap_url_str.ends_with(".gz") {
                            if let Ok(text) = resp.bytes().await {
                                let mut decoder = GzDecoder::new(text.reader());
                                decoder.read_to_string(&mut buf).unwrap();
                            }
                        } else if let Ok(text) = resp.text().await {
                            buf = text.replace('\u{feff}', "");
                        }

                        // cache_sitemap(sitemap_url, &buf);
                        process_site_map(&limiter, buf, allowed, skipped, &mut urls).await;
                    } else {
                        log::debug!("error fetching sitemap: {:?}", resp.error_for_status_ref());
                    }
                }
                Err(err) => log::error!("{:?}", err),
            }
        }
    }

    if !urls.is_empty() {
        log::info!("found {} urls for {}", urls.len(), sitemap_url);
    }

    urls
}

// Helper method used to process a sitemap
async fn process_site_map(
    limiter: &Arc<RateLimit>,
    sitemap: String,
    allowed: &RegexSet,
    skipped: &RegexSet,
    urls: &mut HashSet<String>,
) {
    let parser = SiteMapReader::new(sitemap.as_bytes());
    let mut sitemaps = Vec::new();
    for entity in parser {
        match entity {
            SiteMapEntity::Url(url_entry) => {
                if let Some(loc) = url_entry.loc.get_url() {
                    let url = loc.to_string();
                    if allowed.is_match(&url) && !skipped.is_match(&url) {
                        urls.insert(url);
                    }
                }
            }
            SiteMapEntity::SiteMap(sitemap_entry) => {
                if let Some(loc) = sitemap_entry.loc.get_url() {
                    sitemaps.push(loc.to_string());
                }
            }
            _ => {}
        }
    }

    if !sitemaps.is_empty() {
        log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
        for sitemap_chunk in sitemaps.chunks(10) {
            let mut set = JoinSet::new();
            for sitemap in sitemap_chunk {
                let allowed = allowed.clone();
                let skipped = skipped.clone();
                let limiter = limiter.clone();
                if let Ok(url) = Url::parse(sitemap) {
                    set.spawn(async move {
                        fetch_sitemap(limiter.clone(), &url, &allowed, &skipped).await
                    });
                }
            }

            while let Some(Ok(found)) = set.join_next().await {
                urls.extend(found);
            }
        }
    }
}

// Requests the sitemap
async fn get_sitemap(sitemap_url: &Url, limiter: &Arc<RateLimit>) -> Result<Response, Error> {
    let client = http_client();

    let retry_strat = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(5))
        .take(3);

    RetryIf::spawn(
        retry_strat,
        || async {
            let domain = sitemap_url.domain().expect("No domain in URL");
            limiter.until_key_ready(&domain.to_string()).await;
            let response = client.get(sitemap_url.to_string()).send().await;
            response
        },
        |error: &reqwest::Error| {
            if error.is_status() {
                if let Some(status) = error.status() {
                    let code = status.as_u16();
                    return code != 404 && code != 403;
                }
            }

            true
        },
    )
    .await
}

// Accessor for a sitemap cached to disk
fn get_cached_sitemap(sitemap_url: &Url) -> Option<String> {
    if let Some(domain) = sitemap_url.domain() {
        let site_cache = get_cache_location(domain);
        if site_cache.exists() {
            let tar_gz = std::fs::File::open(site_cache).unwrap();
            let tar = GzDecoder::new(tar_gz);
            let mut archive = Archive::new(tar);
            let site_path = sitemap_url.path().strip_prefix('/').unwrap();
            if let Ok(entries) = archive.entries() {
                for mut entry in entries.flatten() {
                    if let Ok(path) = entry.path() {
                        if path.display().to_string().eq(site_path) {
                            let mut buf = String::new();
                            match entry.read_to_string(&mut buf) {
                                Ok(_) => return Some(buf),
                                Err(err) => log::warn!("Error reading cache file {:?}", err),
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

// Accessor for the cache path based on the url and domain
fn get_cache_location(domain: &str) -> PathBuf {
    let site_cache_path = Path::new(SITE_CACHE_DIR);
    let _ = std::fs::create_dir_all(site_cache_path);
    site_cache_path.join(format!("{domain}.tar.gz"))
}

#[cfg(test)]
mod test {
    use crate::{bootstrap::fetch_rss, site::SiteInfo};

    #[tokio::test]
    #[ignore = "only used for dev"]
    async fn test_fetch_rss() {
        let info = SiteInfo::new("atp.fm")
            .await
            .expect("unable to create siteinfo");

        let feed_urls = fetch_rss(&info).await;
        assert_eq!(feed_urls.len(), 515);
    }
}
