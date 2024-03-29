use async_recursion::async_recursion;
use bytes::Buf;
use dashmap::DashSet;
use feedfinder::FeedType;
use flate2::read::GzDecoder;
use governor::Quota;
use governor::RateLimiter;
use nonzero_ext::nonzero;
use regex::Regex;
use regex::RegexBuilder;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use rss::Channel;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::types::LensRule;
use spyglass_lens::types::UrlSanitizeConfig;
use spyglass_lens::LensConfig;
use std::sync::Arc;
use std::time::Duration;
use std::{collections::HashSet, io::Read};
use tokio::task::JoinSet;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::RetryIf;
use url::Url;

use super::cdx;
use super::crawler::RateLimit;
use crate::{cache::CrawlCache, crawler::http_client, site::SiteInfo};

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

        // Check if the site map found entries for specified url
        // if no entry was found then we should check the cdx
        for prefix in lens.urls.iter() {
            let url = if prefix.ends_with('$') {
                continue;
            } else {
                prefix
            };

            let mut count = 0;
            for crawl in to_crawl.iter() {
                if crawl.starts_with(url) {
                    count += 1;
                }
            }

            if count <= 1 {
                self.cdx_queue.insert(url.to_owned());
            }
        }

        if !self.cdx_queue.is_empty() {
            self.process_cdx(&mut to_crawl, &allowed, &skipped).await;
        }

        let sanitize_rules = lens
            .rules
            .iter()
            .filter_map(|rule| {
                if let LensRule::SanitizeUrls(_, config) = rule {
                    let regex = RegexBuilder::new(&rule.to_regex()).build().ok()?;
                    Some((regex, config.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<(Regex, UrlSanitizeConfig)>>();

        // Clear CDX queue after fetching URLs.
        self.cdx_queue.clear();
        // Ignore invalid URLs and remove fragments from URLs (e.g. http://example.com#Title
        // is considered the same as http://example.com)
        let cleaned: HashSet<String> = to_crawl
            .iter()
            .filter_map(|url| {
                if let Ok(mut url) = Url::parse(&url) {
                    url.set_fragment(None);
                    for (regex, config) in &sanitize_rules {
                        if regex.is_match(url.as_str()) {
                            sanitize_url(&mut url, config);
                        }
                    }

                    Some(url.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(cleaned.into_iter().collect())
    }

    async fn process_sitemaps_and_cdx(
        &self,
        cache: &CrawlCache,
        to_crawl: &mut DashSet<String>,
        allowed: &RegexSet,
        skipped: &RegexSet,
    ) {
        let mut sitemaps = Vec::new();

        for info in cache.cache.values().flatten() {
            // Fetch links from RSS feeds
            to_crawl.extend(fetch_rss(info).await);
            // Grab list of sitemaps
            if !info.sitemaps.is_empty() {
                for sitemap in &info.sitemaps {
                    sitemaps.push(sitemap.clone());
                }
            }
        }

        process_sitemaps(&sitemaps, to_crawl, allowed, skipped).await;
        self.process_cdx(to_crawl, allowed, skipped).await;
    }

    async fn process_cdx(
        &self,
        to_crawl: &mut DashSet<String>,
        allowed: &RegexSet,
        skipped: &RegexSet,
    ) {
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

pub async fn process_sitemaps(
    sitemaps: &Vec<String>,
    to_crawl: &mut DashSet<String>,
    allowed: &RegexSet,
    skipped: &RegexSet,
) {
    // Crawl sitemaps & rss feeds
    let mut handles = JoinSet::new();

    if !sitemaps.is_empty() {
        log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
        let quota = Quota::per_second(nonzero!(2u32));
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

        for sitemap in sitemaps {
            let allowed = allowed.clone();
            let skipped = skipped.clone();
            let lim = lim.clone();
            if let Ok(url) = Url::parse(sitemap) {
                handles.spawn(
                    async move { fetch_sitemap(lim.clone(), &url, &allowed, &skipped).await },
                );
            }
        }

        while let Some(Ok(urls)) = handles.join_next().await {
            to_crawl.extend(urls);
        }
    }
}

pub async fn fetch_rss(info: &SiteInfo) -> Vec<String> {
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

/// Fetch and parse a sitemap file
#[async_recursion]
async fn fetch_sitemap(
    limiter: Arc<RateLimit>,
    sitemap_url: &Url,
    allowed: &RegexSet,
    skipped: &RegexSet,
) -> HashSet<String> {
    let mut urls: HashSet<String> = HashSet::new();
    let client = http_client();

    let retry_strat = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(5))
        .take(3);

    let response = RetryIf::spawn(
        retry_strat,
        || async {
            let domain = sitemap_url.domain().expect("No domain in URL");
            limiter.until_key_ready(&domain.to_string()).await;
            log::debug!("fetching sitemap: {}", sitemap_url);
            client.get(sitemap_url.to_string()).send().await
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
    .await;

    match response {
        Ok(resp) => {
            if resp.status().is_success() {
                let sitemap_url = sitemap_url.to_string();
                let mut buf = String::new();
                // Decode gzipped files. Doesn't work automatically if they were
                // gzipped before uploading it to their destination.
                if sitemap_url.ends_with(".gz") {
                    if let Ok(text) = resp.bytes().await {
                        let mut decoder = GzDecoder::new(text.reader());
                        decoder.read_to_string(&mut buf).unwrap();
                    }
                } else if let Ok(text) = resp.text().await {
                    buf = text.replace('\u{feff}', "");
                }

                let parser = SiteMapReader::new(buf.as_bytes());
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
                    let mut set = JoinSet::new();
                    log::info!("spawning {} tasks for sitemap fetching", sitemaps.len());
                    for sitemap in sitemaps {
                        let allowed = allowed.clone();
                        let skipped = skipped.clone();
                        let limiter = limiter.clone();
                        if let Ok(url) = Url::parse(&sitemap) {
                            set.spawn(async move {
                                fetch_sitemap(limiter.clone(), &url, &allowed, &skipped).await
                            });
                        }
                    }

                    while let Some(Ok(found)) = set.join_next().await {
                        urls.extend(found);
                    }
                }
            } else {
                log::debug!("error fetching sitemap: {:?}", resp.error_for_status_ref());
            }
        }
        Err(err) => log::error!("{:?}", err),
    }

    if !urls.is_empty() {
        log::info!("found {} urls for {}", urls.len(), sitemap_url);
    }

    urls
}

// Helper method used to process the url sanitization configuration for
// the provided url
fn sanitize_url(url: &mut Url, config: &UrlSanitizeConfig) {
    if config.remove_query_parameter {
        url.set_query(None);
    }
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
