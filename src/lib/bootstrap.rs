use async_recursion::async_recursion;
use bytes::Buf;
use dashmap::DashSet;
use feedfinder::FeedType;
use flate2::read::GzDecoder;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use rss::Channel;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use std::{collections::HashSet, io::Read};
use tokio::task::JoinSet;
use url::Url;

use super::cdx;
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
            for sitemap in sitemaps {
                let allowed = allowed.clone();
                let skipped = skipped.clone();
                handles.spawn(async move { fetch_sitemap(&sitemap, &allowed, &skipped).await });
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

/// Fetch and parse a sitemap file
#[async_recursion]
async fn fetch_sitemap(
    sitemap_url: &str,
    allowed: &RegexSet,
    skipped: &RegexSet,
) -> HashSet<String> {
    log::debug!("fetching sitemap: {}", sitemap_url);

    let mut urls: HashSet<String> = HashSet::new();
    let client = http_client();

    if let Ok(resp) = client.get(sitemap_url).send().await {
        if resp.status().is_success() {
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
                    set.spawn(async move { fetch_sitemap(&sitemap, &allowed, &skipped).await });
                }

                while let Some(Ok(found)) = set.join_next().await {
                    urls.extend(found);
                }
            }
        }
    }

    if !urls.is_empty() {
        log::info!("found {} urls for {}", urls.len(), sitemap_url);
    }

    urls
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
