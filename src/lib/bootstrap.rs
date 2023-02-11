use async_recursion::async_recursion;
use bytes::Buf;
use feedfinder::FeedType;
use flate2::read::GzDecoder;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use rss::Channel;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use std::{collections::HashSet, io::Read};
use texting_robots::Robot;
use url::Url;

use super::cdx;
use crate::{cache::CrawlCache, crawler::http_client, site::SiteInfo};

#[derive(Clone)]
pub struct Bootstrapper {
    client: Client,
    // Urls that need to be processed through a cdx index.
    cdx_queue: HashSet<String>,
    // Urls gathered from sitemaps + cdx processing.
    pub to_crawl: HashSet<String>,
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
            to_crawl: HashSet::new(),
        }
    }

    pub async fn find_urls(&mut self, lens: &LensConfig) -> anyhow::Result<()> {
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
            let domain_url = format!("http://{domain}");
            // If there are no sitemaps, add to CDX queue
            if !cache.process_url(&domain_url).await {
                self.cdx_queue.insert(domain_url);
            }
        }

        for prefix in lens.urls.iter() {
            let url = if prefix.ends_with('$') {
                // Remove the '$' suffix and add to the crawl queue
                let url = prefix.trim_end_matches('$');
                self.to_crawl.insert(url.to_string());
                continue;
            } else {
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
        self.process_sitemaps_and_cdx(&mut cache, &allowed, &skipped)
            .await;

        // Clear CDX queue after fetching URLs.
        self.cdx_queue.clear();
        // Ignore invalid URLs and remove fragments from URLs (e.g. http://example.com#Title
        // is considered the same as http://example.com)
        self.to_crawl = self
            .to_crawl
            .iter()
            .filter_map(|url| {
                if let Ok(mut url) = Url::parse(url) {
                    url.set_fragment(None);
                    Some(url.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(())
    }

    async fn process_sitemaps_and_cdx(
        &mut self,
        cache: &mut CrawlCache,
        allowed: &RegexSet,
        skipped: &RegexSet,
    ) {
        // Crawl sitemaps & rss feeds
        for info in cache.cache.values().flatten() {
            // Fetch links from RSS feeds
            self.to_crawl.extend(self.fetch_rss(info).await);

            // Fetch links from sitemap
            if let Some(robot) = &info.robot {
                if !robot.sitemaps.is_empty() {
                    for sitemap in &robot.sitemaps {
                        self.to_crawl
                            .extend(self.fetch_sitemap(robot, sitemap, allowed, skipped).await);
                    }
                }
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
                self.to_crawl.extend(filtered);
                if resume.is_none() {
                    break;
                }

                resume_key = resume;
            }
        }
    }

    async fn fetch_rss(&self, info: &SiteInfo) -> Vec<String> {
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
        &self,
        robot: &Robot,
        sitemap_url: &str,
        allowed: &RegexSet,
        skipped: &RegexSet,
    ) -> Vec<String> {
        log::debug!("fetching sitemap: {}", sitemap_url);
        let mut urls: Vec<String> = Vec::new();

        if let Ok(resp) = self.client.get(sitemap_url).send().await {
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
                for entity in parser {
                    match entity {
                        SiteMapEntity::Url(url_entry) => {
                            if let Some(loc) = url_entry.loc.get_url() {
                                let url = loc.to_string();
                                if robot.allowed(&url)
                                    && allowed.is_match(&url)
                                    && !skipped.is_match(&url)
                                {
                                    urls.push(url);
                                }
                            }
                        }
                        SiteMapEntity::SiteMap(sitemap_entry) => {
                            if let Some(loc) = sitemap_entry.loc.get_url() {
                                urls.extend(
                                    self.fetch_sitemap(robot, loc.as_str(), allowed, skipped)
                                        .await,
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        log::info!("found {} urls for {}", urls.len(), sitemap_url);
        urls
    }
}

#[cfg(test)]
mod test {
    use crate::{bootstrap::Bootstrapper, crawler::http_client, site::SiteInfo};

    #[tokio::test]
    async fn test_fetch_rss() {
        let bs = Bootstrapper::new(&http_client());

        let info = SiteInfo::new("atp.fm")
            .await
            .expect("unable to create siteinfo");

        let feed_urls = bs.fetch_rss(&info).await;
        assert_eq!(feed_urls.len(), 515);
    }
}
