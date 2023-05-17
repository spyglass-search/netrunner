use anyhow::Result;
use std::collections::HashMap;
use texting_robots::Robot;
use url::Url;

use super::{http_client, APP_USER_AGENT};
use crate::site::SiteInfo;

#[derive(Default)]
pub struct CrawlCache {
    pub cache: HashMap<String, Option<SiteInfo>>,
}

type HasSitemap = bool;

impl CrawlCache {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn process_url(&mut self, url: &str) -> HasSitemap {
        if let Ok(base) = Url::parse(url).and_then(|url| url.join("/")) {
            let base_url = base.to_string();

            if self.cache.contains_key(&base_url) {
                return match self.cache.get(&base_url).expect("check") {
                    Some(robot) => !robot.sitemaps.is_empty(),
                    None => false,
                };
            }

            match SiteInfo::new(&base_url).await {
                Ok(info) => {
                    let has_sitemap = !info.sitemaps.is_empty();
                    self.cache.insert(base_url, Some(info));
                    return has_sitemap;
                }
                Err(err) => {
                    log::error!("error grabbing site info: {err}");
                    self.cache.insert(base_url, None);
                }
            }
        }

        false
    }
}

/// Find and read robots.txt from a domain (if any exists)
pub async fn read_robots(robots_url: &str) -> Result<Option<Robot>> {
    // Use a normal user-agent otherwise some sites won't let us crawl
    let client = http_client();
    if let Ok(req) = client.get(robots_url).send().await {
        if req.status().is_success() {
            if let Ok(text) = req.text().await {
                if let Ok(robot) = Robot::new(APP_USER_AGENT, text.as_bytes()) {
                    return Ok(Some(robot));
                }
            }
        }
    }

    Ok(None)
}

#[cfg(test)]
mod test {
    use super::{read_robots, CrawlCache};
    use texting_robots::get_robots_url;

    #[ignore = "External Website Dependency"]
    #[tokio::test]
    async fn test_crawl_cache() {
        let mut crawl_cache = CrawlCache::new();
        assert_eq!(
            crawl_cache
                .process_url("https://questionablecontent.net/")
                .await,
            false
        );
        assert_eq!(
            crawl_cache
                .process_url("https://learn.microsoft.com/")
                .await,
            true
        );
    }

    #[tokio::test]
    async fn test_read_robots_domain() {
        let robots_uri = get_robots_url("https://example.com").expect("Valid URL");

        let res = read_robots(&robots_uri).await;
        assert!(res.is_ok());

        let robot = res.unwrap();
        // Should only fail on invalid URLs, fake ones are fine.
        assert_eq!(robots_uri, "https://example.com/robots.txt");
        assert!(robot.is_none());
    }

    #[tokio::test]
    async fn test_read_robots_failure() {
        let robots_uri = get_robots_url("file:///tmp");
        assert!(robots_uri.is_err());

        let robots_uri = get_robots_url("https://example.com").expect("Valid URL");

        let res = read_robots(&robots_uri).await;
        assert!(res.is_ok());

        let robot = res.unwrap();
        // Should only fail on invalid URLs, fake ones are fine.
        assert_eq!(robots_uri, "https://example.com/robots.txt");
        assert!(robot.is_none());
    }

    #[tokio::test]
    async fn test_read_robots_success() {
        let robots_uri = get_robots_url("https://oldschool.runescape.wiki").expect("Valid URL");

        let res = read_robots(&robots_uri).await;
        assert!(res.is_ok());

        let robot = res.unwrap();
        // Should only fail on invalid URLs, fake ones are fine.
        assert_eq!(robots_uri, "https://oldschool.runescape.wiki/robots.txt");
        assert!(robot.is_some());

        let robot = robot.unwrap();
        assert_eq!(robot.sitemaps.len(), 1);
    }
}
