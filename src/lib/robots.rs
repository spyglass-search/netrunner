use anyhow::Result;
use std::collections::HashMap;
use texting_robots::{get_robots_url, Robot};

use super::{http_client, APP_USER_AGENT};

#[derive(Default)]
pub struct Robots {
    pub cache: HashMap<String, Option<Robot>>,
}

type HasSitemap = bool;

impl Robots {
    pub fn new() -> Self {
        Default::default()
    }

    pub async fn process_url(&mut self, url: &str) -> HasSitemap {
        if let Ok(robots_txt_url) = get_robots_url(url) {
            if self.cache.contains_key(&robots_txt_url) {
                let res = self.cache.get(&robots_txt_url).expect("check");
                return match res {
                    Some(robot) => !robot.sitemaps.is_empty(),
                    None => false,
                };
            }

            if let Ok(robot) = read_robots(&robots_txt_url).await {
                let has_sitemap = match &robot {
                    Some(robot) => !robot.sitemaps.is_empty(),
                    None => false,
                };

                self.cache.insert(robots_txt_url, robot);
                return has_sitemap;
            }
        }

        false
    }
}

/// Find and read robots.txt from a domain (if any exists)
async fn read_robots(robots_url: &str) -> Result<Option<Robot>> {
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
    use super::read_robots;
    use texting_robots::get_robots_url;

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
