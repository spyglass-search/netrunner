use anyhow::Result;
use std::collections::{HashMap, HashSet};
use texting_robots::{get_robots_url, Robot};

use self::config::LensConfig;
pub mod config;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));

// pub async fn enqueue_urls() {
// 1.a Find & parse sitemap (if any)
// 1.b Otherwise use data from Internet Archive CDX

// 2. Add to queue for crawler.
// }

/// Find and read robots.txt from a domain (if any exists)
async fn read_robots(url: &str) -> Result<(String, Option<Robot>)> {
    // Use a normal user-agent otherwise some sites won't let us crawl
    let client = reqwest::Client::builder()
        .user_agent(APP_USER_AGENT)
        .build()
        .expect("Unable to create HTTP client");

    if let Ok(robots_url) = get_robots_url(url) {
        if let Ok(req) = client.get(robots_url.clone()).send().await {
            if req.status().is_success() {
                if let Ok(text) = req.text().await {
                    if let Ok(robot) = Robot::new(APP_USER_AGENT, text.as_bytes()) {
                        return Ok((robots_url, Some(robot)));
                    }
                }
            }
        }

        return Ok((robots_url, None));
    }

    // Invalid URL
    Err(anyhow::anyhow!("Invalid URL"))
}

/// Kick off a crawl for URLs represented by <lens>.
pub async fn crawl(lens: LensConfig) -> Result<()> {
    let mut robots_cache: HashMap<String, Option<Robot>> = HashMap::new();

    let _domains: HashSet<String> = HashSet::new();
    let _to_crawl: HashSet<String> = HashSet::new();

    for domain in lens.domains.iter() {
        println!("{}", domain);
    }

    for prefix in lens.urls.iter() {
        let url = if prefix.ends_with('$') {
            // Remove the '$' suffix and add to the crawl queue
            prefix.strip_suffix('$').expect("No $ at end of prefix")
        } else {
            prefix
        };

        if let Ok((robots_uri, robot)) = read_robots(url).await {
            if let Some(robot) = &robot {
                println!("{:?}", robot.delay);
                println!("{:?}", robot.sitemaps);
            }
            robots_cache.insert(robots_uri, robot);
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use super::read_robots;

    #[tokio::test]
    async fn test_read_robots_failure() {
        let res = read_robots("file:///tmp").await;
        assert!(res.is_err());

        let res = read_robots("https://example.com").await;
        assert!(res.is_ok());
        let (robots_uri, robot) = res.unwrap();
        // Should only fail on invalid URLs, fake ones are fine.
        assert_eq!(robots_uri, "https://example.com/robots.txt");
        assert!(robot.is_none());
    }

    #[tokio::test]
    async fn test_read_robots_success() {
        let res = read_robots("https://oldschool.runescape.wiki").await;
        assert!(res.is_ok());

        let (robots_uri, robot) = res.unwrap();
        // Should only fail on invalid URLs, fake ones are fine.
        assert_eq!(robots_uri, "https://oldschool.runescape.wiki/robots.txt");
        assert!(robot.is_some());

        let robot = robot.unwrap();
        assert_eq!(robot.sitemaps.len(), 1);
    }
}