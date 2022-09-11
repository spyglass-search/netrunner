use std::{
    collections::{HashMap, HashSet},
    io::Read,
};

use anyhow::Result;
use async_recursion::async_recursion;
use bytes::buf::Buf;
use flate2::bufread::GzDecoder;
use reqwest::Client;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use texting_robots::{get_robots_url, Robot};

use self::config::LensConfig;
pub mod config;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));

// pub async fn enqueue_urls() {
// 1.a Find & parse sitemap (if any)
// 1.b Otherwise use data from Internet Archive CDX

// 2. Add to queue for crawler.
// }

fn http_client() -> Client {
    // Use a normal user-agent otherwise some sites won't let us crawl
    reqwest::Client::builder()
        .gzip(true)
        .user_agent(APP_USER_AGENT)
        .build()
        .expect("Unable to create HTTP client")
}

/// Find and read robots.txt from a domain (if any exists)
async fn read_robots(url: &str) -> Result<(String, Option<Robot>)> {
    // Use a normal user-agent otherwise some sites won't let us crawl
    let client = http_client();
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

/// Fetch and parse a sitemap file
#[async_recursion]
async fn fetch_sitemap(client: &Client, robot: &Robot, url: &str) -> Vec<String> {
    println!("fetching sitemap: {}", url);
    let mut urls = Vec::new();

    if let Ok(resp) = client.get(url).send().await {
        if resp.status().is_success() {
            if let Ok(text) = resp.bytes().await {
                let mut buf = String::new();
                // Decode gzipped files. Doesn't work automatically if they were
                // gzipped before uploading it to their destination.
                if url.ends_with(".gz") {
                    let mut decoder = GzDecoder::new(text.reader());
                    decoder.read_to_string(&mut buf).unwrap();
                } else {
                    let res = String::from_utf8((&text).to_vec());
                    // Received an invalid text blob from the server
                    if res.is_err() {
                        return Vec::new();
                    }
                    buf = res.expect("Unable to convert to UTF-8");
                }

                let parser = SiteMapReader::new(buf.as_bytes());
                for entity in parser {
                    match entity {
                        SiteMapEntity::Url(url_entry) => {
                            url_entry
                                .loc
                                .get_url()
                                .map(|loc| {
                                    let url = loc.to_string();
                                    if robot.allowed(&url) {
                                        urls.push(url);
                                    }
                                });
                        }
                        SiteMapEntity::SiteMap(sitemap_entry) => {
                            if let Some(loc) = sitemap_entry.loc.get_url() {
                                urls.extend(fetch_sitemap(client, robot, loc.as_str()).await);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    println!("{}: found {} urls", url, urls.len());
    urls
}

async fn read_sitemaps(robot: &Robot, sitemaps: &Vec<String>) -> Vec<String> {
    let client = http_client();

    let mut urls = Vec::new();
    for sitemap in sitemaps {
        urls.extend(fetch_sitemap(&client, robot, sitemap).await);
    }

    urls
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
                let urls = read_sitemaps(&robot.sitemaps).await;
                println!("number of urls: {}", urls.len());
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
