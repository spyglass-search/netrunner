use std::{collections::HashSet, io::Read};

use anyhow::Result;
use async_recursion::async_recursion;
use bytes::buf::Buf;
use flate2::bufread::GzDecoder;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use texting_robots::Robot;

mod cdx;
mod robots;
use robots::Robots;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));

fn http_client() -> Client {
    // Use a normal user-agent otherwise some sites won't let us crawl
    reqwest::Client::builder()
        .gzip(true)
        .user_agent(APP_USER_AGENT)
        .build()
        .expect("Unable to create HTTP client")
}

/// Fetch and parse a sitemap file
#[async_recursion]
async fn fetch_sitemap(
    client: &Client,
    robot: &Robot,
    sitemap_url: &str,
    filters: &RegexSet,
) -> Vec<String> {
    println!("fetching sitemap: {}", sitemap_url);
    let mut urls = Vec::new();

    if let Ok(resp) = client.get(sitemap_url).send().await {
        if resp.status().is_success() {
            if let Ok(text) = resp.bytes().await {
                let mut buf = String::new();
                // Decode gzipped files. Doesn't work automatically if they were
                // gzipped before uploading it to their destination.
                if sitemap_url.ends_with(".gz") {
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
                            if let Some(loc) = url_entry.loc.get_url() {
                                let url = loc.to_string();
                                if robot.allowed(&url) && filters.is_match(&url) {
                                    urls.push(url);
                                }
                            }
                        }
                        SiteMapEntity::SiteMap(sitemap_entry) => {
                            if let Some(loc) = sitemap_entry.loc.get_url() {
                                urls.extend(
                                    fetch_sitemap(client, robot, loc.as_str(), filters).await,
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    urls
}

async fn read_sitemaps(robot: &Robot, sitemaps: &Vec<String>, filters: &RegexSet) -> Vec<String> {
    let client = http_client();
    let mut urls = Vec::new();

    for sitemap in sitemaps {
        urls.extend(fetch_sitemap(&client, robot, sitemap, filters).await);
    }

    urls
}

/// Kick off a crawl for URLs represented by <lens>.
pub async fn crawl(lens: LensConfig) -> Result<()> {
    let client = http_client();

    let mut robots = Robots::new();
    // Urls that need to be processed through a cdx index.
    let mut cdx_queue: HashSet<String> = HashSet::new();
    // Urls gathered from sitemaps + cdx processing.
    let mut to_crawl: HashSet<String> = HashSet::new();

    // ------------------------------------------------------------------------
    // First, build filters based on the lens. This will be used to filter out
    // urls from sitemaps / cdx indexes
    // ------------------------------------------------------------------------
    let filters = RegexSetBuilder::new(lens.into_regexes())
        .size_limit(10_000_000)
        .build()?;

    // ------------------------------------------------------------------------
    // Second, we fetch robots & sitemaps from the domains/urls represented by the lens
    // ------------------------------------------------------------------------
    for domain in lens.domains.iter() {
        let domain_url = format!("http://{}", domain);
        if !robots.process_url(&domain_url).await {
            cdx_queue.insert(domain_url);
        }
    }

    for prefix in lens.urls.iter() {
        let url = if prefix.ends_with('$') {
            // Remove the '$' suffix and add to the crawl queue
            prefix.strip_suffix('$').expect("No $ at end of prefix")
        } else {
            prefix
        };

        if !robots.process_url(url).await {
            cdx_queue.insert(url.to_owned());
        }
    }

    // ------------------------------------------------------------------------
    // Third, either read the sitemaps or pull data from a CDX to determine which
    // urls to crawl.
    // ------------------------------------------------------------------------
    // Crawl sitemaps
    for robot in robots.cache.values().flatten() {
        if !robot.sitemaps.is_empty() {
            to_crawl.extend(read_sitemaps(robot, &robot.sitemaps, &filters).await);
        }
    }

    // Process any URLs in the cdx queue
    for prefix in cdx_queue {
        loop {
            let _ = cdx::fetch_cdx(&client, &prefix, 1000, None).await;
        }
    }

    Ok(())
}
