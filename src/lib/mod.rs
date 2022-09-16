use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{collections::HashSet, io::Read};

use anyhow::Result;
use async_recursion::async_recursion;
use bytes::buf::Buf;
use flate2::bufread::GzDecoder;
use futures::future;
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::{Client, Response, StatusCode};
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use texting_robots::Robot;
use tokio::task::JoinHandle;
use tokio_retry::Retry;
use tokio_retry::strategy::ExponentialBackoff;
use url::Url;

pub mod archive;
mod cdx;
mod robots;

use archive::Archiver;
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

pub fn cache_storage_path(lens: &LensConfig) -> PathBuf {
    let storage = Path::new("archives").join(&lens.name).to_path_buf();
    if !storage.exists() {
        // No point in continuing if we're unable to create this directory
        std::fs::create_dir_all(storage.clone()).expect("Unable to create crawl folder");
    }

    storage
}

#[derive(Clone)]
struct NetrunnerState {
    has_urls: bool,
}

#[derive(Clone)]
pub struct Netrunner {
    client: Client,
    lens: LensConfig,
    // Urls that need to be processed through a cdx index.
    cdx_queue: HashSet<String>,
    // Urls gathered from sitemaps + cdx processing.
    to_crawl: HashSet<String>,
    storage: PathBuf,
    state: NetrunnerState,
}

impl Netrunner {
    pub fn new(lens: LensConfig) -> Self {
        let client = http_client();
        let storage = cache_storage_path(&lens);
        let state = NetrunnerState {
            has_urls: storage.join("urls.txt").exists(),
        };

        Netrunner {
            client,
            lens,
            storage,
            state,
            cdx_queue: Default::default(),
            to_crawl: Default::default(),
        }
    }

    /// Kick off a crawl for URLs represented by <lens>.
    pub async fn crawl(&mut self) -> Result<()> {
        let mut robots = Robots::new();
        // ------------------------------------------------------------------------
        // First, build filters based on the lens. This will be used to filter out
        // urls from sitemaps / cdx indexes
        // ------------------------------------------------------------------------
        let filters = self.lens.into_regexes();
        let allowed = RegexSetBuilder::new(filters.allowed)
            .size_limit(100_000_000)
            .build()?;
        let skipped = RegexSetBuilder::new(filters.skipped)
            .size_limit(100_000_000)
            .build()?;

        // ------------------------------------------------------------------------
        // Second, we fetch robots & sitemaps from the domains/urls represented by the lens
        // ------------------------------------------------------------------------
        println!("-> Fetching robots.txt & sitemaps.xml");
        for domain in self.lens.domains.iter() {
            let domain_url = format!("http://{}", domain);
            if !robots.process_url(&domain_url).await {
                self.cdx_queue.insert(domain_url);
            }
        }

        for prefix in self.lens.urls.iter() {
            let url = if prefix.ends_with('$') {
                // Remove the '$' suffix and add to the crawl queue
                prefix.strip_suffix('$').expect("No $ at end of prefix")
            } else {
                prefix
            };

            // If there is no sitemaps in the robots, add to CDX queue
            if !robots.process_url(url).await {
                self.cdx_queue.insert(url.to_owned());
            }
        }

        // ------------------------------------------------------------------------
        // Third, either read the sitemaps or pull data from a CDX to determine which
        // urls to crawl.
        // ------------------------------------------------------------------------
        if !self.state.has_urls {
            self.fetch_urls(&robots, &allowed, &skipped).await;
        } else {
            println!("-> Already collected URLs, skipping");
            // Load urls from file
            let file = std::fs::read_to_string(self.storage.join("urls.txt"))?;
            self.to_crawl
                .extend(file.lines().map(|x| x.to_string()).collect::<Vec<String>>());
        }

        // CRAWL BABY CRAWL
        let quota = Quota::per_second(nonzero!(2u32));
        self.crawl_loop(quota).await?;

        Ok(())
    }

    /// Web Archive (WARC) file format definition: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1
    async fn crawl_loop(&mut self, quota: Quota) -> anyhow::Result<()> {
        let mut archiver = Archiver::new(&self.storage).expect("Unable to create archiver");

        // Default to 1 a second
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

        let mut to_crawl: Vec<String> = self.to_crawl.clone().into_iter().collect();
        to_crawl.sort();

        let tasks: Vec<JoinHandle<Option<Response>>> = to_crawl
            .into_iter()
            .map(|url| {
                let url = url.clone();
                let lim = lim.clone();
                tokio::spawn(async move {
                    let parsed_url = Url::parse(&url).expect("Invalid URL");
                    let domain = parsed_url.domain().expect("No domain in URL");
                    let client = http_client();

                    // Crawl!
                    let retry_strat = ExponentialBackoff::from_millis(100).take(3);
                    // If we're hitting the CDX endpoint too fast, wait a little bit before retrying.
                    let res = Retry::spawn(retry_strat, || async {
                        // Wait for when we can crawl this based on the domain
                        lim.until_key_ready(&domain.to_string()).await;
                        println!("fetching {}", url);
                        if let Ok(resp) = client.get(&url).send().await {
                            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                                let retry_after_ms: u64 = resp.headers().get("Retry-After")
                                    .map_or(1000, |header| {
                                        if let Ok(header) = header.to_str() {
                                            u64::from_str_radix(header, 10)
                                                .unwrap_or(1000)
                                        } else {
                                            1000
                                        }
                                    });

                                println!("429 received... retrying after {}", retry_after_ms);
                                tokio::time::sleep(tokio::time::Duration::from_millis(retry_after_ms)).await;
                                return Err(());
                            } else {
                                return Ok(resp);
                            }
                        } else {
                            Err(())
                        }
                    })
                    .await;

                    res.ok()
                })
            })
            .collect();

        // Archive responses
        let responses = future::join_all(tasks).await;
        for result in responses {
            match result {
                Ok(Some(response)) => {
                    archiver.archive_response(response).await?;
                }
                Err(err) => {
                    println!("Invalid result: {}", err);
                }
                _ => {}
            }
        }

        archiver.finish()?;

        Ok(())
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
        println!("fetching sitemap: {}", sitemap_url);
        let mut urls = Vec::new();

        if let Ok(resp) = self.client.get(sitemap_url).send().await {
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
        }

        println!("found {} urls for {}", urls.len(), sitemap_url);
        urls
    }

    pub async fn fetch_urls(&mut self, robots: &Robots, allowed: &RegexSet, skipped: &RegexSet) {
        // Crawl sitemaps
        for robot in robots.cache.values().flatten() {
            if !robot.sitemaps.is_empty() {
                for sitemap in &robot.sitemaps {
                    self.to_crawl
                        .extend(self.fetch_sitemap(robot, sitemap, allowed, skipped).await);
                }
            }
        }

        // Process any URLs in the cdx queue
        for prefix in self.cdx_queue.iter() {
            let mut resume_key = None;
            println!("fetching cdx for: {}", prefix);
            while let Ok((urls, resume)) =
                cdx::fetch_cdx(&self.client, prefix, 1000, resume_key.clone()).await
            {
                self.to_crawl.extend(urls);
                if resume.is_none() {
                    break;
                }

                resume_key = resume;
            }
        }

        // Write out URLs to crawl folder
        let mut file = std::fs::File::create(self.storage.join("urls.txt")).expect("create failed");

        let mut sorted = self
            .to_crawl
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        sorted.sort();

        for url in sorted {
            let _ = file.write(format!("{}\n", url).as_bytes());
        }
    }
}
