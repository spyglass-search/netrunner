use std::io::Write;
use std::path::{Path, PathBuf};
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

#[derive(Default)]
struct NetrunnerState {
    has_urls: bool,
}

#[derive(Default)]
pub struct Netrunner {
    client: Client,
    robots: Robots,
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
        let robots = Robots::new();
        let storage = Path::new(&lens.name).to_path_buf();
        if !storage.exists() {
            // No point in continuing if we're unable to create this directory
            if let Err(e) = std::fs::create_dir_all(storage.clone()) {
                panic!("Unable to create crawl folder: {}", e);
            }
        }

        let state = NetrunnerState {
            has_urls: storage.join("urls.txt").exists(),
        };

        Netrunner {
            client,
            robots,
            lens,
            storage,
            state,
            ..Default::default()
        }
    }

    /// Kick off a crawl for URLs represented by <lens>.
    pub async fn crawl(&mut self) -> Result<()> {
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
        for domain in self.lens.domains.iter() {
            let domain_url = format!("http://{}", domain);
            if !self.robots.process_url(&domain_url).await {
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
            if !self.robots.process_url(url).await {
                self.cdx_queue.insert(url.to_owned());
            }
        }

        // ------------------------------------------------------------------------
        // Third, either read the sitemaps or pull data from a CDX to determine which
        // urls to crawl.
        // ------------------------------------------------------------------------
        if !self.state.has_urls {
            self.fetch_urls(&allowed, &skipped).await;
        }

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

    pub async fn fetch_urls(&mut self, allowed: &RegexSet, skipped: &RegexSet) {
        // Crawl sitemaps
        for robot in self.robots.cache.values().flatten() {
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
