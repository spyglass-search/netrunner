use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::{collections::HashSet, io::Read};

use anyhow::Result;
use async_recursion::async_recursion;
use bytes::buf::Buf;
use flate2::bufread::GzDecoder;
use futures::future;
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::{Client, StatusCode};
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use texting_robots::Robot;
use tokio::task::JoinHandle;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;
use url::Url;

pub mod archive;
mod cdx;
mod robots;
pub mod validator;

use archive::{ArchiveRecord, Archiver};
use robots::Robots;

use crate::cdx::create_archive_url;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));
const RETRY_DELAY_MS: u64 = 5000;

fn http_client() -> Client {
    // Use a normal user-agent otherwise some sites won't let us crawl
    reqwest::Client::builder()
        .gzip(true)
        .user_agent(APP_USER_AGENT)
        .build()
        .expect("Unable to create HTTP client")
}

pub fn cache_storage_path(lens: &LensConfig) -> PathBuf {
    let storage = Path::new("archives").join(&lens.name);
    if !storage.exists() {
        // No point in continuing if we're unable to create this directory
        std::fs::create_dir_all(storage.clone()).expect("Unable to create crawl folder");
    }

    storage
}

pub fn tmp_storage_path(lens: &LensConfig) -> PathBuf {
    let storage = Path::new("tmp").join(&lens.name);
    if !storage.exists() {
        // No point in continuing if we're unable to create this directory
        std::fs::create_dir_all(storage.clone()).expect("Unable to create crawl folder");
    }

    storage
}

async fn fetch_page(
    client: &Client,
    url: &str,
    url_override: Option<String>,
    page_store: &Path,
) -> Result<(), ()> {
    // Wait for when we can crawl this based on the domain
    match client.get(url).send().await {
        Ok(resp) => {
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                let retry_after_ms: u64 =
                    resp.headers()
                        .get("Retry-After")
                        .map_or(RETRY_DELAY_MS, |header| {
                            if let Ok(header) = header.to_str() {
                                log::warn!("found Retry-After: {}", header);
                                header.parse::<u64>().unwrap_or(RETRY_DELAY_MS)
                            } else {
                                RETRY_DELAY_MS
                            }
                        });

                log::warn!("429 received... retrying after {}ms", retry_after_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(retry_after_ms)).await;

                Err(())
            } else {
                log::info!("fetched {}: {}", resp.status(), url);
                // Save response to tmp storage
                if let Ok(record) = ArchiveRecord::from_response(resp, url_override).await {
                    if let Ok(serialized) = ron::to_string(&record) {
                        // Hash the URL to store in the cache
                        let mut hasher = DefaultHasher::new();
                        record.url.hash(&mut hasher);
                        let id = hasher.finish().to_string();
                        let file = page_store.join(id.to_string());
                        let _ = std::fs::write(file.clone(), serialized);
                        log::debug!("cached <{}> -> <{}>", record.url, file.display());
                    }
                }

                Ok(())
            }
        }
        Err(err) => {
            log::error!("Unable to fetch {} - {}", url, err);
            Err(())
        }
    }
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
    // Where the cached web archive will be storage
    pub storage: PathBuf,
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

    pub fn url_txt_path(&self) -> PathBuf {
        self.storage.join("urls.txt")
    }

    /// Kick off a crawl for URLs represented by <lens>.
    pub async fn crawl(&mut self, print_urls: bool, create_crawl_archive: bool) -> Result<()> {
        let mut robots = Robots::new();
        // ------------------------------------------------------------------------
        // First, build filters based on the lens. This will be used to filter out
        // urls from sitemaps / cdx indexes
        // ------------------------------------------------------------------------
        log::info!("Loading lens rules");
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
        log::info!("Fetching robots.txt & sitemaps.xml");
        for domain in self.lens.domains.iter() {
            let domain_url = format!("http://{}", domain);
            if !robots.process_url(&domain_url).await {
                self.cdx_queue.insert(domain_url);
            }
        }

        for prefix in self.lens.urls.iter() {
            let url = if prefix.ends_with('$') {
                // Remove the '$' suffix and add to the crawl queue
                let url = prefix.trim_end_matches('$');
                self.to_crawl.insert(url.to_string());
                continue;
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
            log::info!("Already collected URLs, skipping");
            // Load urls from file
            let file = std::fs::read_to_string(self.url_txt_path())?;
            self.to_crawl
                .extend(file.lines().map(|x| x.to_string()).collect::<Vec<String>>());
        }

        if print_urls {
            let mut sorted_urls = self.to_crawl.clone().into_iter().collect::<Vec<String>>();
            sorted_urls.sort();
            for url in &sorted_urls {
                log::info!("{}", url);
            }
            log::info!("Discovered {} urls for lens", sorted_urls.len());
        }

        if create_crawl_archive {
            // CRAWL BABY CRAWL
            // Default to max 2 requests per second for a domain.
            let quota = Quota::per_second(nonzero!(5u32));
            self.crawl_loop(tmp_storage_path(&self.lens), quota).await?;
        }

        Ok(())
    }

    fn cached_records(&self, tmp_storage: &PathBuf) -> Vec<ArchiveRecord> {
        let paths = std::fs::read_dir(tmp_storage).expect("unable to read tmp storage dir");

        let mut recs = Vec::new();
        for path in paths.flatten() {
            let record = ron::from_str::<ArchiveRecord>(
                &std::fs::read_to_string(path.path()).expect("Unable to read cache file"),
            )
            .expect("Unable to deserialize record");
            recs.push(record);
        }

        recs
    }

    /// Web Archive (WARC) file format definition: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1
    async fn crawl_loop(&mut self, tmp_storage: PathBuf, quota: Quota) -> anyhow::Result<()> {
        let mut archiver = Archiver::new(&self.storage).expect("Unable to create archiver");
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

        let progress = Arc::new(AtomicUsize::new(0));
        let total = self.to_crawl.len();
        let to_crawl = self.to_crawl.clone().into_iter();
        let mut already_crawled: HashSet<String> = HashSet::new();

        // Before we begin, check to see if we've already crawled anything
        let recs = self.cached_records(&tmp_storage);
        log::debug!("found {} crawls in cache", recs.len());
        for rec in recs {
            already_crawled.insert(rec.url);
        }

        log::info!(
            "beginning crawl, already crawled {} urls",
            already_crawled.len()
        );
        progress.store(already_crawled.len(), Ordering::SeqCst);

        // Spin up tasks to crawl through everything
        let tasks: Vec<JoinHandle<()>> = to_crawl
            .filter_map(|url| {
                if already_crawled.contains(&url) {
                    log::info!("-> skipping {}, already crawled", url);
                    return None;
                }

                let progress = progress.clone();
                let lim = lim.clone();
                let tmp_storage = tmp_storage.clone();

                let res = tokio::spawn(async move {
                    // OG url
                    let parsed_url = Url::parse(&url).expect("Invalid URL");
                    // URL to Wayback Machine
                    let ia_url = create_archive_url(parsed_url.as_ref());

                    let domain = parsed_url.domain().expect("No domain in URL");
                    let client = http_client();

                    let retry_strat = ExponentialBackoff::from_millis(100).take(3);

                    // Retry if we run into 429 / timeout errors
                    let web_archive = Retry::spawn(retry_strat.clone(), || async {
                        // Wait for when we can crawl this based on the domain
                        lim.until_key_ready(&domain.to_string()).await;
                        fetch_page(&client, &ia_url, Some(parsed_url.to_string()), &tmp_storage)
                            .await
                    })
                    .await;

                    if web_archive.is_err() {
                        let _ = Retry::spawn(retry_strat, || async {
                            // Wait for when we can crawl this based on the domain
                            lim.until_key_ready(&domain.to_string()).await;
                            fetch_page(&client, parsed_url.as_ref(), None, &tmp_storage).await
                        })
                        .await;
                    }

                    let old_val = progress.fetch_add(1, Ordering::SeqCst);
                    if old_val % 100 == 0 {
                        log::info!("progress: {} / {}", old_val, total)
                    }
                });

                Some(res)
            })
            .collect();

        // Archive responses
        let _ = future::join_all(tasks).await;

        log::info!("Archiving responses");
        let recs = self.cached_records(&tmp_storage);
        for rec in recs {
            // Only save successes to the archive
            if rec.status >= 200 && rec.status <= 299 {
                archiver.archive_record(&rec).await?;
            }
        }

        archiver.finish()?;
        log::info!("Finished crawl");

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
        log::debug!("fetching sitemap: {}", sitemap_url);
        let mut urls = Vec::new();

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

        // Write out URLs to crawl folder
        let mut file = std::fs::File::create(self.url_txt_path()).expect("create failed");

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
