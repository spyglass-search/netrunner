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
use feedfinder::FeedType;
use flate2::read::GzDecoder;
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use parser::ParseResult;
use regex::{RegexSet, RegexSetBuilder};
use reqwest::Client;
use rss::Channel;
use site::SiteInfo;
use sitemap::reader::{SiteMapEntity, SiteMapReader};
use spyglass_lens::LensConfig;
use texting_robots::Robot;
use url::Url;

pub mod archive;
mod cache;
mod cdx;
mod crawler;
pub mod parser;
pub mod s3;
pub mod site;
pub mod validator;

use crate::crawler::{handle_crawl, http_client};
use archive::{create_archives, ArchiveFiles, ArchiveRecord};
use cache::CrawlCache;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));

#[derive(Default)]
pub struct CrawlOpts {
    pub print_urls: bool,
    pub create_warc: bool,
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

#[derive(Clone)]
pub struct NetrunnerState {
    pub has_urls: bool,
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
    pub state: NetrunnerState,
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

    pub async fn get_urls(&mut self) -> Vec<String> {
        let _rslt = self
            .crawl(CrawlOpts {
                print_urls: false,
                create_warc: false,
            })
            .await;

        self.to_crawl.clone().into_iter().collect()
    }

    /// Kick off a crawl for URLs represented by <lens>.
    pub async fn crawl(&mut self, opts: CrawlOpts) -> Result<Option<ArchiveFiles>> {
        let mut cache = CrawlCache::new();
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
            let domain_url = format!("http://{domain}");
            // If there are no sitemaps, add to CDX queue
            if !cache.process_url(&domain_url).await {
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
            if !cache.process_url(url).await {
                self.cdx_queue.insert(url.to_owned());
            }
        }

        // ------------------------------------------------------------------------
        // Third, either read the sitemaps or pull data from a CDX to determine which
        // urls to crawl.
        // ------------------------------------------------------------------------
        if !self.state.has_urls {
            self.fetch_urls(&cache, &allowed, &skipped).await;
        } else {
            log::info!("Already collected URLs, skipping");
            // Load urls from file
            let file = std::fs::read_to_string(self.url_txt_path())?;
            self.to_crawl
                .extend(file.lines().map(|x| x.to_string()).collect::<Vec<String>>());
        }

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

        if opts.print_urls {
            let mut sorted_urls = self.to_crawl.clone().into_iter().collect::<Vec<String>>();
            sorted_urls.sort();
            for url in &sorted_urls {
                println!("{url}");
            }
            eprintln!("Discovered {} urls for lens", sorted_urls.len());
        }

        if opts.create_warc {
            // CRAWL BABY CRAWL
            // Default to max 2 requests per second for a domain.
            let quota = Quota::per_second(nonzero!(2u32));
            let tmp_storage = tmp_storage_path(&self.lens);
            self.crawl_loop(&tmp_storage, quota).await?;
            let archives =
                create_archives(&self.storage, &self.cached_records(&tmp_storage)).await?;
            return Ok(Some(archives));
        }

        Ok(None)
    }

    pub async fn crawl_url(
        &mut self,
        url: String,
    ) -> Result<Vec<(ArchiveRecord, Option<ParseResult>)>> {
        self.to_crawl.insert(url);
        let quota = Quota::per_second(nonzero!(2u32));
        let tmp_storage = tmp_storage_path(&self.lens);
        self.crawl_loop(&tmp_storage, quota).await?;
        let archived = self.cached_records(&tmp_storage);

        let mut records = Vec::new();
        for (_, path) in archived {
            if let Ok(Ok(rec)) =
                std::fs::read_to_string(path).map(|s| ron::from_str::<ArchiveRecord>(&s))
            {
                if rec.status >= 200 && rec.status <= 299 {
                    let parsed = crate::parser::html::html_to_text(&rec.url, &rec.content);
                    records.push((rec, Some(parsed)));
                } else {
                    records.push((rec, None));
                }
            }
        }

        Ok(records)
    }

    pub fn clear_cache(&self) -> Result<(), std::io::Error> {
        std::fs::remove_dir_all(tmp_storage_path(&self.lens))
    }

    fn cached_records(&self, tmp_storage: &PathBuf) -> Vec<(String, PathBuf)> {
        let paths = std::fs::read_dir(tmp_storage).expect("unable to read tmp storage dir");

        let mut recs = Vec::new();
        for path in paths.flatten() {
            match std::fs::read_to_string(path.path()) {
                Ok(contents) => {
                    if let Ok(record) = ron::from_str::<ArchiveRecord>(&contents) {
                        recs.push((record.url, path.path()));
                    }
                }
                Err(_) => {
                    let _ = std::fs::remove_file(path.path());
                }
            }
        }

        recs
    }

    /// Web Archive (WARC) file format definition: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1
    async fn crawl_loop(&mut self, tmp_storage: &PathBuf, quota: Quota) -> anyhow::Result<()> {
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

        let progress = Arc::new(AtomicUsize::new(0));
        let total = self.to_crawl.len();
        let mut already_crawled: HashSet<String> = HashSet::new();

        // Before we begin, check to see if we've already crawled anything
        let recs = self.cached_records(tmp_storage);
        log::debug!("found {} crawls in cache", recs.len());
        for (url, _) in recs {
            already_crawled.insert(url);
        }

        log::info!(
            "beginning crawl, already crawled {} urls",
            already_crawled.len()
        );
        progress.store(already_crawled.len(), Ordering::SeqCst);

        // Spin up tasks to crawl through everything
        for url in self.to_crawl.iter().filter_map(|url| Url::parse(url).ok()) {
            if already_crawled.contains(&url.to_string()) {
                log::info!("-> skipping {}, already crawled", url);
                continue;
            }

            let progress = progress.clone();
            handle_crawl(&http_client(), tmp_storage.clone(), lim.clone(), &url).await;
            let old_val = progress.fetch_add(1, Ordering::SeqCst);
            if old_val % 100 == 0 {
                log::info!("progress: {} / {}", old_val, total)
            }
        }

        Ok(())
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

    pub async fn fetch_urls(&mut self, cache: &CrawlCache, allowed: &RegexSet, skipped: &RegexSet) {
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

        // Write out URLs to crawl folder
        let mut file = std::fs::File::create(self.url_txt_path()).expect("create failed");

        let mut sorted = self
            .to_crawl
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>();
        sorted.sort();

        for url in sorted {
            let _ = file.write(format!("{url}\n").as_bytes());
        }
    }
}

#[cfg(test)]
mod test {
    use spyglass_lens::LensConfig;
    use std::io;
    use std::path::Path;
    use tracing_log::LogTracer;
    use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

    use crate::{
        parser::ParseResult, site::SiteInfo, validator::validate_lens, CrawlOpts, Netrunner,
    };

    #[tokio::test]
    #[ignore]
    async fn test_crawl() {
        // Setup some nice console logging for tests
        let subscriber = tracing_subscriber::registry()
            .with(
                EnvFilter::from_default_env()
                    .add_directive(tracing::Level::INFO.into())
                    .add_directive("libnetrunner=TRACE".parse().expect("invalid log filter")),
            )
            .with(fmt::Layer::new().with_ansi(false).with_writer(io::stdout));
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set a global subscriber");
        LogTracer::init().expect("Unable to initialize logger");

        let lens_file = "fixtures/test.ron";
        let lens = LensConfig::from_path(Path::new(&lens_file).to_path_buf())
            .expect("Unable to load lens file");

        // Test crawling logic
        let mut netrunner = Netrunner::new(lens.clone());
        let archives = netrunner
            .crawl(CrawlOpts {
                print_urls: false,
                create_warc: true,
            })
            .await
            .expect("Unable to crawl");

        // Validate archives created are readable.
        if let Some(archives) = archives {
            assert!(archives.warc.exists());
            assert!(archives.parsed.exists());

            let reader =
                ParseResult::iter_from_gz(&archives.parsed).expect("Unable to read parsed archive");

            assert_eq!(reader.count(), 1);
        }

        // Test validation logic
        if let Err(err) = validate_lens(&lens) {
            eprintln!("Failed validation: {err}");
            panic!("Failed");
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_fetch_rss() {
        let lens = LensConfig {
            author: "test".to_string(),
            name: "test".to_string(),
            domains: vec!["atp.fm".to_string()],
            ..Default::default()
        };

        let netrunner = Netrunner::new(lens);
        let info = SiteInfo::new("atp.fm")
            .await
            .expect("unable to create siteinfo");
        let feed_urls = netrunner.fetch_rss(&info).await;
        assert_eq!(feed_urls.len(), 515);
    }
}
