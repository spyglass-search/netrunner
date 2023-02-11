use anyhow::Result;
use bootstrap::Bootstrapper;
use governor::{Quota, RateLimiter};
use nonzero_ext::nonzero;
use parser::ParseResult;
use reqwest::Client;
use spyglass_lens::LensConfig;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use url::Url;

pub mod archive;
pub mod bootstrap;
mod cache;
mod cdx;
mod crawler;
pub mod parser;
pub mod s3;
pub mod site;
pub mod validator;

use crate::crawler::{handle_crawl, http_client};
use archive::{create_archives, ArchiveFiles, ArchiveRecord};

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));

pub struct CrawlOpts {
    pub create_warc: bool,
    pub requests_per_second: u32,
}

impl Default for CrawlOpts {
    fn default() -> Self {
        Self {
            create_warc: false,
            requests_per_second: 2,
        }
    }
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
    bootstrapper: Bootstrapper,
    client: Client,
    lens: LensConfig,
    // Where the cached web archive will be storage
    pub storage: PathBuf,
    pub state: NetrunnerState,
}

impl Netrunner {
    pub fn new(lens: LensConfig) -> Self {
        let storage = cache_storage_path(&lens);
        let state = NetrunnerState {
            has_urls: storage.join("urls.txt").exists(),
        };
        let client = http_client();

        Netrunner {
            bootstrapper: Bootstrapper::new(&client),
            client,
            lens,
            storage,
            state,
        }
    }

    pub fn url_txt_path(&self) -> PathBuf {
        self.storage.join("urls.txt")
    }

    pub async fn get_urls(&mut self) -> Vec<String> {
        let _ = self.bootstrapper.find_urls(&self.lens).await;
        self.bootstrapper.to_crawl.clone().into_iter().collect()
    }

    /// Kick off a crawl for URLs represented by <lens>.
    pub async fn crawl(&mut self, opts: CrawlOpts) -> Result<Option<ArchiveFiles>> {
        if !self.state.has_urls {
            self.bootstrapper.find_urls(&self.lens).await?;
        } else {
            log::info!("Already collected URLs, skipping");
            // Load urls from file
            let file = std::fs::read_to_string(self.url_txt_path())?;
            self.bootstrapper
                .to_crawl
                .extend(file.lines().map(|x| x.to_string()).collect::<Vec<String>>());
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
        self.bootstrapper.to_crawl.insert(url);
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

        let mut existing = HashSet::new();
        let mut to_remove = Vec::new();
        let mut recs = Vec::new();
        for path in paths.flatten() {
            match std::fs::read_to_string(path.path()) {
                Ok(contents) => {
                    if let Ok(record) = ron::from_str::<ArchiveRecord>(&contents) {
                        let url = record.url;
                        if existing.contains(&url) {
                            to_remove.push(path.path());
                        } else {
                            existing.insert(url.clone());
                            recs.push((url, path.path()));
                        }
                    }
                }
                Err(_) => {
                    let _ = std::fs::remove_file(path.path());
                }
            }
        }

        log::info!("Removing {} existing caches", to_remove.len());
        for path in to_remove {
            let _ = std::fs::remove_file(path);
        }

        recs
    }

    /// Web Archive (WARC) file format definition: https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1
    async fn crawl_loop(&mut self, tmp_storage: &PathBuf, quota: Quota) -> anyhow::Result<()> {
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));
        let to_crawl = &self.bootstrapper.to_crawl;

        let progress = Arc::new(AtomicUsize::new(0));
        let total = to_crawl.len();
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
        for url in to_crawl.iter().filter_map(|url| Url::parse(url).ok()) {
            if already_crawled.contains(&url.to_string()) {
                log::info!("-> skipping {}, already crawled", url);
                continue;
            }

            let progress = progress.clone();
            handle_crawl(&self.client, tmp_storage.clone(), lim.clone(), &url).await;
            let old_val = progress.fetch_add(1, Ordering::SeqCst);
            if old_val % 100 == 0 {
                log::info!("progress: {} / {}", old_val, total)
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use spyglass_lens::LensConfig;
    use std::io;
    use std::path::Path;
    use tracing_log::LogTracer;
    use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

    use crate::{parser::ParseResult, validator::validate_lens, CrawlOpts, Netrunner};

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
                create_warc: true,
                ..Default::default()
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
}
