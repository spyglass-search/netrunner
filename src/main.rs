use anyhow::anyhow;
use clap::{Parser, Subcommand};
use tracing_log::LogTracer;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

use spyglass_lens::LensConfig;

use std::collections::HashSet;
use std::io;
use std::path::Path;
use tokio::runtime;
use url::Url;

use libnetrunner::archive::Archiver;
use libnetrunner::{cache_storage_path, Netrunner};

const LOG_LEVEL: tracing::Level = tracing::Level::INFO;

#[cfg(debug_assertions)]
const LIB_LOG_LEVEL: &str = "libnetrunner=DEBUG";

#[cfg(not(debug_assertions))]
const LIB_LOG_LEVEL: &str = "libnetrunner=INFO";

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Lens file
    #[clap(short, long, value_parser, value_name = "FILE")]
    lens_file: Option<String>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Commands {
    /// Grabs all the URLs represented by <lens-file> for review.
    CheckUrls,
    /// Removes temporary directories/files
    Clean,
    /// Crawls & creates a web archive for the pages represented by <lens-file>
    Crawl,
    /// Validate the lens file and, if available, the cached web archive for <lens-file>
    Validate,
}

fn main() -> Result<(), anyhow::Error> {
    // Setup some nice console logging
    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env()
            .add_directive(LOG_LEVEL.into())
            .add_directive(LIB_LOG_LEVEL.parse().expect("invalid log filter"))
        )
        .with(fmt::Layer::new().with_writer(io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global subscriber");
    LogTracer::init()?;

    let cli = Cli::parse();
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("netrunner-worker")
        .build()?;

    if cli.command == Commands::Clean {
        log::info!("Cleaning up temp directories/files");
        let tmp = Path::new("tmp");
        if tmp.exists() {
            std::fs::remove_dir_all("tmp")?;
        }
        return Ok(());
    }

    if cli.lens_file.is_none() {
        return Err(anyhow::anyhow!(
            "Please point to either a local lens file or an HTTPS link.".to_string()
        ));
    }

    let lens_file = cli.lens_file.expect("Expecting lens file");
    let lens = if lens_file.starts_with("http") {
        // Attempt to read download and read file from internet
        log::info!("Detected URL, attempting to download lens file.");
        let resp = runtime.block_on(async move {
            let resp = reqwest::get(lens_file).await?;
            resp.text().await
        })?;
        ron::from_str(&resp)?
    } else {
        LensConfig::from_path(Path::new(&lens_file).to_path_buf())?
    };

    match &cli.command {
        Commands::CheckUrls => {
            // Remove previous urls.txt if any
            let mut netrunner = Netrunner::new(lens);
            if netrunner.url_txt_path().exists() {
                let _ = std::fs::remove_file(netrunner.url_txt_path());
            }
            runtime.block_on(netrunner.crawl(true, false))
        }
        Commands::Crawl => {
            let mut netrunner = Netrunner::new(lens);
            runtime.block_on(netrunner.crawl(false, true))
        }
        Commands::Validate => {
            // Check that we can read the WARC file
            log::info!("Checking for archive file.");
            let path = cache_storage_path(&lens);
            match Archiver::read(&path) {
                Ok(records) => {
                    let mut failed_validation = false;

                    let urls_txt =
                        std::fs::read_to_string(path.join("urls.txt")).expect("urls.txt not found");
                    let expected_urls: HashSet<Url> =
                        urls_txt.lines()
                            .flat_map(|x| {
                                match Url::parse(x) {
                                    Ok(url) => Some(url),
                                    Err(err) => {
                                        failed_validation = true;
                                        log::error!("Invalid URL in `urls.txt`: <{}> - {}", x, err);
                                        None
                                    }
                                }
                            })
                            .collect();

                    let mut zero_len_headers = 0;
                    let mut zero_len_content = 0;

                    let mut found_urls: HashSet<Url> = HashSet::new();
                    for rec in &records {
                        match Url::parse(&rec.url) {
                            Ok(url) => {
                                found_urls.insert(url);
                            },
                            Err(err) => {
                                log::error!("Invalid URL in archive: <{}> - {}", rec.url, err);
                                failed_validation = true;
                                continue;
                            }
                        }

                        if rec.headers.is_empty() {
                            zero_len_headers += 1;
                        }

                        if rec.content.is_empty() {
                            zero_len_content += 1;
                        }
                    }

                    if zero_len_headers > 0 {
                        log::error!("Found {} 0-length headers", zero_len_headers);
                        failed_validation = true;
                    }

                    if zero_len_content > 0 {
                        log::error!("Found {} 0-length content", zero_len_content);
                        failed_validation = true;
                    }

                    let missing_urls: Vec<Url> =
                        expected_urls.difference(&found_urls).cloned().collect();

                    if !missing_urls.is_empty() {
                        log::error!("{} missing urls", missing_urls.len());
                        log::error!("{:?}", missing_urls);
                        failed_validation = true;
                    }

                    log::info!("Found & validated {} records", records.len());
                    if failed_validation {
                        Err(anyhow!("Failed validation"))
                    } else {
                        Ok(())
                    }
                }
                Err(e) => Err(e),
            }
        }
        _ => Ok(()),
    }
}
