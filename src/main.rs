use clap::{Parser, Subcommand};
use libnetrunner::CrawlOpts;
use spyglass_lens::LensConfig;
use tracing_log::LogTracer;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

use std::io;
use std::path::{Path, PathBuf};
use tokio::runtime;

use libnetrunner::validator::validate_lens;
use libnetrunner::{site::SiteInfo, Netrunner};

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
    /// Upload finished archive to S3.
    #[clap(short, long, value_parser, value_name = "S3_BUCKET_NAME")]
    s3_bucket: Option<String>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand, PartialEq, Eq)]
enum Commands {
    /// Print out useful information about a domain, such as whether there is an
    /// rss feed, an robots.txt, and a sitemap
    CheckDomain { domain: String },
    /// Grabs all the URLs represented by <lens-file> for review.
    CheckUrls,
    /// Removes temporary directories/files
    Clean,
    /// Crawls & creates a web archive for the pages represented by <lens-file>
    Crawl,
    /// Generate a preprocessed archive file from an archive.warc.gz file.
    Preprocess { warc: PathBuf },
    /// Validate the lens file and, if available, the cached web archive for <lens-file>
    Validate,
}

fn main() -> Result<(), anyhow::Error> {
    // Setup some nice console logging
    let subscriber = tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive(LOG_LEVEL.into())
                .add_directive(LIB_LOG_LEVEL.parse().expect("invalid log filter")),
        )
        .with(fmt::Layer::new().with_writer(io::stdout));
    tracing::subscriber::set_global_default(subscriber).expect("Unable to set a global subscriber");
    LogTracer::init()?;

    let mut cli = Cli::parse();
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("netrunner-worker")
        .build()?;

    runtime.block_on(_run_cmd(&mut cli))
}

async fn _parse_lens(cli: &Cli) -> Result<LensConfig, anyhow::Error> {
    if cli.lens_file.is_none() {
        return Err(anyhow::anyhow!(
            "Please point to either a local lens file or an HTTPS link.".to_string()
        ));
    }

    let lens_file = cli.lens_file.as_ref().expect("Expecting lens file");
    if lens_file.starts_with("http") {
        // Attempt to read download and read file from internet
        log::info!("Detected URL, attempting to download lens file.");
        let resp = reqwest::get(lens_file).await?;
        let resp = resp.text().await?;
        match ron::from_str(&resp) {
            Err(err) => Err(anyhow::anyhow!(err.to_string())),
            Ok(res) => Ok(res),
        }
    } else {
        LensConfig::from_path(Path::new(&lens_file).to_path_buf())
    }
}

async fn _run_cmd(cli: &mut Cli) -> Result<(), anyhow::Error> {
    match &cli.command {
        Commands::CheckDomain { domain } => {
            let site_info = SiteInfo::new(domain).await?;
            site_info.print();

            Ok(())
        }
        Commands::CheckUrls => {
            let lens = _parse_lens(cli).await?;
            let mut netrunner = Netrunner::new(lens);
            // Remove previous urls.txt if any
            if netrunner.url_txt_path().exists() {
                let _ = std::fs::remove_file(netrunner.url_txt_path());
                netrunner.state.has_urls = false;
            }

            netrunner
                .crawl(CrawlOpts {
                    print_urls: true,
                    create_warc: false,
                })
                .await?;

            Ok(())
        }
        Commands::Clean => {
            log::info!("Cleaning up temp directories/files");
            let tmp = Path::new("tmp");
            if tmp.exists() {
                std::fs::remove_dir_all("tmp")?;
            }

            Ok(())
        }
        Commands::Crawl => {
            let lens = _parse_lens(cli).await?;
            let mut netrunner = Netrunner::new(lens.clone());

            let archive_path = netrunner
                .crawl(CrawlOpts {
                    print_urls: false,
                    create_warc: true,
                })
                .await?;

            if let (Some(archive_path), Some(s3_bucket)) = (archive_path, &cli.s3_bucket) {
                let key = format!("{}/{}.gz", &lens.name, libnetrunner::archive::ARCHIVE_FILE);
                libnetrunner::s3::upload_to_bucket(&archive_path.warc, s3_bucket, &key).await?;

                let key = format!(
                    "{}/{}",
                    &lens.name,
                    libnetrunner::archive::PARSED_ARCHIVE_FILE
                );
                libnetrunner::s3::upload_to_bucket(&archive_path.parsed, s3_bucket, &key).await?;
            }

            Ok(())
        }
        Commands::Preprocess { warc } => {
            if !warc.exists() {
                log::error!("Path \"{}\" does not exist!", warc.display());
                return Err(anyhow::anyhow!("Invalid warc path"));
            }

            libnetrunner::archive::preprocess_warc_archive(warc)
        }
        Commands::Validate => {
            let lens = _parse_lens(cli).await?;
            validate_lens(&lens)
        }
    }
}
