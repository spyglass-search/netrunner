use clap::{Parser, Subcommand};
use tracing_log::LogTracer;
use tracing_subscriber::{fmt, layer::SubscriberExt, EnvFilter};

use spyglass_lens::LensConfig;

use std::io;
use std::path::Path;
use tokio::runtime;

use libnetrunner::validator::validate_lens;
use libnetrunner::Netrunner;

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
        .with(
            EnvFilter::from_default_env()
                .add_directive(LOG_LEVEL.into())
                .add_directive(LIB_LOG_LEVEL.parse().expect("invalid log filter")),
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
        Commands::Validate => validate_lens(&lens),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod test {
    use libnetrunner::{validator::validate_lens, Netrunner};
    use spyglass_lens::LensConfig;
    use std::path::Path;

    #[tokio::test]
    async fn test_crawl() {
        let lens_file = "fixtures/test.ron";
        let lens = LensConfig::from_path(Path::new(&lens_file).to_path_buf())
            .expect("Unable to load lens file");

        // Test crawling logic
        let mut netrunner = Netrunner::new(lens.clone());
        netrunner.crawl(false, true).await.expect("Unable to crawl");

        // Test validation logic
        validate_lens(&lens).expect("Unable to validate lens");
    }
}
