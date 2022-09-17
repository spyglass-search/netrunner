use std::path::Path;
use clap::{Parser, Subcommand};
use spyglass_lens::LensConfig;
use std::path::PathBuf;
use tokio::runtime;

mod lib;
use lib::archive::Archiver;
use lib::{cache_storage_path, Netrunner};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    /// Lens file
    #[clap(short, long, value_parser, value_name = "FILE")]
    lens_file: Option<PathBuf>,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, PartialEq, Eq)]
enum Commands {
    /// Grabs all the URLs represented by <lens-file> for review.
    CheckUrls,
    Clean,
    /// Crawls & creates a web archive for the pages represented by <lens-file>
    Crawl,
    /// Validate the lens file and, if available, the cached web archive for <lens-file>
    Validate,
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("netrunner-worker")
        .build()?;

    if cli.command == Commands::Clean {
        println!("Cleaning up temp directories/files");
        let tmp = Path::new("tmp");
        if tmp.exists() {
            std::fs::remove_dir_all("tmp")?;
        }
        return Ok(());
    }

    if cli.lens_file.is_none() {
        return Err(anyhow::anyhow!("Please provide a lens file".to_string()));
    }

    let lens_file = cli.lens_file.expect("Expecting lens file");

    let lens = LensConfig::from_path(lens_file)?;
    match &cli.command {
        Commands::CheckUrls => {
            let mut netrunner = Netrunner::new(lens);
            runtime.block_on(netrunner.crawl(true, false))
        }
        Commands::Crawl => {
            let mut netrunner = Netrunner::new(lens);
            runtime.block_on(netrunner.crawl(false, true))
        }
        Commands::Validate => {
            // Check that we can read the WARC file
            let path = cache_storage_path(&lens);
            match Archiver::read(&path) {
                Ok(records) => {
                    let mut zero_len_headers = 0;
                    let mut zero_len_content = 0;

                    for rec in &records {
                        if rec.headers.is_empty() {
                            zero_len_headers += 1;
                        }

                        if rec.content.is_empty() {
                            zero_len_content += 1;
                        }
                    }

                    if zero_len_headers > 0 {
                        println!("Found {} 0-length headers", zero_len_headers);
                    }

                    if zero_len_content > 0 {
                        println!("Found {} 0-length content", zero_len_content);
                    }

                    println!("Found & validated {} records", records.len());
                    Ok(())
                }
                Err(e) => Err(e),
            }
        },
        _ => Ok(())
    }
}
