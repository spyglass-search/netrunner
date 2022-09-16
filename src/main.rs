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
    #[clap(short, long, value_parser, value_name = "FILE")]
    lens_file: PathBuf,
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Crawl,
    Validate,
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("netrunner-worker")
        .build()?;

    let lens = LensConfig::from_path(cli.lens_file.clone())?;
    match &cli.command {
        Commands::Crawl => {
            let mut netrunner = Netrunner::new(lens);
            runtime.block_on(netrunner.crawl())
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
        }
    }
}
