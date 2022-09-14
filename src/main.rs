use anyhow::anyhow;
use clap::{Parser, Subcommand};
use spyglass_lens::LensConfig;
use std::path::PathBuf;
use tokio::runtime;

mod lib;
use lib::Netrunner;

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
}

fn main() -> Result<(), anyhow::Error> {
    let cli = Cli::parse();
    let runtime = runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("netrunner-worker")
        .build()?;

    match &cli.command {
        Commands::Crawl => match LensConfig::from_path(cli.lens_file.clone()) {
            Ok(lens) => {
                let mut netrunner = Netrunner::new(lens);
                runtime.block_on(netrunner.crawl())
            }
            Err(e) => {
                println!("Unable to read lens @ \"{}\"", cli.lens_file.display());
                Err(anyhow!(e.to_string()))
            }
        },
    }
}
