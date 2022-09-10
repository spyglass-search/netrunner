use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod lib;
use lib::config::LensConfig;

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

fn main() {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Crawl => {
            if let Ok(lens) = LensConfig::from_path(cli.lens_file) {
                println!("{:?}", lens);
            }
        }
    }
}
