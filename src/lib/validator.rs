use std::collections::HashSet;

use anyhow::{anyhow, Result};
use spyglass_lens::LensConfig;
use url::Url;

use crate::archive::Archiver;
use crate::cache_storage_path;

pub fn validate_lens(lens: &LensConfig) -> Result<()> {
    // Check that we can read the WARC file
    log::info!("Checking for archive file.");
    let path = cache_storage_path(&lens);
    match Archiver::read(&path) {
        Ok(records) => {
            let mut failed_validation = false;

            let urls_txt =
                std::fs::read_to_string(path.join("urls.txt")).expect("urls.txt not found");
            let expected_urls: HashSet<Url> = urls_txt
                .lines()
                .flat_map(|x| match Url::parse(x) {
                    Ok(url) => Some(url),
                    Err(err) => {
                        failed_validation = true;
                        log::error!("Invalid URL in `urls.txt`: <{}> - {}", x, err);
                        None
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
                    }
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

            let missing_urls: Vec<Url> = expected_urls.difference(&found_urls).cloned().collect();

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
