use std::collections::HashSet;
use std::fmt::Write as FmtWrite;
use std::io::{BufWriter, Read, Write};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use crate::parser::ParseResult;
use chrono::prelude::*;
use flate2::{write::GzEncoder, Compression};
use reqwest::Response;
use serde::{Deserialize, Serialize};
use warc::{BufferedBody, RawRecordHeader, Record, RecordType, WarcHeader, WarcReader, WarcWriter};

pub const ARCHIVE_FILE: &str = "archive.warc";
pub const PARSED_ARCHIVE_FILE: &str = "parsed.gz";

pub struct Archiver {
    path: PathBuf,
    writer: WarcWriter<BufWriter<File>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ArchiveRecord {
    pub status: u16,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub content: String,
}

impl ArchiveRecord {
    pub async fn from_response(
        resp: Response,
        url_override: Option<String>,
    ) -> anyhow::Result<Self> {
        let headers: Vec<(String, String)> = resp
            .headers()
            .into_iter()
            .filter_map(|(name, value)| {
                if let Ok(value) = value.to_str() {
                    Some((name.to_string(), value.to_string()))
                } else {
                    None
                }
            })
            .collect();

        let status = resp.status().as_u16();
        let url = if let Some(url_override) = url_override {
            url_override
        } else {
            resp.url().as_str().to_string()
        };

        let content = resp.text().await?;
        Ok(ArchiveRecord {
            status,
            url,
            headers,
            content,
        })
    }
}

impl Archiver {
    fn parse_body(body: &str) -> (Vec<(String, String)>, String) {
        let mut headers = Vec::new();
        let mut content = String::new();

        let mut headers_finished = false;
        for line in body.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                headers_finished = true;
            } else {
                match headers_finished {
                    true => content.push_str(trimmed),
                    false => {
                        if let Some((key, value)) = trimmed.split_once(':') {
                            headers.push((key.trim().to_string(), value.trim().to_string()));
                        }
                    }
                }
            }
        }

        (headers, content)
    }

    pub fn new(storage: &Path) -> anyhow::Result<Self> {
        let path = storage.join(ARCHIVE_FILE);
        Ok(Self {
            path: path.clone(),
            writer: WarcWriter::from_path(path)?,
        })
    }

    pub fn read(path: &Path) -> anyhow::Result<Vec<ArchiveRecord>> {
        let warc_path = if !path.ends_with(format!("{ARCHIVE_FILE}.gz")) {
            let mut warc_path = path.join(ARCHIVE_FILE);
            warc_path.set_extension("warc.gz");
            warc_path
        } else {
            path.to_path_buf()
        };

        log::info!("Reading archive: {}", warc_path.display());

        // Unzip
        let warc = WarcReader::from_path_gzip(&warc_path)?;
        let mut records = Vec::new();
        for record in warc.iter_records().flatten() {
            let url = record
                .header(WarcHeader::TargetURI)
                .expect("TargetURI not set")
                .to_string();

            if let Ok(body) = String::from_utf8(record.body().into()) {
                let (headers, content) = Archiver::parse_body(&body);
                records.push(ArchiveRecord {
                    status: 200u16,
                    url,
                    headers,
                    content,
                });
            }
        }

        log::info!("Found {} records", records.len());
        Ok(records)
    }

    pub fn finish(self) -> anyhow::Result<PathBuf> {
        // Make sure our buffer has been flushed to the filesystem.
        if let Ok(mut inner_writer) = self.writer.into_inner() {
            let _ = inner_writer.flush();
        }

        // Read file from filesystem & compress.
        let file = std::fs::read(&self.path)?;
        let before = file.len();
        log::debug!(
            "compressing data from {} | {} bytes",
            self.path.display(),
            before
        );
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&file)?;

        // Check to see if we have an existing archive & remove it.
        let mut compressed = self.path;
        compressed.set_extension("warc.gz");
        if compressed.exists() {
            log::warn!("{} exists, removing!", compressed.display());
            std::fs::remove_file(compressed.clone())?;
        }

        let contents = encoder.finish()?;
        let after = contents.len();
        let compresion_percentage = (before as f64 - after as f64) / before as f64 * 100.0;
        std::fs::write(compressed.clone(), contents)?;
        log::info!(
            "saved to: {} | {} -> {} bytes ({:0.2}%)",
            compressed.display(),
            file.len(),
            after,
            compresion_percentage
        );

        Ok(compressed.clone())
    }

    pub fn generate_header(url: &str, content_length: usize) -> RawRecordHeader {
        let date = Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true);
        RawRecordHeader {
            version: "1.1".to_owned(),
            headers: vec![
                (
                    // mandatory
                    // Globally unique identifier for the current record
                    WarcHeader::RecordID,
                    Record::<BufferedBody>::generate_record_id().into_bytes(),
                ),
                (
                    // mandatory
                    // Number of octets in the block.
                    WarcHeader::ContentLength,
                    content_length.to_string().into_bytes(),
                ),
                (
                    // mandatory
                    // UTC timestamp that represents the instant the data
                    // was captured.
                    WarcHeader::Date,
                    date.into_bytes(),
                ),
                (
                    // mandatory
                    // Type of record
                    WarcHeader::WarcType,
                    RecordType::Response.to_string().into_bytes(),
                ),
                // Optional attributes
                (
                    WarcHeader::ContentType,
                    "text/html".to_string().into_bytes(),
                ),
                (WarcHeader::TargetURI, url.to_owned().into_bytes()),
            ]
            .into_iter()
            .collect(),
        }
    }

    pub async fn archive_record(&mut self, record: &ArchiveRecord) -> anyhow::Result<usize> {
        let url = record.url.clone();
        log::debug!("archiving {}", url);

        // Output headers into HTTP format
        let mut headers = "HTTP/1.1 200 OK\n".to_string();
        for (name, value) in record.headers.iter() {
            let _ = writeln!(headers, "{name}: {value}");
        }

        let body = record.content.clone();
        let content = format!("{headers}\n{body}");
        let warc_header = Self::generate_header(&url, content.len());

        let bytes_written = self.writer.write_raw(warc_header, &content)?;
        log::debug!("wrote {} bytes", bytes_written);
        Ok(bytes_written)
    }
}

pub struct ArchiveFiles {
    pub warc: PathBuf,
    pub parsed: PathBuf,
}

/// Create a preproprocessed archive from an existing WARC file
pub fn preprocess_warc_archive(warc: &Path) -> anyhow::Result<PathBuf> {
    let parent_dir = warc.parent().expect("Unable to get parent folder");
    log::info!("Saving preprocessed archive to: {}", parent_dir.display());

    let warc = WarcReader::from_path_gzip(warc)?;
    let path = parent_dir.join(PARSED_ARCHIVE_FILE);
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }

    let archive_path = std::fs::File::create(path.clone()).expect("Unable to create file");
    let mut gz = GzEncoder::new(&archive_path, Compression::default());

    let mut buffer = String::new();
    let mut archived_urls = HashSet::new();
    let mut duplicate_count = 0;

    for record in warc.iter_records().flatten() {
        buffer.clear();
        if record.body().read_to_string(&mut buffer).is_ok() {
            if let Some(url) = record.header(WarcHeader::TargetURI) {
                let (_, content) = Archiver::parse_body(&buffer);
                let parsed = crate::parser::html::html_to_text(&url, &content);
                let ser = ron::ser::to_string(&parsed).unwrap();

                if let Some(canonical) = parsed.canonical_url {
                    if let Ok(canonical) = url::Url::parse(&canonical) {
                        let url_str = canonical.to_string();
                        if !archived_urls.contains(&url_str) {
                            gz.write_fmt(format_args!("{ser}\n"))?;
                            archived_urls.insert(url_str);
                        } else {
                            duplicate_count += 1;
                        }
                    }
                }
            }
        }
    }

    gz.finish()?;
    log::info!("Found {duplicate_count} duplicates");
    log::info!("Preprocess {} docs", archived_urls.len());
    log::info!("Saved parsed results to: {}", path.display());

    Ok(path)
}

pub fn validate_preprocessed_archive(path: &Path) -> anyhow::Result<()> {
    let mut urls = HashSet::new();
    for res in ParseResult::iter_from_gz(path)? {
        let url = res.canonical_url.expect("Must have canonical URL");
        if urls.contains(&url) {
            return Err(anyhow::anyhow!(
                "Duplicate URL found in preprocessed archive: {:?}",
                url
            ));
        } else {
            urls.insert(url);
        }
    }

    Ok(())
}

pub fn warc_to_iterator(
    warc: &Path,
) -> anyhow::Result<impl Iterator<Item = Option<ArchiveRecord>>> {
    let parent_dir = warc.parent().expect("Unable to get parent folder");
    log::info!("Saving preprocessed archive to: {}", parent_dir.display());

    let warc = WarcReader::from_path_gzip(warc)?;

    let record_itr = warc.iter_records().map(move |record_rslt| {
        if let Ok(record) = record_rslt {
            let url = record
                .header(WarcHeader::TargetURI)
                .expect("TargetURI not set")
                .to_string();

            if let Ok(body) = String::from_utf8(record.body().into()) {
                let (headers, content) = Archiver::parse_body(&body);
                return Option::Some(ArchiveRecord {
                    status: 200u16,
                    url,
                    headers,
                    content,
                });
            }
        }
        Option::None
    });

    Ok(record_itr)
}

/// Creates gzipped archives for all the crawls & preprocessed crawl content.
pub async fn create_archives(
    storage: &Path,
    records: &[ArchiveRecord],
) -> anyhow::Result<ArchiveFiles> {
    log::info!("Archiving responses & pre-processed");
    let mut archiver = Archiver::new(storage).expect("Unable to create archiver");

    let parsed_archive_path = storage.join(PARSED_ARCHIVE_FILE);
    let parsed_archive =
        std::fs::File::create(parsed_archive_path.clone()).expect("Unable to create file");
    let mut gz = GzEncoder::new(&parsed_archive, Compression::default());
    let mut archived_urls = HashSet::new();
    for rec in records {
        // Only save successes to the archive
        if rec.status >= 200 && rec.status <= 299 && !archived_urls.contains(&rec.url) {
            let parsed = crate::parser::html::html_to_text(&rec.url, &rec.content);
            let canonical_url = parsed.canonical_url.clone();
            archiver.archive_record(rec).await?;

            // Only add to preprocessed file if canonical url is unique.
            if let Some(Ok(canonical)) = canonical_url.map(|x| url::Url::parse(&x)) {
                if !archived_urls.contains(&canonical.to_string()) {
                    let ser = ron::ser::to_string(&parsed).unwrap();
                    gz.write_fmt(format_args!("{ser}\n"))?;
                    archived_urls.insert(canonical.to_string());
                }
            }
        }
    }
    gz.finish()?;
    let archive_file = archiver.finish()?;
    log::info!("Saved parsed results to: {}", parsed_archive_path.display());
    log::info!("Finished crawl");

    Ok(ArchiveFiles {
        warc: archive_file,
        parsed: parsed_archive_path,
    })
}
