use std::fmt::Write;
use std::io::{BufWriter, Read};
use std::{
    fs::File,
    path::{Path, PathBuf},
};

use chrono::prelude::*;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use reqwest::Response;
use serde::{Deserialize, Serialize};
use warc::{BufferedBody, RawRecordHeader, Record, RecordType, WarcHeader, WarcReader, WarcWriter};

const ARCHIVE_FILE: &str = "archive.warc";

pub struct Archiver {
    path: PathBuf,
    writer: WarcWriter<BufWriter<File>>,
}

#[derive(Serialize, Deserialize)]
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
        let mut warc_path = path.join(ARCHIVE_FILE);
        warc_path.set_extension("warc.gz");
        log::info!("Reading archive: {}", warc_path.display());

        // Unzip
        let file = std::fs::read(&warc_path)?;
        let mut d = GzDecoder::new(&file[..]);
        let mut s = String::new();
        d.read_to_string(&mut s)?;

        let mut records = Vec::new();
        let reader = WarcReader::new(s.as_bytes());
        for record in reader.iter_records().flatten() {
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
        use std::io::Write;
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
        log::debug!(
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
            let _ = writeln!(headers, "{}: {}", name, value);
        }

        let body = record.content.clone();
        let content = format!("{}\n{}", headers, body);
        let warc_header = Self::generate_header(&url, content.len());

        let bytes_written = self.writer.write_raw(warc_header, &content)?;
        log::debug!("wrote {} bytes", bytes_written);
        Ok(bytes_written)
    }
}
