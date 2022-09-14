use std::fmt::Write;
use std::io::BufWriter;
use std::{fs::File, path::Path};

use chrono::prelude::*;
use reqwest::Response;
use warc::{BufferedBody, RawRecordHeader, Record, RecordType, WarcHeader, WarcWriter};

pub struct Archiver {
    writer: WarcWriter<BufWriter<File>>,
}

impl Archiver {
    pub fn new(storage: &Path) -> anyhow::Result<Self> {
        // let mut file = WarcWriter::from_path_gzip(self.storage.join("warc.gz"))
        //     .unwrap();

        Ok(Self {
            writer: WarcWriter::from_path(storage.join("archive.warc"))?,
        })
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

    pub async fn archive_response(&mut self, resp: Response) -> anyhow::Result<usize> {
        let url = resp.url().as_str().to_owned();

        // Output headers into HTTP format
        let mut headers = "HTTP/1.1 200 OK\n".to_string();
        for (name, value) in resp.headers() {
            if let Ok(value) = value.to_str() {
                let _ = writeln!(headers, "{}: {}", name, value);
            }
        }

        let body = resp.text().await.unwrap();
        let content = format!("{}\n{}", headers, body);
        let warc_header = Self::generate_header(&url, content.len());

        let bytes_written = self.writer.write_raw(warc_header, &content)?;
        println!("Wrote {} bytes", bytes_written);

        // if let Ok(gzip_stream) = file.into_inner() {
        //     gzip_stream.finish().into_result()?;
        // }
        Ok(bytes_written)
    }
}
