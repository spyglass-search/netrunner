use blake2::{Blake2s256, Digest};
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

pub mod html;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ParseResult {
    /// Index should use this URL instead of the one that lead to the content.
    pub canonical_url: Option<String>,
    /// Text content from page after stripping HTML tags & any semantically
    /// unimportant sections (header/footer/etc.)
    pub content: String,
    /// Used to determine whether document content has changed.
    pub content_hash: String,
    /// Page description, extracted from meta tags or summarized from the actual content
    pub description: String,
    /// Links found in the page.
    #[serde(skip)]
    pub links: HashSet<String>,
    /// Meta (OpenGraph, etc) tags associated w/ this content.
    pub meta: HashMap<String, String>,
    /// Title of the page, document, etc.
    pub title: Option<String>,
}

impl ParseResult {
    pub fn builder() -> ParseResultBuilder {
        ParseResultBuilder::new()
    }

    pub fn iter_from_gz(file: &Path) -> anyhow::Result<ParseResultGzIterator> {
        let file = File::open(file)?;
        Ok(ParseResultGzIterator::new(BufReader::new(GzDecoder::new(
            file,
        ))))
    }
}

type GzBufReader = BufReader<GzDecoder<File>>;
pub struct ParseResultGzIterator {
    reader: GzBufReader,
    buffer: String,
}

/// Utility iterator that reads in lines from a gzipped archive of serialized
/// ParseResults
impl ParseResultGzIterator {
    pub fn new(reader: GzBufReader) -> Self {
        Self {
            reader,
            buffer: String::new(),
        }
    }
}

impl Iterator for ParseResultGzIterator {
    type Item = ParseResult;
    fn next(&mut self) -> Option<Self::Item> {
        self.buffer.clear();
        if let Ok(read) = self.reader.read_line(&mut self.buffer) {
            if read == 0 {
                return None;
            }

            if let Ok(res) = ron::de::from_str::<ParseResult>(&self.buffer) {
                Some(res)
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl Default for ParseResultBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub struct ParseResultBuilder {
    result: ParseResult,
}

impl ParseResultBuilder {
    pub fn build(self) -> ParseResult {
        self.result
    }

    pub fn new() -> Self {
        ParseResultBuilder {
            result: ParseResult::default(),
        }
    }

    pub fn canonical_url(mut self, url: Option<String>) -> Self {
        self.result.canonical_url = url;
        self
    }

    pub fn content(mut self, content: String) -> Self {
        let mut hasher = Blake2s256::new();
        hasher.update(content.clone());
        let res = hasher.finalize();

        self.result.content = content;
        self.result.content_hash = hex::encode(res);

        self
    }

    pub fn description(mut self, desc: String) -> Self {
        self.result.description = desc;
        self
    }

    pub fn links(mut self, links: HashSet<String>) -> Self {
        self.result.links = links;
        self
    }

    pub fn meta(mut self, meta: HashMap<String, String>) -> Self {
        self.result.meta = meta;
        self
    }

    pub fn title(mut self, title: Option<String>) -> Self {
        self.result.title = title;
        self
    }
}
