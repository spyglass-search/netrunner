use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Different rules that filter out the URLs that would be crawled for a lens
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum LensRule {
    /// Limits the depth of a URL to a certain depth.
    /// For example:
    ///  - LimitURLDepth("https://example.com/", 1) will limit it to https://example.com/<path 1>
    ///  - LimitURLDepth("https://example.com/", 2) will limit it to https://example.com/<path 1>/<path 2>
    ///  - etc.
    LimitURLDepth(String, u8),
    /// Skips are applied when bootstrapping & crawling
    SkipURL(String),
}

/// Contexts are a set of domains/URLs/etc. that restricts a search space to
/// improve results.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct LensConfig {
    #[serde(default = "LensConfig::default_author")]
    pub author: String,
    pub name: String,
    pub description: Option<String>,
    pub domains: Vec<String>,
    pub urls: Vec<String>,
    pub version: String,
    #[serde(default = "LensConfig::default_is_enabled")]
    pub is_enabled: bool,
    #[serde(default)]
    pub rules: Vec<LensRule>,
    #[serde(default)]
    pub trigger: String,
}

impl LensConfig {
    fn default_author() -> String {
        "Unknown".to_string()
    }

    fn default_is_enabled() -> bool {
        true
    }

    pub fn from_path(path: PathBuf) -> anyhow::Result<Self> {
        let contents = std::fs::read_to_string(path)?;
        match ron::from_str::<LensConfig>(&contents) {
            Ok(lens) => Ok(lens),
            Err(e) => Err(anyhow::Error::msg(e.to_string())),
        }
    }
}
