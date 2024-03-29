use crate::cache::read_robots;
use feedfinder::{detect_feeds, Feed};
use texting_robots::{get_robots_url, Robot};
use url::Url;

#[derive(Debug)]
pub struct SiteInfo {
    pub domain: String,
    pub feeds: Vec<Feed>,
    pub robots_txt: Option<String>,
    pub robot: Option<Robot>,
    pub sitemaps: Vec<String>,
}

impl SiteInfo {
    pub async fn new(domain: &str) -> anyhow::Result<Self> {
        let domain_url = if domain.starts_with("http") {
            domain.to_string()
        } else {
            format!("http://{domain}")
        };

        let mut feeds = Vec::new();
        let url = Url::parse(&domain_url)?;
        if let Ok(resp) = reqwest::get(url).await {
            let url = resp.url().clone();
            if let Ok(html) = resp.text().await {
                if let Ok(detected) = detect_feeds(&url, &html) {
                    feeds.extend(detected);
                }
            }
        }

        let robots_txt = get_robots_url(&domain_url).ok();
        let mut robot = None;
        let mut sitemaps: Vec<String> = Vec::new();
        if let Some(ref robots_txt) = robots_txt {
            if let Ok(Some(bot)) = read_robots(robots_txt).await {
                sitemaps.extend(bot.sitemaps.clone());
                robot = Some(bot);
            }
        }

        // Always try sitemap at root
        if sitemaps.is_empty() {
            let root_sitemap = format!("{domain_url}sitemap.xml");
            match reqwest::get(root_sitemap.clone()).await {
                Ok(response) => {
                    if response.error_for_status().is_ok() {
                        log::info!("Found root sitemap: {}", root_sitemap);
                        sitemaps.push(root_sitemap);
                    } else {
                        log::info!("no sitemaps found for {domain_url}");
                    }
                }
                Err(_) => {
                    log::info!("no sitemaps found for {domain_url}");
                }
            }
        }

        Ok(Self {
            domain: domain.to_owned(),
            feeds,
            robots_txt: robots_txt.clone(),
            robot,
            sitemaps,
        })
    }

    pub fn print(&self) {
        println!("Domain: {}", self.domain);
        println!(
            "Robots: {}",
            self.robots_txt.as_ref().unwrap_or(&"N/A".to_string())
        );

        println!("\n== Feeds ({}) ==", self.feeds.len());
        for feed in &self.feeds {
            println!(
                "- {}:{:?}:{}",
                feed.title().unwrap_or_default(),
                feed.feed_type(),
                feed.url()
            );
        }

        println!("\n== Sitemaps ({}) ==", self.sitemaps.len());
        for sm in &self.sitemaps {
            println!("- {sm}");
        }
    }
}
