use chrono::{Duration, Utc};
use reqwest::{Client, Error};
use std::collections::HashSet;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;

static ARCHIVE_CDX_ENDPOINT: &str = "https://web.archive.org/cdx/search/cdx";

type CDXResumeKey = Option<String>;
type FetchCDXResult = anyhow::Result<(HashSet<String>, CDXResumeKey)>;

const ARCHIVE_WEB_ENDPOINT: &str = "https://web.archive.org/web";

pub fn create_archive_url(url: &str) -> String {
    // Always try to grab the latest archived crawl
    let date = Utc::now();
    format!(
        "{}/{}000000id_/{}",
        ARCHIVE_WEB_ENDPOINT,
        date.format("%Y%m%d"),
        url
    )
}

pub async fn fetch_cdx(
    client: &Client,
    prefix: &str,
    limit: usize,
    resume_key: Option<String>,
) -> FetchCDXResult {
    let last_year = Utc::now() - Duration::weeks(52);
    let last_year = last_year.format("%Y").to_string();

    // More docs on parameters here:
    // https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server#filtering
    let mut params: Vec<(String, String)> = vec![
        // TODO: Make this configurable in the lens format?
        ("matchType".into(), "prefix".into()),
        // Only successful pages
        ("filter".into(), "statuscode:200".into()),
        // Only HTML docs
        ("filter".into(), "mimetype:text/html".into()),
        // Remove dupes
        ("collapse".into(), "urlkey".into()),
        // If there are too many URLs, let's us paginate
        ("showResumeKey".into(), "true".into()),
        ("limit".into(), limit.to_string()),
        // Only care about the original URL crawled
        ("fl".into(), "original".into()),
        // Only look at things that have existed within the last year.
        ("from".into(), last_year),
        ("url".into(), prefix.into()),
    ];

    if let Some(resume) = resume_key {
        params.push(("resumeKey".into(), resume));
    }

    let response = fetch_cdx_page(client, params).await?;

    let mut urls = HashSet::new();
    let mut resume_key = None;

    for url in response.split('\n') {
        if url.is_empty() {
            continue;
        }

        // Text after the limit num is the resume key
        if urls.len() >= limit {
            resume_key = Some(url.to_string());
        } else {
            urls.insert(url.into());
        }
    }

    Ok((urls, resume_key))
}

async fn fetch_cdx_page(
    client: &Client,
    params: Vec<(String, String)>,
) -> anyhow::Result<String, Error> {
    let retry_strat = ExponentialBackoff::from_millis(1000).take(3);
    // If we're hitting the CDX endpoint too fast, wait a little bit before retrying.
    Retry::spawn(retry_strat, || async {
        let req = client.get(ARCHIVE_CDX_ENDPOINT).query(&params);
        let resp = req.send().await;
        match resp {
            Ok(resp) => resp.text().await,
            Err(err) => Err(err),
        }
    })
    .await
}
