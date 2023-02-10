use governor::clock::QuantaClock;
use governor::state::keyed::DashMapStateStore;
use governor::RateLimiter;
use reqwest::{Client, StatusCode};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio_retry::strategy::ExponentialBackoff;
use tokio_retry::Retry;

use crate::archive::ArchiveRecord;
use crate::cdx::create_archive_url;

static APP_USER_AGENT: &str = concat!("netrunner", "/", env!("CARGO_PKG_VERSION"));
const RETRY_DELAY_MS: u64 = 5000;

type RateLimit = RateLimiter<String, DashMapStateStore<String>, QuantaClock>;

pub fn http_client() -> Client {
    // Use a normal user-agent otherwise some sites won't let us crawl
    reqwest::Client::builder()
        .gzip(true)
        .user_agent(APP_USER_AGENT)
        .connect_timeout(Duration::from_secs(1))
        .timeout(Duration::from_secs(10))
        .build()
        .expect("Unable to create HTTP client")
}

pub async fn handle_crawl(
    client: &Client,
    tmp_storage: PathBuf,
    lim: Arc<RateLimit>,
    url: &url::Url,
) {
    // URL to Wayback Machine
    let ia_url = create_archive_url(url.as_ref());

    let domain = url.domain().expect("No domain in URL");

    let retry_strat = ExponentialBackoff::from_millis(100)
        .max_delay(Duration::from_secs(5))
        .take(3);

    // Retry if we run into 429 / timeout errors
    let web_archive = Retry::spawn(retry_strat.clone(), || async {
        log::info!("trying to fetch from IA");
        // Wait for when we can crawl this based on the domain
        lim.until_key_ready(&domain.to_string()).await;
        fetch_page(client, &ia_url, Some(url.to_string()), &tmp_storage).await
    })
    .await;
    // If we fail trying to get the page from the web archive, hit the
    // site directly.
    if web_archive.is_err() {
        let retry_strat = ExponentialBackoff::from_millis(100)
            .max_delay(Duration::from_secs(5))
            .take(3);
        let _ = Retry::spawn(retry_strat, || async {
            log::info!("trying to fetch from origin");
            // Wait for when we can crawl this based on the domain
            lim.until_key_ready(&domain.to_string()).await;
            fetch_page(client, url.as_ref(), None, &tmp_storage).await
        })
        .await;
    }
}

async fn fetch_page(
    client: &Client,
    url: &str,
    url_override: Option<String>,
    page_store: &Path,
) -> Result<(), ()> {
    // Wait for when we can crawl this based on the domain
    match client.get(url).send().await {
        Ok(resp) => {
            if resp.status() == StatusCode::TOO_MANY_REQUESTS {
                let retry_after_ms: u64 =
                    resp.headers()
                        .get("Retry-After")
                        .map_or(RETRY_DELAY_MS, |header| {
                            if let Ok(header) = header.to_str() {
                                log::warn!("found Retry-After: {}", header);
                                header.parse::<u64>().unwrap_or(RETRY_DELAY_MS)
                            } else {
                                RETRY_DELAY_MS
                            }
                        });

                log::info!("429 received... retrying after {}ms", retry_after_ms);
                tokio::time::sleep(tokio::time::Duration::from_millis(retry_after_ms)).await;

                Err(())
            } else if let Err(err) = resp.error_for_status_ref() {
                log::error!("Unable to fetch [{:?}] {} - {}", err.status(), url, err);
                Err(())
            } else {
                log::info!("fetched {}: {}", resp.status(), url);
                // Save response to tmp storage
                if let Ok(record) = ArchiveRecord::from_response(resp, url_override).await {
                    if let Ok(serialized) = ron::to_string(&record) {
                        // Hash the URL to store in the cache
                        let mut hasher = DefaultHasher::new();
                        record.url.hash(&mut hasher);
                        let id = hasher.finish().to_string();
                        let file = page_store.join(id);
                        let _ = std::fs::write(file.clone(), serialized);
                        log::debug!("cached <{}> -> <{}>", record.url, file.display());
                    }
                }

                Ok(())
            }
        }
        Err(err) => {
            log::error!("Unable to fetch [{:?}] {} - {}", err.status(), url, err);
            Err(())
        }
    }
}

#[cfg(test)]
mod test {
    use super::{handle_crawl, http_client};
    use governor::{Quota, RateLimiter};
    use nonzero_ext::nonzero;
    use std::io;
    use std::{path::Path, sync::Arc};
    use tracing_log::LogTracer;
    use tracing_subscriber::EnvFilter;
    use tracing_subscriber::{fmt, prelude::__tracing_subscriber_SubscriberExt};
    use url::Url;

    #[tokio::test]
    #[ignore = "live http request"]
    async fn test_handle_404() {
        // Setup some nice console logging
        let subscriber = tracing_subscriber::registry()
            .with(
                EnvFilter::from_default_env()
                    .add_directive("libnetrunner=DEBUG".parse().expect("Invalid log filter")),
            )
            .with(fmt::Layer::new().with_writer(io::stdout));
        tracing::subscriber::set_global_default(subscriber)
            .expect("Unable to set a global subscriber");
        LogTracer::init().expect("Unable to create logger");

        let client = http_client();
        let path = Path::new("/tmp");
        let quota = Quota::per_second(nonzero!(2u32));
        let lim = Arc::new(RateLimiter::<String, _, _>::keyed(quota));

        // Known to 404 in the web archive, but actually exists.
        let url = Url::parse(
            "https://developers.home-assistant.io/blog/2020/05/08/logos-custom-integrations",
        )
        .expect("Invalid URL");

        handle_crawl(&client, path.to_path_buf(), lim.clone(), &url).await;
    }
}
