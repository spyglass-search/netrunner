use rusoto_core::Region;
use rusoto_s3::{PutObjectRequest, S3Client, StreamingBody, S3};
use std::path::Path;
use tokio::io::AsyncReadExt;

pub async fn upload_to_bucket(file: &Path, s3_bucket: &str, key: &str) -> anyhow::Result<()> {
    log::info!(
        "uploading parsed archive to bucket: {}, key: {}",
        s3_bucket,
        key
    );

    let client = S3Client::new(Region::UsEast1);
    let mut file = tokio::fs::File::open(file).await?;
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).await?;

    let _ = client
        .put_object(PutObjectRequest {
            bucket: s3_bucket.into(),
            key: key.to_string(),
            body: Some(StreamingBody::from(buffer)),
            ..Default::default()
        })
        .await?;

    Ok(())
}
