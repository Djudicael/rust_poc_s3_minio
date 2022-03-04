use std::{error::Error, str};

use dotenv::dotenv;
use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::region::Region;
use s3::BucketConfiguration;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv()?;
    let minio_endpoint = env::var("MINIO_ENDPOINT")?;
    let minio_access_key = env::var("MINIO_ACCESS_KEY")?;
    let minio_secret_key = env::var("MINIO_SECRET_KEY")?;
    // 1 instantiate the bucket client
    let bucket = Bucket::new_with_path_style(
        "rust-s3",
        Region::Custom {
            region: "".to_owned(),
            endpoint: minio_endpoint,
        },
        Credentials {
            access_key: Some(minio_access_key),
            secret_key: Some(minio_secret_key),
            security_token: None,
            session_token: None,
        },
    )?;
    // 2 create bucket if doesnt  not exist
    let (_, code) = bucket.head_object("/").await?;
    if code == 404 {
        let create_result = Bucket::create_with_path_style(
            bucket.name.as_str(),
            bucket.region.clone(),
            bucket.credentials.clone(),
            BucketConfiguration::default(),
        )
        .await?;
        println!(
            "==== Bucket created \n{} - {} - {}",
            bucket.name, create_result.response_code, create_result.response_text
        );
    }

    // 3 create object
    let key = "test_file";
    println!("== Put content");
    bucket
        .put_object_with_content_type(key, "Some Stuff!!!".as_bytes(), "test/plain")
        .await?;
    // 4 list bucket content
    println!("=== List bucket content");
    let results = bucket.list("/".to_owned(), Some("/".to_owned())).await?;

    for result in results {
        for item in result.contents {
            println!("Key: {}", item.key);
        }
    }
    // 5 get object content back
    println!("=== Get content");
    let (data, _) = bucket.get_object(key).await?;
    let data = str::from_utf8(&data).expect("wrong data");
    println!("data: {}", data);

    Ok(())
}
