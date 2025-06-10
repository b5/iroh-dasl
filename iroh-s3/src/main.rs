use std::str::FromStr;

use anyhow::{Context, Result};
use iroh_docs::DocTicket;
use iroh_s3::bucket::Config;
use iroh_s3::http::serve_s3_api;
use iroh_s3::node::Node;
use s3::creds::Credentials;
use s3::region::Region;

const DOC_TICKET: &str = "docaaacakqzgxe3lo3xom36oco3in3ynaeioipmnaxjz5faiz4rc2utoay7aea54kekl2oa7yd5ro3u65kynwolq3h2msu65c3jfj44ja23fstmcaaa";

#[tokio::main]
async fn main() -> Result<()> {
    let ticket = DocTicket::from_str(DOC_TICKET).unwrap();
    let bucket_configs = vec![Config {
        name: "number0".to_string(),
        region: Region::UsEast1,
        credentials: Credentials {
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
            expiration: None,
        },
        ticket,
    }];

    println!("Starting...");
    let node = Node::new(bucket_configs)
        .await
        .context("failed to start node")?;
    let addr = std::net::SocketAddr::from(([127, 0, 0, 1], 8181));
    let dir = std::path::PathBuf::from("data");
    let server = serve_s3_api(addr, &dir, node).await?;
    server.with_graceful_shutdown(shutdown_signal()).await?;

    // let bucket = node.get_bucket("number0").expect("missing bucket");
    // let res = bucket
    //     .get_object("hello.txt")
    //     .await
    //     .context("failed to get_object")?;
    // println!("status={}", res.status_code());
    // let data = res.to_string().await?;
    // println!("get_object: data={}", data);

    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
