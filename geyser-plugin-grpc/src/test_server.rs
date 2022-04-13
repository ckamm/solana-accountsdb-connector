use tonic;
use nats;
use geyser_proto::{
    accounts_db_client::AccountsDbClient,
    SubscribeRequest,
    update::UpdateOneof,
};
use prost::Message;


pub mod geyser_proto {
    tonic::include_proto!("accountsdb");
}

const NATS_TX_SUBJECT: &str = "transactions";
const NATS_SERVER_URL: &str = "demo.nats.io"; // Default localhost url
const GRPC_SERVER_URL: &str = "http://127.0.0.1:10000";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Starting transaction consumer...");

    let opts = nats::asynk::Options::new();
    let nc = opts.with_name("nats_bench rust client").connect(NATS_SERVER_URL).await?;
    
    println!("Connected to NATS server");
    let mut client = AccountsDbClient::connect(GRPC_SERVER_URL).await?;

    let request = tonic::Request::new(SubscribeRequest{});
    let mut streaming_response = client.subscribe(request).await?.into_inner();

    println!("Connected to gRPC server\nForwarding transcations to NATS Subject {}", NATS_TX_SUBJECT);

    while let Some(update) = streaming_response.message().await? {
        if let Some(oneof) = update.update_oneof {
            if let UpdateOneof::Transaction(tx) = oneof {
                // Publish encoded protobuf to NATS
                let mut buf: Vec<u8> = Vec::with_capacity(tx.encoded_len());
                tx.encode(&mut buf).unwrap();
                nc.publish(NATS_TX_SUBJECT, buf).await?;
            }
        }
    }

    Ok(())
}
