use rand::Rng;
use tokio::sync::{broadcast, mpsc};
use tonic::transport::Server;

pub mod geyser_proto {
    tonic::include_proto!("accountsdb");
}
use geyser_proto::{update::UpdateOneof, SlotUpdate, SubscribeRequest, Update};

pub mod geyser_service {
    use super::*;
    use {
        geyser_proto::accounts_db_server::AccountsDb,
        tokio_stream::wrappers::ReceiverStream,
        tonic::{Request, Response, Status},
    };

    #[derive(Debug)]
    pub struct Service {
        pub sender: broadcast::Sender<Update>,
    }

    impl Service {
        pub fn new() -> Self {
            let (tx, _) = broadcast::channel(100);
            Self { sender: tx }
        }
    }

    #[tonic::async_trait]
    impl AccountsDb for Service {
        type SubscribeStream = ReceiverStream<Result<Update, Status>>;

        async fn subscribe(
            &self,
            _request: Request<SubscribeRequest>,
        ) -> Result<Response<Self::SubscribeStream>, Status> {
            println!("new client");
            let (tx, rx) = mpsc::channel(100);
            let mut broadcast_rx = self.sender.subscribe();
            tokio::spawn(async move {
                loop {
                    let msg = broadcast_rx.recv().await.unwrap();
                    tx.send(Ok(msg)).await.unwrap();
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:10000".parse().unwrap();

    let service = geyser_service::Service::new();
    let sender = service.sender.clone();
    let svc = geyser_proto::accounts_db_server::AccountsDbServer::new(service);

    tokio::spawn(async move {
        let mut slot = 1;
        loop {
            if sender.receiver_count() > 0 {
                println!("sending...");
                slot += 1;
                let parent = slot - rand::thread_rng().gen_range(1..=2);
                sender
                    .send(Update {
                        update_oneof: Some(UpdateOneof::SlotUpdate(SlotUpdate {
                            slot,
                            parent: Some(parent),
                            status: rand::thread_rng().gen_range(0..=2),
                        })),
                    })
                    .unwrap();
            }
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    });

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
