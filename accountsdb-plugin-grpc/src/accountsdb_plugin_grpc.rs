use {
    crate::accounts_selector::AccountsSelector,
    accountsdb_proto::{
        slot_update::Status as SlotUpdateStatus, update::UpdateOneof, AccountWrite, SlotUpdate,
        SubscribeRequest, Update,
    },
    bs58,
    futures_util::FutureExt,
    log::*,
    serde_derive::{Deserialize, Serialize},
    serde_json,
    solana_accountsdb_plugin_interface::accountsdb_plugin_interface::{
        AccountsDbPlugin, AccountsDbPluginError, ReplicaAccountInfoVersions,
        Result as PluginResult, SlotStatus,
    },
    std::sync::Mutex,
    std::{fs::File, io::Read},
    thiserror::Error,
    tokio::sync::{broadcast, mpsc, oneshot},
    tonic::transport::Server,
};

pub mod accountsdb_proto {
    tonic::include_proto!("accountsdb");
}

pub mod accountsdb_service {
    use super::*;
    use {
        accountsdb_proto::accounts_db_server::{AccountsDb, AccountsDbServer},
        tokio_stream::wrappers::ReceiverStream,
        tonic::{Code, Request, Response, Status},
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
                let mut exit = false;
                while !exit {
                    let fwd = broadcast_rx.recv().await.map_err(|err| {
                        // TODO: Deal with lag! though maybe just close if RecvError::Lagged happens
                        warn!("error while receiving message to be broadcast: {:?}", err);
                        exit = true;
                        Status::new(Code::Internal, err.to_string())
                    });
                    if let Err(err) = tx.send(fwd).await {
                        info!("subscriber stream closed");
                        exit = true;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }
}

#[derive(Default)]
pub struct AccountsDbPluginGrpc {
    server_broadcast: Option<broadcast::Sender<Update>>,
    server_exit_sender: Option<oneshot::Sender<()>>,
    accounts_selector: Option<AccountsSelector>,
}

impl std::fmt::Debug for AccountsDbPluginGrpc {
    fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AccountsDbPluginGrpcConfig {
    pub bind_string: String,
}

#[derive(Error, Debug)]
pub enum AccountsDbPluginGrpcError {}

impl AccountsDbPluginGrpc {
    fn broadcast(&self, update: UpdateOneof) {
        if let Some(sender) = &self.server_broadcast {
            // Don't care about the error if there are no receivers.
            let _ = sender.send(Update {
                update_oneof: Some(update),
            });
        }
    }
}

impl AccountsDbPlugin for AccountsDbPluginGrpc {
    fn name(&self) -> &'static str {
        "AccountsDbPluginGrpc"
    }

    fn on_load(&mut self, config_file: &str) -> PluginResult<()> {
        solana_logger::setup_with_default("info");
        info!(
            "Loading plugin {:?} from config_file {:?}",
            self.name(),
            config_file
        );
        let mut file = File::open(config_file)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;

        let result: serde_json::Value = serde_json::from_str(&contents).unwrap();
        self.accounts_selector = Some(Self::create_accounts_selector_from_config(&result));

        let config: AccountsDbPluginGrpcConfig =
            serde_json::from_str(&contents).map_err(|err| {
                AccountsDbPluginError::ConfigFileReadError {
                    msg: format!(
                        "The config file is not in the JSON format expected: {:?}",
                        err
                    ),
                }
            })?;

        let addr = config.bind_string.parse().map_err(|err| {
            AccountsDbPluginError::ConfigFileReadError {
                msg: format!("Error parsing the bind_string {:?}", err),
            }
        })?;

        let service = accountsdb_service::Service::new();
        let (exit_sender, exit_receiver) = oneshot::channel::<()>();
        self.server_exit_sender = Some(exit_sender);
        self.server_broadcast = Some(service.sender.clone());

        let server = accountsdb_proto::accounts_db_server::AccountsDbServer::new(service);
        tokio::spawn(
            Server::builder()
                .add_service(server)
                .serve_with_shutdown(addr, exit_receiver.map(drop)),
        );

        Ok(())
    }

    fn on_unload(&mut self) {
        info!("Unloading plugin: {:?}", self.name());

        if let Some(sender) = self.server_exit_sender.take() {
            sender
                .send(())
                .expect("sending grpc server termination should succeed");
        }
    }

    fn update_account(
        &mut self,
        account: ReplicaAccountInfoVersions,
        slot: u64,
        is_startup: bool,
    ) -> PluginResult<()> {
        match account {
            ReplicaAccountInfoVersions::V0_0_1(account) => {
                if let Some(accounts_selector) = &self.accounts_selector {
                    if !accounts_selector.is_account_selected(account.pubkey, account.owner) {
                        return Ok(());
                    }
                } else {
                    return Ok(());
                }

                debug!(
                    "Updating account {:?} with owner {:?} at slot {:?} using account selector {:?}",
                    bs58::encode(account.pubkey).into_string(),
                    bs58::encode(account.owner).into_string(),
                    slot,
                    self.accounts_selector.as_ref().unwrap()
                );

                // TODO: send the update to all connected streams
                self.broadcast(UpdateOneof::AccountWrite(AccountWrite {
                    slot,
                    is_startup,
                    write_version: account.write_version,
                    pubkey: account.pubkey.to_vec(),
                    lamports: account.lamports,
                    owner: account.owner.to_vec(),
                    executable: account.executable,
                    rent_epoch: account.rent_epoch,
                    data: account.data.to_vec(),
                }));
            }
        }
        Ok(())
    }

    fn update_slot_status(
        &mut self,
        slot: u64,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        info!("Updating slot {:?} at with status {:?}", slot, status);

        let status = match status {
            SlotStatus::Processed => SlotUpdateStatus::Processed,
            SlotStatus::Confirmed => SlotUpdateStatus::Confirmed,
            SlotStatus::Rooted => SlotUpdateStatus::Rooted,
        };
        self.broadcast(UpdateOneof::SlotUpdate(SlotUpdate {
            slot,
            parent,
            status: status as i32,
        }));

        Ok(())
    }

    fn notify_end_of_startup(&mut self) -> PluginResult<()> {
        Ok(())
    }
}

impl AccountsDbPluginGrpc {
    fn create_accounts_selector_from_config(config: &serde_json::Value) -> AccountsSelector {
        let accounts_selector = &config["accounts_selector"];

        if accounts_selector.is_null() {
            AccountsSelector::default()
        } else {
            let accounts = &accounts_selector["accounts"];
            let accounts: Vec<String> = if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            let owners = &accounts_selector["owners"];
            let owners: Vec<String> = if owners.is_array() {
                owners
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                Vec::default()
            };
            AccountsSelector::new(&accounts, &owners)
        }
    }

    pub fn new() -> Self {
        AccountsDbPluginGrpc {
            server_broadcast: None,
            server_exit_sender: None,
            accounts_selector: None,
        }
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the AccountsDbPluginGrpc pointer as trait AccountsDbPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn AccountsDbPlugin {
    let plugin = AccountsDbPluginGrpc::new();
    let plugin: Box<dyn AccountsDbPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}

#[cfg(test)]
pub(crate) mod tests {
    use {super::*, serde_json};

    #[test]
    fn test_accounts_selector_from_config() {
        let config = "{\"accounts_selector\" : { \
           \"owners\" : [\"9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin\"] \
        }}";

        let config: serde_json::Value = serde_json::from_str(config).unwrap();
        AccountsDbPluginGrpc::create_accounts_selector_from_config(&config);
    }
}
