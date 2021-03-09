use actix;
use clap::Clap;

use tokio::sync::mpsc;

use std::sync::{ Arc, Mutex };
use configs::{ init_logging, Opts, SubCommand };
use dotenv::dotenv;


mod configs;
mod capacitor;
mod http_server;
mod indexer;
mod database;
mod balancer_data;

use capacitor::Capacitor;
use http_server::{ start_http_server };
use indexer::{ handle_blocks_message };
use database::{ db_connect };

use near_indexer;

async fn start_process(stream: mpsc::Receiver<near_indexer::StreamerMessage>) {
    let database_client = db_connect().await;
    let mut capacitor_ins = Capacitor::new(database_client, vec![]);
    capacitor_ins.load().await;

    let mutex_capacitor: Mutex<Capacitor> = Mutex::new(capacitor_ins);
    let wrapped_capacitor = Arc::new(mutex_capacitor);

    actix::spawn(handle_blocks_message(wrapped_capacitor.clone(), stream));
    actix::spawn(start_http_server(wrapped_capacitor.clone()));
}

fn main() {
    // We use it to automatically search the for root certificates to perform HTTPS calls
    // (sending telemetry and downloading genesis)
    println!("🚀 Starting flux capacitor");
    openssl_probe::init_ssl_cert_env_vars();
    init_logging();
    dotenv().ok();
    
    let opts: Opts = Opts::parse();
    let home_dir = opts.home_dir.unwrap_or(std::path::PathBuf::from(near_indexer::get_default_home()));
    
    match opts.subcmd {
        SubCommand::Run => {
            let indexer_config = near_indexer::IndexerConfig {
                home_dir,
                sync_mode: near_indexer::SyncModeEnum::FromInterruption,
                await_for_node_synced: near_indexer::AwaitForNodeSyncedEnum::WaitForFullSync
            };
            actix::System::builder()
            .stop_on_panic(true)
            .run(move || {
                let indexer = near_indexer::Indexer::new(indexer_config);
                let stream = indexer.streamer();
                actix::spawn(start_process(stream));
            })
            .unwrap();
        }
        SubCommand::Init(config) => near_indexer::init_configs(
            &home_dir,
            config.chain_id.as_ref().map(AsRef::as_ref),
            config.account_id.as_ref().map(AsRef::as_ref),
            config.test_seed.as_ref().map(AsRef::as_ref),
            config.num_shards,
            config.fast,
            config.genesis.as_ref().map(AsRef::as_ref),
            config.download,
            config.download_genesis_url.as_ref().map(AsRef::as_ref)
        ),
    }
}