use near_indexer::near_primitives::{
    views::{
        ExecutionOutcomeWithIdView, 
        ExecutionOutcomeView,
        ExecutionStatusView
    },
};
use tokio_stream::StreamExt;
use mongodb::{ Client, Database, options::{ UpdateOptions } };
use bson::{ Bson, doc };
use serde_json::{ Value };
use std::vec::Vec;
use std::convert::TryInto;
use chrono::{ Utc };
use crate::balancer_data::*;
use std::fs::metadata;
use std::error::Error;

pub struct Capacitor {
    capacitor_db: Database,
    database_client: Client,
    allowed_ids: Vec<String>,
}

impl Capacitor {
    pub fn new(database_client: Client, temp_allowed_ids: Vec<String>) -> Self {
        Self {
            capacitor_db: database_client.database("capacitor"),
            allowed_ids: temp_allowed_ids,
            database_client,
        }
    }

    // TODO: Finish function to take processed log and turn it into a BSON compliant doc to be pushed to the db
    // TODO: Convert type to Result<T>
    // Function to turn a processed log into a tuple of instructions on how to insert/update & data to insert into the database
    pub fn prepare_log(log: &Vec<&str>) -> Result<(DBMetadata, String), Box<dyn Error>> {
        let mut metadata: DBMetadata = DBMetadata {
            table: "".to_string(),
            action: DBAction::None,
            primary_key: "".to_string(),
            pk_col: "".to_string()
        };
        let mut log_json = String::from("");

        // log = [function_hash, num_topics, args]
        // match doesn't seem to be able to do this, so if statements have to be used :/
        if log[0] == LOG_NEW_POOL_HASH {
            let pool = Pool {
                id: log[3].to_string(), // TODO: Inspect exact formatting of the logs for this!
                publicSwap: "true".to_string(),
                swapFee: "0.000001".to_string(),
                tokens: vec![],
                tokensList: vec![],
                totalWeight: "0".to_string()
            };
            metadata = DBMetadata {
                table: "pools".to_string(),
                action: DBAction::Create,
                primary_key: log[3].to_string(), // TODO: Inspect exact formatting of logs and make sure this can't panic! Bzw. handle panic
                pk_col: "id".to_string()
            };
            log_json = serde_json::to_string(&pool)?;
        } else if log[0] == LOG_SWAP_HASH {
            // TODO: Handle updating
        }

        Ok((metadata, log_json))
    }

    // Function to process explicit ethereum log and split it into a Vec<&str> as demonstrated in the comment at the start of the function
    pub fn process_explicit_log(log: &String) -> Vec<&str> {
        /*
        Example of log parsing
        038c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925000000000000000000000000cbda96b3f2b8eb962f97ae50c3852ca976740e2b000000000000000000000000db9217df5c41887593e463cfa20036b62a4e331cffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
        03 // number of topics
        8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925 // keccak 256 hash of Approval(address,address,uint256)
        000000000000000000000000cbda96b3f2b8eb962f97ae50c3852ca976740e2b // owner address (my address)
        000000000000000000000000db9217df5c41887593e463cfa20036b62a4e331c // spender address (exchange proxy address)
        ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff // amount (max uint256)
        */

        let num_topics: i8 = if log.len() > 0 {
            log.as_str()[..2].parse::<i8>().unwrap()
        } else {
            0
        };

        // FIXME: This is most likely redundant now that we check this in the generic process log function
        let function_hash: &str = if log.len() > 65 {
            &log.as_str()[2..66]
        } else {
            "No function hash"
        };

        let mut args: Vec<&str> = Vec::new();
        for i in 1..=num_topics {
            let f = 66 + (i as i32 - 1) * 64;
            let t = 66 + i as i32 * 64;

            if log.len() < t as usize {
                args.push("Missing arg");
                continue;
            }

            args.push(&log.as_str()[f as usize..t as usize]);
        }

        let mut processed_log = vec!(function_hash, &log.as_str()[..2]);
        processed_log.append(&mut args);

        processed_log
    }

    // TODO: Unfinished function to split anonymous ethereum logs into a Vec<&str> for further use
    pub fn process_anonymous_log(log: &String) -> Vec<&str> {
        // This will only be implemented for the specific functions that we need to parse for balancer to work properly

        /*
        handleRebind anonymous log. Parsed out properly
        02
        e4e1e53800000000000000000000000000000000000000000000000000000000
        00000000000000000000000051a6a1bf310fd5505d4fa838000049fc646ea223
        0000000000000000000000000000000000000000000000000000000000000020
        0000000000000000000000000000000000000000000000000000000000000064
        e4e1e538
        000000000000000000000000e599045a0a93ff901b995c755f1599db6acd44e6
        0000000000000000000000000000000000000000000000056bc75e2d63100000
        0000000000000000000000000000000000000000000000008ac7230489e80000
        00000000000000000000000000000000000000000000000000000000
        */

        // Anonymous logs that we care bout always have numtopics = 2, so this is probably redundant
        // since it also doesn't influence anonymous logs either, afaik
        let num_topics: i8 = if log.len() > 0 {
            log.as_str()[..2].parse::<i8>().unwrap()
        } else {
            0
        };

        // FIXME: This is most likely redundant now that we check this in the generic process log function
        let function_hash: &str = if log.len() > 65 {
            &log.as_str()[2..66]
        } else {
            "No function hash"
        };

        let mut args: Vec<&str> = Vec::new();
        for i in 1..=num_topics {
            let f = 66 + (i as i32 - 1) * 64;
            let t = 66 + i as i32 * 64;

            if log.len() < t as usize {
                args.push("Missing arg");
                continue;
            }

            args.push(&log.as_str()[f as usize..t as usize]);
        }

        let mut processed_log = vec!(function_hash, &log.as_str()[..2]);
        processed_log.append(&mut args);

        processed_log
    }

    // Generic EVM log processing function that calls the appropriate function to process the log, then returns its result
    pub fn process_log(log: &String) -> Vec<&str> {
        if log == &String::from("") {
            return vec!("No logs for this transaction.")
        }

        if log.len() > 65 {
            match &log.as_str()[2..66] {
                APPROVAL_HASH | LOG_NEW_POOL_HASH | LOG_JOIN_POOL_HASH | LOG_EXIT_POOL_HASH |
                LOG_SWAP_HASH | TRANSFER_HASH | OWNERSHIP_TRANSFERRED_HASH => process_explicit_log(log),
                LOG_HANDLE_SET_SWAP_FEE_HASH | LOG_HANDLE_SET_CONTROLLER_HASH | LOG_HANDLE_SET_PUBLIC_SWAP_HASH |
                LOG_HANDLE_FINALIZE_HASH | LOG_HANDLE_REBIND1_HASH | LOG_HANDLE_REBIND2_HASH | LOG_HANDLE_UNBIND_HASH => process_anonymous_log(log),
                _ => vec!("Something went wrong. function hash not recognised. Raw log: {}", log)
            }
        } else {
            // This must be some sort of erroneous behaviour.
            vec!("Something went wrong. log length is too short to contain a function hash")
        }
    }

    // Debug function that takes as an input an EVM Log and returns information about the log in human-readable form
    pub fn inspect_log(log: &String) -> String {
        let mut log_message: String = String::from("");

        if log == &String::from("") {
            return String::from("No logs for this transaction.");
        }

        let num_topics: i8 = if log.len() > 0 {
            log.as_str()[..2].parse::<i8>().unwrap()
        } else {
            0
        };

        let function_hash: &str = if log.len() > 65 {
            &log.as_str()[2..66]
        } else {
            // This must be some sort of erroneous behaviour.
            "No function hash"
        };

        let mut args: Vec<&str> = Vec::new();
        for i in 1..=num_topics {
            let f = 66 + (i as i32 - 1) * 64;
            let t = 66 + i as i32 * 64;

            if log.len() < t as usize {
                args.push("Missing arg");
                continue;
            }

            args.push(&log.as_str()[f as usize..t as usize]);
        }

        log_message = match function_hash {
            APPROVAL_HASH => format!("Event: Approval, Owner: {}, Spender: {}, Amount: {}", args.get(0).unwrap_or(&"No arg provided."), args.get(1).unwrap_or(&"No arg provided."), args.get(2).unwrap_or(&"No arg provided.")),
            TRANSFER_HASH => format!("Event: Transfer, From: {}, To: {}, Amount: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_NEW_POOL_HASH => format!("Event: LOG_NEW_POOL, Address 1: {}, Address 2: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided.")),
            LOG_JOIN_POOL_HASH => format!("Event: LOG_JOIN, Address 1 (Sender?): {}, Address 2 (Pool?): {}, Amount(?): {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), args.get(2).unwrap_or(&"No arg provided.")),
            LOG_EXIT_POOL_HASH => format!("Event: LOG_EXIT, Address 2 (Sender?): {}, Address 2 (Pool?): {}, Amount(?): {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), args.get(2).unwrap_or(&"No arg provided.")),
            LOG_SWAP_HASH => format!("Event: LOG_SWAP, Address 1: {}, Address 2: {}, Address 3: {}, uint256 1 (Swap In?): {}, uint256 2 (Swap Out?): {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided."), args.get(3).unwrap_or(&"No arg provided."), args.get(4).unwrap_or(&"No arg provided.")),
            OWNERSHIP_TRANSFERRED_HASH => format!("Event: OwnershipTransferred, Address 1: {}, Address 2: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_SET_SWAP_FEE_HASH => format!("Event: handleSetSwapFee (Anonymous), bytes4: {}, Address: {}, bytes: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_SET_CONTROLLER_HASH => format!("Event: handleSetController (Anonymous), bytes4: {}, Address: {}, bytes: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_SET_PUBLIC_SWAP_HASH => format!("Event: handleSetPublicSwap (Anonymous), bytes4: {}, Address: {}, bytes: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_FINALIZE_HASH => format!("Event: handleFinalize (Anonymous), bytes4: {}, Address: {}, bytes: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_REBIND1_HASH => format!("Event: handleRebind (Anonymous), function hash: {} bytes4: {}, Address: {}, bytes: {}", function_hash, &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_REBIND2_HASH => format!("Event: handleRebind (Anonymous), function hash: {} bytes4: {}, Address: {}, bytes: {}", function_hash, &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            LOG_HANDLE_UNBIND_HASH => format!("Event: handleUnbind (Anonymous), bytes4: {}, Address: {}, bytes: {}", &args.get(0).unwrap_or(&"No arg provided."), &args.get(1).unwrap_or(&"No arg provided."), &args.get(2).unwrap_or(&"No arg provided.")),
            "No function hash" => format!("Event: (Unknown - No function hash), Raw log: {}", log),
            _ => format!("Event: (Unknown) {}, Num. Topics: {}, Args: {:?}", function_hash, num_topics, args)
        };
        log_message.push_str(&*format!("\n Raw log: {}", &log));

        log_message
    }

    pub async fn load(&mut self) {
        let allowed_collection = self.capacitor_db.collection("allowed_account_ids");
        let mut cursor = allowed_collection.find(None, None).await.unwrap();

        while let Some(doc) = cursor.next().await {
            let allowed_doc = doc.unwrap();
            let account_id = allowed_doc.get("account_id").and_then(Bson::as_str).unwrap();

            if !self.allowed_ids.contains(&account_id.to_string()) {
                self.allowed_ids.push(account_id.to_string());
            }
        }

        println!("📝 Listening for the following contracts: {:?}", self.allowed_ids);
    }

    pub async fn add_account_id(&mut self, account_id: String) {
        let allowed_collection = self.capacitor_db.collection("allowed_account_ids");
        let doc = doc! {
            "account_id": account_id.to_string(),
        };

        let mut cursor = allowed_collection.find(doc.clone(), None).await.unwrap();

        while let Some(doc) = cursor.next().await {
            let allowed_doc = doc.unwrap();
            let doc_account_id = allowed_doc.get("account_id").and_then(Bson::as_str).unwrap();

            if doc_account_id == account_id {
                return ();
            }
        }

        allowed_collection.insert_one(doc.clone(), None).await.unwrap();
        self.allowed_ids.push(account_id.to_string());
    }

    pub fn is_valid_receipt(&self, execution_outcome: &ExecutionOutcomeWithIdView) -> bool {
        match &execution_outcome.outcome.status {
            ExecutionStatusView::SuccessValue(_) => (),
            ExecutionStatusView::SuccessReceiptId(_) => (),
            _ => return false
        }

        self.allowed_ids.contains(&execution_outcome.outcome.executor_id)
    }

    pub async fn process_outcome(&self, outcome: ExecutionOutcomeView) {
        println!("🤖 Processing logs for {}", &outcome.executor_id);
        let normalized_database_name = outcome.executor_id.replace(".", "_");
        let database = self.database_client.database(&normalized_database_name);

        for log in outcome.logs {
            let processed_log = Capacitor::process_log(&log);
            let (metadata, log_json) = Capacitor::prepare_log(&processed_log);
            let bson_data: Bson = log_json.try_into().unwrap();
            let mut doc = bson::to_document(&bson_data).unwrap();
            let collection = database.collection(metadata.table.as_str());
            let creation_date_time = Utc::now();
            doc.insert("cap_creation_date", creation_date_time);

            if metadata.action == DBAction::Update {
                let options = UpdateOptions::builder().upsert(true).build();
                let update_doc = doc! {
                        "$set": bson_data,
                    };

                collection.update_one(doc!{ metadata.pk_col: metadata.primary_key }, update_doc, options) // FIXME: We don't currently use cap_id, this should be the respective PK!
                    .await                                                           // I am thinking of passing this via the metadata struct.
                    .unwrap_or_else(|_| panic!("🛑 Database could not insert document"));
            } else {
                collection.insert_one(doc, None)
                    .await
                    .unwrap_or_else(|_| panic!("🛑 Database could not insert document"));
            }

            /*
             Old capacitor code
            let logs_parse_res: Option<Value> = serde_json::from_str(log.as_str()).unwrap();
            if logs_parse_res.is_some() {
                let parsed_logs: Value = serde_json::from_str(log.as_str()).unwrap();
                let log_type = &parsed_logs["type"].as_str().unwrap().to_string();
                let cap_id = &parsed_logs["cap_id"].as_str().unwrap_or("None").to_string();
                let action_type = &parsed_logs["action"].as_str().unwrap_or("write").to_string();
                let collection = database.collection(log_type);
    
                let stringified_params = serde_json::to_string(&parsed_logs["params"]).unwrap();
                let parsed_params: Value = serde_json::from_str(&stringified_params).unwrap();
    
                let data: Bson = parsed_params.try_into().unwrap();
                let mut doc = bson::to_document(&data).unwrap();
                let creation_date_time = Utc::now();
                
                doc.insert("cap_creation_date", creation_date_time);
                doc.insert("cap_id", cap_id);
    
                if action_type == "update" {
                    let options = UpdateOptions::builder().upsert(true).build();
                    let update_doc = doc! {
                        "$set": data,
                    };
    
                    collection.update_one(doc!{ "cap_id": cap_id }, update_doc, options)
                        .await
                        .unwrap_or_else(|_| panic!("🛑 Database could not insert document"));
                } else {
                    collection.insert_one(doc, None)
                        .await
                        .unwrap_or_else(|_| panic!("🛑 Database could not insert document"));
                }
            }
            */
        }
    }
}
