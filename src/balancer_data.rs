use serde::{Deserialize, Serialize};

// Event name hashes
pub(crate) const APPROVAL_HASH: &str = "8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925";
pub(crate) const LOG_NEW_POOL_HASH: &str = "8ccec77b0cb63ac2cafd0f5de8cdfadab91ce656d262240ba8a6343bccc5f945";
pub(crate) const LOG_JOIN_POOL_HASH: &str = "63982df10efd8dfaaaa0fcc7f50b2d93b7cba26ccc48adee2873220d485dc39a";
pub(crate) const LOG_EXIT_POOL_HASH: &str = "e74c91552b64c2e2e7bd255639e004e693bd3e1d01cc33e65610b86afcc1ffed";
pub(crate) const LOG_SWAP_HASH: &str = "908fb5ee8f16c6bc9bc3690973819f32a4d4b10188134543c88706e0e1d43378";
pub(crate) const TRANSFER_HASH: &str = "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
pub(crate) const OWNERSHIP_TRANSFERRED_HASH: &str = "8be0079c531659141344cd1fd0a4f28419497f9722a3daafe3b4186f6b6457e0";

// Anonymous Event Hashes
pub(crate) const LOG_HANDLE_SET_SWAP_FEE_HASH: &str= "34e1990700000000000000000000000000000000000000000000000000000000";
pub(crate) const LOG_HANDLE_SET_CONTROLLER_HASH: &str= "92eefe9b00000000000000000000000000000000000000000000000000000000";
pub(crate) const LOG_HANDLE_SET_PUBLIC_SWAP_HASH: &str= "49b5955200000000000000000000000000000000000000000000000000000000";
pub(crate) const LOG_HANDLE_FINALIZE_HASH: &str= "4bb278f300000000000000000000000000000000000000000000000000000000";
pub(crate) const LOG_HANDLE_REBIND1_HASH: &str= "3fdddaa200000000000000000000000000000000000000000000000000000000";
pub(crate) const LOG_HANDLE_REBIND2_HASH: &str= "e4e1e53800000000000000000000000000000000000000000000000000000000"; // why are there two?
pub(crate) const LOG_HANDLE_UNBIND_HASH: &str = "cf5e7bd300000000000000000000000000000000000000000000000000000000";

// Data Structures

#[derive(Debug)]
pub enum DBAction {
    Create,
    Update,
    None
}

#[derive(Debug)]
pub struct DBMetadata {
    pub table: String,
    pub action: DBAction,
    pub primary_key: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Token {
    pub address: String,
    pub balance: String,
    pub decimals: String,
    pub denormWeight: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Pool {
    pub id: String,
    pub publicSwap: String,
    pub swapFee: String,
    pub tokens: Vec<Token>,
    pub tokensList: Vec<String>,
    pub totalWeight: String
}
