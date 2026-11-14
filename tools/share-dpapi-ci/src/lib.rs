extern crate self as pi_rs;
pub type Result<T> = std::result::Result<T, std::io::Error>;
pub struct Session;
impl Session {
    pub fn session_id(&self) -> uuid::Uuid { uuid::Uuid::nil() }
    pub fn head(&self) -> u64 { 0 }
    pub fn records(&self) -> Vec<serde_json::Value> { Vec::new() }
}
pub mod private_share_store;
