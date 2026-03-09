#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub node_id: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1:9000".to_string(),
            node_id: 1,
        }
    }
}
