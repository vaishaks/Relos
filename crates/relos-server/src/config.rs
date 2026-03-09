#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    pub grpc_port: u16,
    pub node_id: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "127.0.0.1".to_string(),
            grpc_port: 9000,
            node_id: 1,
        }
    }
}

impl ServerConfig {
    /// Returns the full socket address for the gRPC server (e.g. "127.0.0.1:9000").
    pub fn grpc_addr(&self) -> String {
        format!("{}:{}", self.listen_addr, self.grpc_port)
    }
}
