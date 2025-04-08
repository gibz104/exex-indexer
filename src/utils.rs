use crate::table_definitions::TABLES;
use alloy_eips::eip1559::ETHEREUM_BLOCK_GAS_LIMIT;
use reth::builder::NodeTypes;
use reth_node_api::FullNodeComponents;
use reth_rpc::{EthApi, TraceApi};
use reth_rpc_eth_api::helpers::{Call, LoadPendingBlock};
use reth_rpc_eth_types::{EthStateCache, GasPriceOracle, FeeHistoryCache, FeeHistoryCacheConfig};
use reth_rpc_server_types::constants::{
    DEFAULT_ETH_PROOF_WINDOW,
    DEFAULT_MAX_SIMULATE_BLOCKS,
    DEFAULT_PROOF_PERMITS,
};
use reth_tasks::pool::{BlockingTaskGuard, BlockingTaskPool};
use reth_tracing::tracing::info;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, collections::HashMap, env};
use tokio_postgres::{Client, NoTls};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub enabled_events: HashMap<String, bool>,
    #[serde(flatten)]
    pub dataset_configs: HashMap<String, serde_yaml::Value>,
}

impl Config {
    pub fn load() -> eyre::Result<Self> {
        // The path is relative to this file
        let config_str = include_str!("../config.yaml");
        let config = serde_yaml::from_str(config_str)?;
        Ok(config)
    }

    pub fn is_event_enabled(&self, event_name: &str) -> bool {
        self.enabled_events.get(event_name).cloned().unwrap_or(true)
    }

    pub fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> eyre::Result<T> {
        self.dataset_configs.get(key)
            .ok_or_else(|| eyre::eyre!("No config found for {}", key))
            .and_then(|v| serde_yaml::from_value(v.clone())
                .map_err(|e| eyre::eyre!("Failed to parse config for {}: {}", key, e)))
    }
}

/// Sanitize a byte slice to ensure it's valid UTF-8.
pub(crate) fn sanitize_bytes(input: &[u8]) -> String {
    // First, remove all null bytes
    let without_nulls: Vec<u8> = input.iter().filter(|&&b| b != 0).cloned().collect();

    // Then, convert to a string, replacing any invalid UTF-8 sequences
    String::from_utf8_lossy(&without_nulls).into_owned()
}

/// Connect to the Postgres database.
pub async fn connect_to_postgres() -> eyre::Result<Client> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    Ok(client)
}

/// Create Postgres tables if they do not exist.
pub async fn create_tables(client: &Client) -> eyre::Result<()> {
    for table in TABLES.iter() {
        // Create the table
        let create_table_sql = table.create_table_sql();
        client.execute(&create_table_sql, &[]).await?;

        // Create the indexes
        for index_sql in table.create_index_statements() {
            client.execute(&index_sql, &[]).await?;
        }
    }

    info!("Initialized database tables");
    Ok(())
}

/// Create a trace API instance with all the required components and trait bounds
pub fn create_trace_api<Node>(
    provider: Node::Provider,
    evm_config: Node::Evm,
    pool: Node::Pool,
    network: Node::Network,
) -> Arc<TraceApi<EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>>>
where
    Node: FullNodeComponents,
    Node::Types: NodeTypes,
    EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock,
{
    let cache = EthStateCache::spawn(
        provider.clone(),
        Default::default(),
    );

    let fee_history_cache = FeeHistoryCache::new(FeeHistoryCacheConfig::default());

    let gas_oracle = GasPriceOracle::new(
        provider.clone(),
        Default::default(),
        cache.clone()
    );

    let eth_api = EthApi::new(
        provider.clone(),
        pool,
        network,
        cache.clone(),
        gas_oracle,
        ETHEREUM_BLOCK_GAS_LIMIT,
        DEFAULT_MAX_SIMULATE_BLOCKS,
        DEFAULT_ETH_PROOF_WINDOW,
        BlockingTaskPool::build().expect("failed to build tracing pool"),
        fee_history_cache,
        evm_config.clone(),
        DEFAULT_PROOF_PERMITS,
    );

    Arc::new(TraceApi::new(
        eth_api,
        BlockingTaskGuard::new(10),
    ))
}

pub(crate) fn create_eth_api<Node>(
    provider: Node::Provider,
    evm_config: Node::Evm,
    pool: Node::Pool,
    network: Node::Network,
) -> Arc<EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>>
where
    Node: FullNodeComponents,
    Node::Types: NodeTypes,
    EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock,
{
    let cache = EthStateCache::spawn(
        provider.clone(),
        Default::default(),
    );

    let fee_history_cache = FeeHistoryCache::new(FeeHistoryCacheConfig::default());

    let gas_oracle = GasPriceOracle::new(
        provider.clone(),
        Default::default(),
        cache.clone()
    );

    Arc::new(EthApi::new(
        provider.clone(),
        pool.clone(),
        network.clone(),
        cache.clone(),
        gas_oracle,
        ETHEREUM_BLOCK_GAS_LIMIT,
        DEFAULT_MAX_SIMULATE_BLOCKS,
        DEFAULT_ETH_PROOF_WINDOW,
        BlockingTaskPool::build().expect("failed to build tracing pool"),
        fee_history_cache,
        evm_config.clone(),
        DEFAULT_PROOF_PERMITS,
    ))
}