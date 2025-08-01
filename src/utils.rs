use crate::table_definitions::TABLES;
use reth_tracing::tracing::info;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env};
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