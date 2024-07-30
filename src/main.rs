mod rbuilder_types;

use std::env;
use eyre::{eyre, Result, WrapErr};
use futures::Future;
use serde::Serialize;
use reth_exex::{ExExContext, ExExEvent};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{BlockHash, Header, SealedBlockWithSenders};
use reth_tracing::tracing::{info, warn};
use reqwest::Client;
use serde_json::json;
use rbuilder_types::PayloadDeliveredFetcher;
use chrono::Utc;

// Define a struct for the insert request
#[derive(Serialize)]
struct InsertRequest {
    table: String,
    columns: Vec<String>,
    values: Vec<serde_json::Value>,
}

/// Initializes the ExEx.
///
/// Opens up a DuckDb database and creates the tables.
async fn init<Node: FullNodeComponents>(
    ctx: ExExContext<Node>,
    connection: Client,
) -> Result<impl Future<Output = Result<()>>> {
    create_tables(&connection).await?;
    Ok(indexer_exex(ctx, connection))
}

/// Create DuckDB tables if they do not exist.
async fn create_tables(connection: &Client) -> Result<()> {
    let mut db_endpoint = env::var("DB_ENDPOINT").expect("DB_ENDPOINT must be set");
    db_endpoint = db_endpoint + "/duckdb/query";

    let queries = vec![
        r#"
        CREATE TABLE IF NOT EXISTS headers (
            block_number     INTEGER PRIMARY KEY,
            timestamp        BIGINT NOT NULL UNIQUE,
            parent_hash      VARCHAR NOT NULL UNIQUE,
            hash             VARCHAR NOT NULL UNIQUE,
            beneficiary      VARCHAR NOT NULL,
            difficulty       INTEGER NOT NULL,
            gas_limit        INTEGER NOT NULL,
            gas_used         INTEGER NOT NULL,
            base_fee_per_gas BIGINT,
            blob_gas_used    INTEGER,
            excess_blob_gas  INTEGER,
            extra_data       VARCHAR NOT NULL,
            updated_at       TIMESTAMP_MS NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS mevboost (
            block_number     INTEGER NOT NULL,
            relay_id         VARCHAR NOT NULL,
            slot             BIGINT NOT NULL,
            parent_hash      VARCHAR NOT NULL,
            block_hash       VARCHAR NOT NULL,
            builder_pubkey   VARCHAR NOT NULL,
            proposer_pubkey  VARCHAR NOT NULL,
            proposer_fee_recipient VARCHAR NOT NULL,
            gas_limit        BIGINT NOT NULL,
            gas_used         BIGINT NOT NULL,
            value            VARCHAR NOT NULL,
            num_tx           BIGINT NOT NULL,
            timestamp        BIGINT NOT NULL,
            timestamp_ms     BIGINT NOT NULL,
            optimistic_submission BOOLEAN NOT NULL,
            updated_at       TIMESTAMP_MS NOT NULL,
            PRIMARY KEY (block_number, relay_id)
        );
        "#,
    ];

    for query in queries {
        let response = connection.post(&db_endpoint)
            .json(&json!({ "sql": query }))
            .send()
            .await
            .wrap_err("Failed to send create tables request")?;

        if !response.status().is_success() {
            let error_text = response.text().await.wrap_err("Failed to get error response text")?;
            return Err(eyre!("Failed to create tables: {}", error_text));
        }
    }

    info!("Initialized database tables");
    Ok(())
}

/// Write header data to the remote DuckDB instance.
async fn write_headers(connection: &Client, blocks: impl Iterator<Item = SealedBlockWithSenders>) -> Result<usize> {
    let mut db_endpoint = env::var("DB_ENDPOINT").expect("DB_ENDPOINT must be set");
    db_endpoint = db_endpoint + "/duckdb/insert";
    let mut headers_inserted = 0;

    for block in blocks {
        let header: &Header = block.block.header.header();
        let hash: BlockHash = block.block.header.hash();

        let insert_request = InsertRequest {
            table: "headers".to_string(),
            columns: vec![
                "block_number", "timestamp", "parent_hash", "hash", "beneficiary",
                "difficulty", "gas_limit", "gas_used", "base_fee_per_gas",
                "blob_gas_used", "excess_blob_gas", "extra_data", "updated_at"
            ].into_iter().map(String::from).collect(),
            values: vec![
                json!(header.number),
                json!(header.timestamp),
                json!(header.parent_hash.to_string()),
                json!(hash.to_string()),
                json!(header.beneficiary.to_string()),
                json!(header.difficulty),
                json!(header.gas_limit),
                json!(header.gas_used),
                json!(header.base_fee_per_gas.unwrap_or_default()),
                json!(header.blob_gas_used.unwrap_or_default()),
                json!(header.excess_blob_gas.unwrap_or_default()),
                json!(Utc::now().format("%Y-%m-%d %H:%M:%S.%3f").to_string()),
            ],
        };

        let response = connection.post(&db_endpoint)
            .json(&insert_request)
            .send()
            .await
            .wrap_err_with(|| format!("Failed to send insert request for block {}", header.number))?;

        if response.status().is_success() {
            headers_inserted += 1;
        } else {
            let error_text = response.text().await.wrap_err("Failed to get error response text")?;
            info!("Failed to insert header for block {}: {}", header.number, error_text);
        }
    }

    Ok(headers_inserted)
}

/// Write MEV-boost data to the remote DuckDB instance.
async fn write_mevboost_data(connection: &Client, blocks: impl Iterator<Item = SealedBlockWithSenders>) -> Result<usize> {
    let mut db_endpoint = env::var("DB_ENDPOINT").expect("DB_ENDPOINT must be set");
    db_endpoint = db_endpoint + "/duckdb/insert";
    let mut records_inserted = 0;

    for block in blocks {
        let block_number = block.block.header.header().number;
        let fetcher = PayloadDeliveredFetcher::default();
        let result = fetcher.get_payload_delivered(block_number).await;

        // Log any relay errors
        if !result.relay_errors.is_empty() {
            for (relay_id, error) in &result.relay_errors {
                warn!("Relay error for block {}: Relay ID: {}, Error: {:?}", block_number, relay_id, error);
            }
        }

        let blocks_delivered = result.delivered;

        for (relay_id, block) in blocks_delivered {
            let insert_request = InsertRequest {
                table: "mevboost".to_string(),
                columns: vec![
                    "block_number", "relay_id", "slot", "parent_hash", "block_hash", "builder_pubkey",
                    "proposer_pubkey", "proposer_fee_recipient", "gas_limit", "gas_used", "value",
                    "num_tx", "timestamp", "timestamp_ms", "optimistic_submission", "updated_at"
                ].into_iter().map(String::from).collect(),
                values: vec![
                    json!(block.block_number),
                    json!(relay_id),
                    json!(block.slot),
                    json!(block.parent_hash.to_string()),
                    json!(block.block_hash.to_string()),
                    json!(format!("{:?}", block.builder_pubkey)),
                    json!(format!("{:?}", block.proposer_pubkey)),
                    json!(block.proposer_fee_recipient.to_string()),
                    json!(block.gas_limit),
                    json!(block.gas_used),
                    json!(block.value.to_string()),
                    json!(block.num_tx),
                    json!(block.timestamp),
                    json!(block.timestamp_ms),
                    json!(block.optimistic_submission),
                    json!(Utc::now().format("%Y-%m-%d %H:%M:%S.%3f").to_string()),
                ],
            };

            let response = connection.post(&db_endpoint)
                .json(&insert_request)
                .send()
                .await
                .wrap_err_with(|| format!("Failed to send insert request for MEV-boost data for block {}", block.block_number))?;

            if response.status().is_success() {
                records_inserted += 1;
            } else {
                let error_text = response.text().await.wrap_err("Failed to get error response text")?;
                info!("Failed to insert MEV-boost data for block {}: {}", block.block_number, error_text);
            }
        }
    }

    Ok(records_inserted)
}

/// An ExEx that follows ETH head and stores data in a remote DuckDB instance.
async fn indexer_exex<Node: FullNodeComponents>(
    mut ctx: ExExContext<Node>,
    connection: Client,
) -> Result<()> {
    // Process all new chain state notifications
    while let Some(notification) = ctx.notifications.recv().await {
        // Revert all headers
        if let Some(reverted_chain) = notification.reverted_chain() {
            let blocks = reverted_chain.blocks_iter();

            let query = format!("DELETE FROM headers WHERE block_number IN ({});",
                                blocks.cloned().map(|b| b.block.header.header().number.to_string()).collect::<Vec<_>>().join(","));
            let mut db_endpoint = env::var("DB_ENDPOINT").expect("DB_ENDPOINT must be set");
            db_endpoint = db_endpoint + "/duckdb/query";
            let response = connection.post(db_endpoint)
                .json(&json!({ "sql": query }))
                .send()
                .await
                .wrap_err("Failed to send delete request for reverted blocks")?;

            if response.status().is_success() {
                info!(block_range = ?reverted_chain.range(), "Reverted headers");
            } else {
                let error_text = response.text().await.wrap_err("Failed to get error response text")?;
                info!("Failed to delete headers for reverted blocks: {}", error_text);
            }
        }

        // Insert all new headers and MEV-boost data
        if let Some(committed_chain) = notification.committed_chain() {
            // Write headers
            let headers_inserted = write_headers(&connection, committed_chain.blocks_iter().cloned()).await?;
            info!(block_range = ?committed_chain.range(), %headers_inserted, "Committed headers");

            // Fetch and write MEV-boost data
            let mevboost_inserted = write_mevboost_data(&connection, committed_chain.blocks_iter().cloned()).await?;
            info!(block_range = ?committed_chain.range(), %mevboost_inserted, "Committed MEV-boost data");

            // Send a finished height event, signaling the node that we don't
            // need any blocks below this height anymore
            ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().number))
                .wrap_err("Failed to send FinishedHeight event")?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("exex-indexer", |ctx| async move {
                let connection = Client::new();
                init(ctx, connection).await
            })
            .launch()
            .await
            .wrap_err("Failed to launch node")?;

        handle.wait_for_node_exit().await.wrap_err("Failed while waiting for node exit")
    })
}