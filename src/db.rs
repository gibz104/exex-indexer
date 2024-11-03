use std::env;
use tokio_postgres::{Client, NoTls};
use reth_tracing::tracing::info;

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
    let queries = vec![
        r#"
        CREATE TABLE IF NOT EXISTS headers (
            block_number                BIGINT PRIMARY KEY,
            timestamp                   BIGINT NOT NULL,
            parent_hash                 VARCHAR NOT NULL,
            hash                        VARCHAR NOT NULL UNIQUE,
            beneficiary                 VARCHAR NOT NULL,
            difficulty                  VARCHAR NOT NULL,
            gas_limit                   BIGINT NOT NULL,
            gas_used                    BIGINT NOT NULL,
            base_fee_per_gas            BIGINT,
            blob_gas_used               BIGINT,
            excess_blob_gas             BIGINT,
            extra_data                  TEXT,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS transactions (
            block_number                BIGINT NOT NULL,
            tx_hash                     VARCHAR PRIMARY KEY,
            chain_id                    BIGINT,
            from_addr                   VARCHAR NOT NULL,
            to_addr                     VARCHAR,
            nonce                       BIGINT NOT NULL,
            max_fee_per_gas             BIGINT NOT NULL,
            max_priority_fee_per_gas    BIGINT,
            gas_limit                   BIGINT NOT NULL,
            gas_used                    BIGINT,
            value                       VARCHAR NOT NULL,
            input                       VARCHAR NOT NULL,
            tx_type                     VARCHAR NOT NULL,
            access_list                 VARCHAR,
            authorization_list          VARCHAR,
            is_dynamic_fee              BOOLEAN NOT NULL,
            blob_versioned_hashes       VARCHAR,
            max_fee_per_blob_gas        BIGINT,
            blob_gas_used               BIGINT,
            log_count                   BIGINT,
            success                     BOOLEAN,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS logs (
            block_number                BIGINT NOT NULL,
            block_hash                  VARCHAR,
            transaction_index           BIGINT NOT NULL,
            log_index                   BIGINT NOT NULL,
            transaction_hash            VARCHAR NOT NULL,
            address                     VARCHAR NOT NULL,
            topic0                      VARCHAR,
            topic1                      VARCHAR,
            topic2                      VARCHAR,
            topic3                      VARCHAR,
            data                        VARCHAR NOT NULL,
            n_data_bytes                BIGINT NOT NULL,
            chain_id                    BIGINT,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS builder_bids (
            block_number                BIGINT NOT NULL,
            relay_id                    VARCHAR NOT NULL,
            slot                        BIGINT NOT NULL,
            parent_hash                 VARCHAR NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            builder_pubkey              VARCHAR NOT NULL,
            proposer_pubkey             VARCHAR NOT NULL,
            proposer_fee_recipient      VARCHAR NOT NULL,
            gas_limit                   BIGINT NOT NULL,
            gas_used                    BIGINT NOT NULL,
            value                       VARCHAR NOT NULL,
            num_tx                      BIGINT NOT NULL,
            timestamp                   BIGINT NOT NULL,
            timestamp_ms                BIGINT NOT NULL,
            optimistic_submission       BOOLEAN NOT NULL,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS proposer_payloads (
            block_number                BIGINT NOT NULL,
            relay_id                    VARCHAR NOT NULL,
            slot                        BIGINT NOT NULL,
            parent_hash                 VARCHAR NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            builder_pubkey              VARCHAR NOT NULL,
            proposer_pubkey             VARCHAR NOT NULL,
            proposer_fee_recipient      VARCHAR NOT NULL,
            gas_limit                   BIGINT NOT NULL,
            gas_used                    BIGINT NOT NULL,
            value                       VARCHAR NOT NULL,
            num_tx                      BIGINT NOT NULL,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS ommers (
            block_number                BIGINT NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            ommer_index                 BIGINT NOT NULL,
            parent_hash                 VARCHAR NOT NULL,
            ommer_hash                  VARCHAR NOT NULL,
            beneficiary                 VARCHAR NOT NULL,
            state_root                  VARCHAR NOT NULL,
            transactions_root           VARCHAR NOT NULL,
            receipts_root               VARCHAR NOT NULL,
            difficulty                  VARCHAR NOT NULL,
            number                      BIGINT NOT NULL,
            gas_limit                   BIGINT NOT NULL,
            gas_used                    BIGINT NOT NULL,
            timestamp                   BIGINT NOT NULL,
            mix_hash                    VARCHAR NOT NULL,
            nonce                       VARCHAR NOT NULL,
            base_fee_per_gas            BIGINT,
            withdrawals_root            VARCHAR,
            blob_gas_used               BIGINT,
            excess_blob_gas             BIGINT,
            parent_beacon_block_root    VARCHAR,
            extra_data                  TEXT NOT NULL,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL,
            PRIMARY KEY (block_number, ommer_index)
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS withdrawals (
            block_number                BIGINT NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            withdrawal_index            BIGINT NOT NULL,
            validator_index             BIGINT NOT NULL,
            address                     VARCHAR NOT NULL,
            amount_gwei                 BIGINT NOT NULL,
            amount_wei                  VARCHAR NOT NULL,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL,
            PRIMARY KEY (block_number, withdrawal_index)
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS erc20_transfers (
            block_number                BIGINT NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            transaction_index           INTEGER NOT NULL,
            log_index                   INTEGER NOT NULL,
            transaction_hash            VARCHAR NOT NULL,
            erc20                       VARCHAR NOT NULL,
            from_address                VARCHAR NOT NULL,
            to_address                  VARCHAR NOT NULL,
            value                       VARCHAR NOT NULL,
            chain_id                    BIGINT,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL,
            PRIMARY KEY (block_number, transaction_index, log_index)
        );
        "#,
        r#"
        CREATE TABLE IF NOT EXISTS traces (
            block_number                BIGINT NOT NULL,
            block_hash                  VARCHAR NOT NULL,
            transaction_hash            VARCHAR,
            transaction_index           INTEGER NOT NULL,
            trace_address               TEXT NOT NULL,
            subtraces                   INTEGER NOT NULL,
            action_type                 VARCHAR NOT NULL,
            from_address                VARCHAR,
            to_address                  VARCHAR,
            value                       VARCHAR,
            gas                         BIGINT,
            gas_used                    BIGINT,
            input                       TEXT,
            output                      VARCHAR,
            success                     BOOLEAN,
            tx_success                  BOOLEAN,
            error                       VARCHAR,
            deployed_contract_address   VARCHAR,
            deployed_contract_code      VARCHAR,
            call_type                   VARCHAR,
            reward_type                 VARCHAR,
            updated_at                  TIMESTAMP WITH TIME ZONE NOT NULL
        );
        "#,
    ];

    for query in queries {
        client.execute(query, &[]).await?;
    }

    info!("Initialized database tables");
    Ok(())
}
