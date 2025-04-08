use lazy_static::lazy_static;
use tokio_postgres::types::Type;

#[derive(Debug, Clone)]
pub struct TableDefinition {
    pub name: &'static str,
    pub columns: Vec<ColumnDefinition>,
    pub indexes: Vec<&'static str>,
}

#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub name: &'static str,
    pub sql_type: &'static str,
    pub nullable: bool,
    pub primary_key: bool,
}

impl ColumnDefinition {
    pub fn get_postgres_type(&self) -> Type {
        match self.sql_type {
            "BIGINT" => Type::INT8,
            "INTEGER" => Type::INT4,
            "SMALLINT" => Type::INT2,
            "TEXT" | "VARCHAR" => Type::TEXT,
            "BOOLEAN" => Type::BOOL,
            "DOUBLE PRECISION" => Type::FLOAT8,
            "REAL" => Type::FLOAT4,
            "TIMESTAMP WITH TIME ZONE" => Type::TIMESTAMPTZ,
            "TIMESTAMP" => Type::TIMESTAMP,
            "DATE" => Type::DATE,
            "BYTEA" => Type::BYTEA,
            "JSON" => Type::JSON,
            "JSONB" => Type::JSONB,
            _ => Type::TEXT, // Default to TEXT for unknown types
        }
    }
}

impl TableDefinition {
    pub fn create_table_sql(&self) -> String {
        let columns: Vec<String> = self.columns
            .iter()
            .map(|col| {
                let nullable = if col.nullable { "" } else { " NOT NULL" };
                let pk = if col.primary_key { " PRIMARY KEY" } else { "" };
                format!("{} {}{}{}", col.name, col.sql_type, nullable, pk)
            })
            .collect();

        format!(
            "CREATE TABLE IF NOT EXISTS {} (\n    {}\n)",
            self.name,
            columns.join(",\n    ")
        )
    }

    pub fn create_index_statements(&self) -> Vec<String> {
        self.indexes.iter().map(|idx| idx.to_string()).collect()
    }

    pub fn revert_statement(&self) -> String {
        format!("DELETE FROM {} WHERE block_number = ANY($1::bigint[])", self.name)
    }
}

// Define all table schemas
pub fn get_table_definitions() -> Vec<TableDefinition> {
    vec![
        TableDefinition {
            name: "headers",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "timestamp",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "parent_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "beneficiary",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "difficulty",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas_limit",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas_used",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "base_fee_per_gas",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "blob_gas_used",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "excess_blob_gas",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "extra_data",
                    sql_type: "TEXT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_headers_timestamp ON headers (timestamp)",
            ],
        },
        TableDefinition {
            name: "transactions",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tx_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "chain_id",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "from_addr",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "to_addr",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "nonce",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "max_fee_per_gas",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "max_priority_fee_per_gas",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas_limit",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas_used",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "value",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "input",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tx_type",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "access_list",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "authorization_list",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "is_dynamic_fee",
                    sql_type: "BOOLEAN",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "blob_versioned_hashes",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "max_fee_per_blob_gas",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "blob_gas_used",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_count",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "success",
                    sql_type: "BOOLEAN",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_transactions_block_number ON transactions (block_number)",
                "CREATE INDEX IF NOT EXISTS idx_transactions_block_success ON transactions (block_number, success)",
                "CREATE INDEX IF NOT EXISTS idx_transactions_from_addr ON transactions (from_addr)",
                "CREATE INDEX IF NOT EXISTS idx_transactions_to_addr ON transactions (to_addr)",
            ],
        },
        TableDefinition {
            name: "logs",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "block_hash",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "topic0",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "topic1",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "topic2",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "topic3",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "data",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "n_data_bytes",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "chain_id",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![],
        },
        TableDefinition {
            name: "erc20_transfers",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "block_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "erc20",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "from_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "to_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "value",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "chain_id",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_erc20_transfers_block_number_desc ON erc20_transfers (block_number DESC, log_index DESC)",
                "CREATE INDEX IF NOT EXISTS idx_erc20_transfers_erc20 ON erc20_transfers (erc20)",
                "CREATE INDEX IF NOT EXISTS idx_erc20_transfers_addresses ON erc20_transfers (from_address, to_address)",
            ],
        },
        TableDefinition {
            name: "traces",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "block_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "trace_address",
                    sql_type: "TEXT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "subtraces",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "action_type",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "from_address",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "to_address",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "value",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "gas_used",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "input",
                    sql_type: "TEXT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "output",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "success",
                    sql_type: "BOOLEAN",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tx_success",
                    sql_type: "BOOLEAN",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "error",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "deployed_contract_address",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "deployed_contract_code",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "call_type",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "reward_type",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![],
        },
        TableDefinition {
            name: "native_transfers",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "block_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transfer_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "from_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "to_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "value",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transfer_type",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![],
        },
        TableDefinition {
            name: "contracts",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "block_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "create_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "contract_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "deployer",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "factory",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "init_code",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "code",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "init_code_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "n_init_code_bytes",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "n_code_bytes",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "code_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "chain_id",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![],
        },
        TableDefinition {
            name: "erc20_metadata",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "erc20",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "name",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "symbol",
                    sql_type: "VARCHAR",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "decimals",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "chain_id",
                    sql_type: "BIGINT",
                    nullable: true,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![],
        },
        TableDefinition {
            name: "uni_v2_pools",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token0",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token1",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "pair_index",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v2_pools_pool_address ON uni_v2_pools (pool_address)",
            ],
        },
        TableDefinition {
            name: "uni_v3_pools",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token0",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token1",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "fee",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tick_spacing",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v3_pools_pool_address ON uni_v3_pools (pool_address)",
            ],
        },
        TableDefinition {
            name: "uni_v4_pools",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_hash",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "transaction_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_index",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "log_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_id",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: true,
                },
                ColumnDefinition {
                    name: "currency0",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "currency1",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "fee",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tick_spacing",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "hooks",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "sqrt_price_x96",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tick",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v4_pools_pool_id ON uni_v4_pools (pool_id)",
            ],
        },
        TableDefinition {
            name: "uni_v2_pools_volume_and_tvl",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token0_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token1_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "reserve0",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "reserve1",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "price",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tvl",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "n_trades",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "volume",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v2_pools_volume_and_tvl_block_number_pool_address ON uni_v2_pools_volume_and_tvl (block_number, pool_address)",
            ],
        },
        TableDefinition {
            name: "uni_v3_pools_volume_and_tvl",
            columns: vec![
                ColumnDefinition {
                    name: "block_number",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "pool_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token0_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "token1_address",
                    sql_type: "VARCHAR",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "price",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "tvl",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "n_trades",
                    sql_type: "BIGINT",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "volume",
                    sql_type: "DOUBLE PRECISION",
                    nullable: false,
                    primary_key: false,
                },
                ColumnDefinition {
                    name: "updated_at",
                    sql_type: "TIMESTAMP WITH TIME ZONE",
                    nullable: false,
                    primary_key: false,
                },
            ],
            indexes: vec![
                "CREATE INDEX IF NOT EXISTS idx_uni_v3_pools_volume_and_tvl_block_number_pool_address ON uni_v3_pools_volume_and_tvl (block_number, pool_address)",
            ],
        },
    ]
}

// Lazy static reference to table definitions for easy access
lazy_static! {
    pub static ref TABLES: Vec<TableDefinition> = get_table_definitions();
}

// Helper function to get a specific table definition
pub fn get_table_definition(name: &str) -> Option<TableDefinition> {
    TABLES.iter().find(|t| t.name == name).cloned()
}

pub fn get_table(name: &str) -> Option<TableDefinition> {
    get_table_definition(name)
}