use crate::table_definitions::TableDefinition;
use eyre::Result;
use std::sync::Arc;
use tokio_postgres::{Client, Statement, types::Type};
use futures::pin_mut;
use tokio_postgres::binary_copy::BinaryCopyInWriter;

pub struct DbWriter {
    client: Arc<Client>,
    revert_stmt: Statement,
    table: TableDefinition,
    records: Vec<Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>>,
}

impl DbWriter {
    pub async fn new(client: &Arc<Client>, table: TableDefinition) -> Result<Self> {
        let revert_stmt = client
            .prepare(&table.revert_statement())
            .await?;

        Ok(Self {
            client: Arc::clone(client),
            revert_stmt,
            table,
            records: Vec::new(),
        })
    }

    pub async fn write_record(&mut self, record: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>>) -> Result<()> {
        self.records.push(record);
        Ok(())
    }

    pub async fn finish(self) -> Result<usize> {
        if self.records.is_empty() {
            return Ok(0);
        }

        let column_names = self.table.columns.iter()
            .map(|col| col.name)
            .collect::<Vec<_>>()
            .join(", ");

        // Start COPY operation
        let copy_stmt = format!(
            "COPY {} ({}) FROM STDIN BINARY",
            self.table.name,
            column_names
        );

        let sink = self.client.copy_in(&copy_stmt).await?;

        // Create a binary writer with the correct types
        let writer = BinaryCopyInWriter::new(
            sink,
            &self.table.columns.iter()
                .map(|col| col.get_postgres_type())
                .collect::<Vec<Type>>()
        );
        pin_mut!(writer);

        // Process all records
        let mut total_records = 0;
        for record in self.records {
            // Convert the boxed ToSql traits to references
            let record_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = record
                .iter()
                .map(|value| value.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
                .collect();

            writer.as_mut().write(&record_refs).await?;
            total_records += 1;
        }

        // Finish the COPY operation
        writer.finish().await?;

        Ok(total_records)
    }

    pub async fn revert(&self, block_numbers: &[i64]) -> Result<()> {
        self.client
            .execute(&self.revert_stmt, &[&block_numbers])
            .await?;
        Ok(())
    }
}

// Helper trait to make it easier to convert common Ethereum data types to Postgres types
pub trait EthereumValue {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send>;
}

impl EthereumValue for primitive_types::H256 {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

impl EthereumValue for primitive_types::U256 {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

impl EthereumValue for alloy::primitives::Address {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_checksum(Some(1)))
    }
}

impl EthereumValue for Option<String> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.clone())
    }
}

impl<'a> EthereumValue for Option<&'a str> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.map(|s| s.to_string()))
    }
}

impl EthereumValue for Option<i64> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.clone())
    }
}

impl EthereumValue for Option<i32> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.map(|v| v as i64))
    }
}

impl EthereumValue for Option<bool> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.clone())
    }
}

impl EthereumValue for Option<chrono::DateTime<chrono::Utc>> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.clone())
    }
}

impl EthereumValue for Option<alloy::primitives::Address> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.as_ref().map(|addr| addr.to_checksum(Some(1))))
    }
}

impl EthereumValue for Option<primitive_types::H256> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.as_ref().map(|h| h.to_string()))
    }
}

impl EthereumValue for Option<primitive_types::U256> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.as_ref().map(|u| u.to_string()))
    }
}

impl EthereumValue for Option<alloy::primitives::FixedBytes<32>> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.as_ref().map(|bytes| bytes.to_string()))
    }
}

impl<'a> EthereumValue for Option<&'a alloy::primitives::FixedBytes<32>> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.map(|bytes| bytes.to_string()))
    }
}

impl EthereumValue for Option<alloy::primitives::Uint<256, 4>> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.as_ref().map(|u| u.to_string()))
    }
}

impl EthereumValue for Vec<u8> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(alloy::primitives::hex::encode_prefixed(self))
    }
}

impl EthereumValue for [u8] {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(alloy::primitives::hex::encode_prefixed(self))
    }
}

impl EthereumValue for String {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.clone())
    }
}

impl EthereumValue for str {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

impl EthereumValue for i64 {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(*self)
    }
}

impl EthereumValue for i32 {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(*self as i64)
    }
}

impl EthereumValue for bool {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(*self)
    }
}

impl EthereumValue for chrono::DateTime<chrono::Utc> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(*self)
    }
}

impl EthereumValue for alloy::primitives::FixedBytes<32> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

impl<const BITS: usize, const LIMBS: usize> EthereumValue for alloy::primitives::Uint<BITS, LIMBS> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

impl<const BITS: usize, const LIMBS: usize> EthereumValue for alloy::primitives::Signed<BITS, LIMBS> {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(self.to_string())
    }
}

// Implement for references to types that implement EthereumValue
impl<T: EthereumValue + ?Sized> EthereumValue for &T {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        (*self).to_sql_value()
    }
}

impl EthereumValue for f64 {
    fn to_sql_value(&self) -> Box<dyn tokio_postgres::types::ToSql + Sync + Send> {
        Box::new(*self)
    }
}

// Macro to help convert datatypes to Postgres types (via EthereumValue trait implementations)
#[macro_export]
macro_rules! record_values {
    ($($value:expr),* $(,)?) => {{
        // Import here so it does not need to be imported in dataset files
        use $crate::db_writer::EthereumValue;
        let values: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = vec![$($value.to_sql_value()),*];
        values
    }};
}