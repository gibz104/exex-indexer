use crate::utils::sanitize_bytes;
use crate::record_values;
use crate::db_writer::DbWriter;
use reth_primitives::{SealedBlockWithSenders, Receipt};
use chrono::Utc;
use eyre::Result;
use reth_node_api::FullNodeComponents;
use crate::indexer::ProcessingComponents;

pub async fn process_headers<Node: FullNodeComponents>(
    block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
    _components: ProcessingComponents<Node>,
    writer: &mut DbWriter,
) -> Result<()> {
    let block = &block_data.0;
    let header = block.block.header.header();

    writer
        .write_record(record_values![
            header.number as i64,
            header.timestamp as i64,
            header.parent_hash,
            block.block.header.hash(),
            header.beneficiary,
            header.difficulty,
            header.gas_limit as i64,
            header.gas_used as i64,
            header.base_fee_per_gas.map(|fee| fee as i64),
            header.blob_gas_used.map(|gas| gas as i64),
            header.excess_blob_gas.map(|gas| gas as i64),
            sanitize_bytes(&header.extra_data),
            Utc::now(),
        ])
        .await?;

    Ok(())
}
