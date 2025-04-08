use crate::utils::Config;
use crate::table_definitions::get_table;
use crate::db_writer::DbWriter;
use crate::datasets::{
    contracts::process_contracts,
    erc20_metadata::process_erc20_metadata,
    erc20_transfers::process_erc20_transfers,
    headers::process_headers,
    logs::process_logs,
    native_transfers::process_native_transfers,
    traces::process_traces,
    transactions::process_transactions,
    uni_v2_pools::process_uni_v2_pools,
    uni_v3_pools::process_uni_v3_pools,
    uni_v4_pools::process_uni_v4_pools,
    uni_v2_pools_volume_and_tvl::process_uni_v2_pools_volume_and_tvl,
    uni_v3_pools_volume_and_tvl::process_uni_v3_pools_volume_and_tvl
};
use alloy_consensus::{Header, Block, BlockHeader};
use alloy_rpc_types::{BlockId, BlockNumberOrTag};
use alloy_rpc_types_trace::parity::{TraceResultsWithTransactionHash, TraceType};
use eyre::Result;
use reth::builder::NodeTypes;
use reth::primitives::{EthereumHardforks, NodePrimitives};
use reth_primitives::{TransactionSigned, Receipt, SealedBlockWithSenders};
use reth_node_api::{ConfigureEvmEnv, FullNodeComponents, FullNodeTypes};
use reth_rpc::EthApi;
use reth_rpc_eth_api::{FullEthApiTypes, helpers::{Call, LoadPendingBlock}};
use reth_tracing::tracing::{info, warn};
use std::{sync::Arc, time::Instant, collections::HashSet};
use tokio_postgres::Client;

// Structure to hold all the components needed for processing
#[derive(Clone)]
pub struct ProcessingComponents<Node: FullNodeComponents> {
    pub eth_api: Arc<EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>>,
    pub block_traces: Option<Vec<TraceResultsWithTransactionHash>>,
    pub provider: Node::Provider,
    pub client: Arc<Client>,
    pub config: Config,
}

struct ProcessorInfo<Node: FullNodeComponents> {
    table_name: &'static str,
    processor_name: &'static str,
    processor: for<'a> fn(
        &'a (SealedBlockWithSenders, Vec<Option<Receipt>>),
        ProcessingComponents<Node>,
        &'a mut DbWriter
    ) -> futures::future::BoxFuture<'a, Result<()>>,
}

impl<Node: FullNodeComponents> ProcessorInfo<Node> {
    fn new(
        table_name: &'static str,
        processor_name: &'static str,
        processor: for<'a> fn(
            &'a (SealedBlockWithSenders, Vec<Option<Receipt>>),
            ProcessingComponents<Node>,
            &'a mut DbWriter
        ) -> futures::future::BoxFuture<'a, Result<()>>,
    ) -> Self {
        Self {
            table_name,
            processor_name,
            processor,
        }
    }
}

pub struct Indexer<Node: FullNodeComponents> {
    processors: Vec<ProcessorInfo<Node>>,
    config: Config,
}

impl<Node> Indexer<Node>
where
    Node: FullNodeComponents + FullNodeTypes,
    Node::Types: NodeTypes,
    <Node::Types as NodeTypes>::ChainSpec: EthereumHardforks,
    <Node::Types as NodeTypes>::Primitives: NodePrimitives<
        BlockHeader = Header,
        Block = Block<TransactionSigned>,
        Receipt = Receipt,
        SignedTx = TransactionSigned,
    >,
    Node::Provider: reth::providers::BlockReader<Block = Block<TransactionSigned>>
    + reth::providers::HeaderProvider<Header = Header>
    + reth::providers::ReceiptProvider<Receipt = Receipt>
    + reth::providers::TransactionsProvider<Transaction = TransactionSigned>,
    Node::Evm: ConfigureEvmEnv<Header = Header>,
    EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock + FullEthApiTypes,
{
    pub fn new(config: Config) -> Self {
        let mut indexer = Self {
            processors: Vec::new(),
            config,
        };

        // Register all available processors
        indexer.add_processor("headers", "Headers");
        indexer.add_processor("transactions", "Transactions");
        indexer.add_processor("logs", "Logs");
        indexer.add_processor("erc20_transfers", "Erc20Transfers");
        indexer.add_processor("erc20_metadata", "Erc20Metadata");
        indexer.add_processor("traces", "Traces");
        indexer.add_processor("native_transfers", "NativeTransfers");
        indexer.add_processor("contracts", "Contracts");
        indexer.add_processor("uni_v2_pools", "UniV2Pools");
        indexer.add_processor("uni_v3_pools", "UniV3Pools");
        indexer.add_processor("uni_v4_pools", "UniV4Pools");
        indexer.add_processor("uni_v2_pools_volume_and_tvl", "UniV2PoolVolumeTVL");
        indexer.add_processor("uni_v3_pools_volume_and_tvl", "UniV3PoolVolumeTVL");

        info!("Initialized indexer with processors: {:?}", indexer.list_processors());
        indexer
    }

    pub fn add_processor(&mut self, table_name: &'static str, processor_name: &'static str) {
        let processor = match table_name {
            "headers" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_headers::<Node>(block_data, components, writer))
            ),
            "transactions" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_transactions::<Node>(block_data, components, writer))
            ),
            "logs" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_logs::<Node>(block_data, components, writer))
            ),
            "erc20_transfers" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_erc20_transfers::<Node>(block_data, components, writer))
            ),
            "traces" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_traces::<Node>(block_data, components, writer))
            ),
            "native_transfers" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_native_transfers::<Node>(block_data, components, writer))
            ),
            "contracts" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_contracts::<Node>(block_data, components, writer))
            ),
            "erc20_metadata" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_erc20_metadata::<Node>(block_data, components, writer))
            ),
            "uni_v2_pools" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v2_pools::<Node>(block_data, components, writer))
            ),
            "uni_v3_pools" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v3_pools::<Node>(block_data, components, writer))
            ),
            "uni_v4_pools" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v4_pools::<Node>(block_data, components, writer))
            ),
            "uni_v2_pools_volume_and_tvl" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v2_pools_volume_and_tvl::<Node>(block_data, components, writer))
            ),
            "uni_v3_pools_volume_and_tvl" => ProcessorInfo::new(
                table_name,
                processor_name,
                |block_data, components, writer| Box::pin(process_uni_v3_pools_volume_and_tvl::<Node>(block_data, components, writer))
            ),
            _ => return, // Skip unknown processors
        };
        self.processors.push(processor);
    }

    pub fn list_processors(&self) -> Vec<&str> {
        self.processors.iter().map(|p| p.processor_name).collect()
    }

    pub async fn revert_blocks(&self, block_numbers: &[i64], client: &Arc<Client>) -> Result<()> {
        for processor in &self.processors {
            // Skip disabled processors
            if !self.config.is_event_enabled(processor.processor_name) {
                continue;
            }

            // Get table
            let table = get_table(processor.table_name)
                .expect(&format!("Table definition not found for {}", processor.table_name));

            // Get db writer
            let writer = DbWriter::new(client, table).await?;

            // Revert blocks
            if let Err(e) = writer.revert(block_numbers).await {
                warn!("Failed to revert {} for blocks: {}", processor.table_name, e);
            }
        }
        Ok(())
    }

    pub async fn process_blocks(
        &self,
        blocks_and_receipts: impl Iterator<Item = (&SealedBlockWithSenders, &Vec<Option<Receipt>>)>,
        client: &Arc<Client>,
        provider: Node::Provider,
        evm_config: Arc<Node::Evm>,
        pool: Arc<Node::Pool>,
        network: Arc<Node::Network>,
    ) -> Result<()>
    where
        Node::Evm: ConfigureEvmEnv<Header = Header>,
        EthApi<Node::Provider, Node::Pool, Node::Network, Node::Evm>: Call + LoadPendingBlock + FullEthApiTypes,
    {
        // Convert the iterator items into owned values directly
        let blocks_and_receipts: Vec<_> = blocks_and_receipts
            .map(|(block, receipts)| (block.clone(), receipts.clone()))
            .collect();

        for (block, receipts) in blocks_and_receipts {
            let block_number = block.block.header().number();
            let block_id = BlockId::Number(BlockNumberOrTag::from(block_number));

            // Create EthAPI
            let eth_api = crate::utils::create_eth_api::<Node>(
                provider.clone(),
                (*evm_config).clone(),
                (*pool).clone(),
                (*network).clone()
            );

            // Create TraceAPI
            let trace_api = crate::utils::create_trace_api::<Node>(
                provider.clone(),
                (*evm_config).clone(),
                (*pool).clone(),
                (*network).clone()
            );

            // Get traces once for the block
            let block_traces = match trace_api.replay_block_transactions(
                block_id,
                HashSet::from_iter(vec![TraceType::Trace])
            ).await {
                Ok(traces) => traces,
                Err(e) => {
                    warn!("Failed to get traces for block {}: {}", block_number, e);
                    None
                }
            };

            // Create components for processing
            let components = ProcessingComponents {
                eth_api: eth_api.clone(),
                block_traces,
                provider: provider.clone(),
                client: Arc::clone(client),
                config: self.config.clone(),
            };

            let block_data = (block, receipts);
            if let Err(e) = self.process_block_data(&block_data, components).await {
                warn!("Failed to process block {}: {}", block_number, e);
            }
        }

        Ok(())
    }

    pub async fn process_block_data(
        &self,
        block_data: &(SealedBlockWithSenders, Vec<Option<Receipt>>),
        components: ProcessingComponents<Node>,
    ) -> Result<()>
    where
        Node::Types: NodeTypes,
        <Node::Types as NodeTypes>::ChainSpec: EthereumHardforks,
        <Node::Types as NodeTypes>::Primitives: NodePrimitives<
            BlockHeader = Header,
            Block = Block<TransactionSigned>,
            Receipt = Receipt,
            SignedTx = TransactionSigned
        >,
    {
        let block_number = block_data.0.block.header.header().number;

        // Create a vector to store all processing tasks
        let mut tasks = Vec::new();

        // Spawn a task for each enabled processor
        for processor in &self.processors {
            if self.config.is_event_enabled(processor.processor_name) {
                // Clone necessary data for the task
                let processor_name = processor.processor_name;
                let processor_fn = processor.processor;
                let block_data = block_data.clone();
                let components = components.clone();
                let table = get_table(processor.table_name)
                    .expect(&format!("Table definition not found for {}", processor.table_name));

                // Spawn the task
                let task = tokio::spawn(async move {
                    let event_start_time = Instant::now();
                    let mut writer = match DbWriter::new(&components.client, table).await {
                        Ok(w) => w,
                        Err(e) => return Err((processor_name, e.to_string()))
                    };
                    match processor_fn(&block_data, components, &mut writer).await {
                        Ok(()) => {
                            match writer.finish().await {
                                Ok(records_written) => Ok((processor_name, records_written, event_start_time.elapsed())),
                                Err(e) => Err((processor_name, e.to_string()))
                            }
                        },
                        Err(e) => Err((processor_name, e.to_string()))
                    }
                });

                tasks.push(task);
            }
        }

        // Wait for all tasks to complete and collect results
        let mut total_records = 0;
        let mut event_results = Vec::new();
        let mut failed_events = Vec::new();

        for task in tasks {
            match task.await {
                Ok(Ok((name, records, duration))) => {
                    total_records += records;
                    event_results.push((name, records, duration));
                }
                Ok(Err((name, error))) => {
                    failed_events.push((name, error));
                }
                Err(e) => {
                    warn!("Task join error: {}", e);
                }
            }
        }

        // Sort events by name for consistent logging
        event_results.sort_by(|a, b| a.0.cmp(&b.0));

        // Create a consolidated success log
        if !event_results.is_empty() {
            let events_summary: Vec<String> = event_results
                .iter()
                .map(|(name, records, time)| {
                    format!("{}({}, {:.2}s)", name, records, time.as_secs_f64())
                })
                .collect();

            info!(
                "exex{{id=\"exex-indexer\"}}: Block {} processed - Events: [{}], Total records: {}",
                block_number,
                events_summary.join(", "),
                total_records,
            );
        }

        // Create a consolidated error log
        if !failed_events.is_empty() {
            let failure_summary: Vec<String> = failed_events
                .iter()
                .map(|(name, error)| format!("{}: {}", name, error))
                .collect();

            warn!(
                "exex{{id=\"exex-indexer\"}}: Block {} failures - {}",
                block_number,
                failure_summary.join(", ")
            );
        }

        Ok(())
    }
}