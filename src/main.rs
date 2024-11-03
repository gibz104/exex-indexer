mod relay;
mod events;
mod db;
mod utils;
mod indexer;
mod config;

use db::{connect_to_postgres, create_tables};
use events::Event;
use indexer::Indexer;
use config::Config;

use eyre::{Result, WrapErr};
use futures::{Future, TryStreamExt};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{SealedBlockWithSenders, Receipt, BlockId, BlockNumberOrTag};
use reth_tracing::tracing::{info, warn};
use std::{sync::Arc, time::Duration};
use std::time::Instant;
use tokio::{task, sync::mpsc, time};
use tokio_postgres::Client;
use reth::primitives::{EthereumHardforks};
use reth::builder::NodeTypes;

/// Initializes the ExEx.
///
/// Connects to the PostgreSQL database and creates the tables.
async fn init<Node: FullNodeComponents>(
    ctx: ExExContext<Node>,
) -> Result<impl Future<Output = Result<()>>>
where
    Node::Types: NodeTypes,
    <Node::Types as NodeTypes>::ChainSpec: EthereumHardforks,
{
    let config = Config::load().wrap_err("Failed to load configuration")?;
    let client = connect_to_postgres().await?;
    create_tables(&client).await?;

    let mut indexer = Indexer::new(config);

    // Automatically add all available events
    for event in Event::all() {
        indexer.add_event(event);
    }

    info!("Enabled events: {:?}", indexer.list_events());

    Ok(indexer_exex(ctx, Arc::new(client), Arc::new(indexer)))
}

/// An ExEx that follows ETH head and stores data in a Postgres database.
async fn indexer_exex<Node: FullNodeComponents>(
    mut ctx: ExExContext<Node>,
    client: Arc<Client>,
    indexer: Arc<Indexer>,
) -> Result<()>
where
    Node::Types: NodeTypes,
    <Node::Types as NodeTypes>::ChainSpec: EthereumHardforks,
{
    let (block_sender, mut block_receiver) = mpsc::channel::<(SealedBlockWithSenders, Vec<Option<Receipt>>)>(100);

    // Spawn a task to process blocks
    let process_task = task::spawn({
        let client = Arc::clone(&client);
        let indexer = Arc::clone(&indexer);
        let trace_api = utils::create_trace_api(&ctx);

        async move {
            while let Some(block_data) = block_receiver.recv().await {
                let block_data = Arc::new(block_data);
                let block_number = block_data.0.block.header.header().number;
                let block_id = BlockId::Number(BlockNumberOrTag::from(block_number));

                // Get traces once for the block
                let block_traces = match trace_api.trace_block(block_id).await {
                    Ok(traces) => traces,
                    Err(e) => {
                        warn!("Failed to get traces for block {}: {}", block_number, e);
                        None
                    }
                };

                if let Err(e) = indexer.process_block(Arc::clone(&block_data), Arc::clone(&client), &block_traces).await {
                    warn!(
                        "Failed to process block {}: {}",
                        block_data.0.block.header.header().number,
                        e
                    );
                }
            }
        }
    });

    // Spawn a task for health checks
    let health_check_task = task::spawn({
        let indexer = Arc::clone(&indexer);
        async move {
            let mut interval = time::interval(Duration::from_secs(600));
            loop {
                interval.tick().await;
                if let Err(e) = indexer.health_check().await {
                    warn!("Health check failed: {}", e);
                }
            }
        }
    });

    // Process all new chain state notifications
    while let Some(notification) = ctx.notifications.try_next().await? {
        match &notification {
            ExExNotification::ChainReverted { old } => {
                let blocks: Vec<_> = old.blocks_iter().collect();
                let block_numbers: Vec<i64> = blocks.iter().map(|b| b.block.header.header().number as i64).collect();

                // Revert all events for the given blocks
                for event in indexer.events() {
                    if let Err(e) = event.revert(&block_numbers, &client).await {
                        warn!("Failed to revert event {} for blocks: {}", event.name(), e);
                    }
                }

                info!(block_range = ?old.range(), "Successfully reverted block data");
            },
            ExExNotification::ChainCommitted { new } => {
                // Convert the iterator items into owned values directly
                let blocks_and_receipts: Vec<_> = new.blocks_and_receipts()
                    .map(|(block, receipts)| (block.clone(), receipts.clone()))
                    .collect();

                for (block, receipts) in blocks_and_receipts {
                    block_sender.send((block, receipts)).await?;
                }
            },
            ExExNotification::ChainReorged { old, new } => {
                info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
            },
        }

        if let Some(committed_chain) = notification.committed_chain() {
            ctx.events.send(ExExEvent::FinishedHeight(committed_chain.tip().num_hash()))?;
        }
    }

    // Wait for the processing task to complete
    drop(block_sender);
    process_task.await?;
    health_check_task.abort();

    Ok(())
}

fn main() -> Result<()> {
    reth::cli::Cli::parse_args().run(|builder, _| async move {
        let handle = builder
            .node(EthereumNode::default())
            .install_exex("exex-indexer", init)
            .launch()
            .await
            .wrap_err("Failed to launch node")?;

        handle.wait_for_node_exit().await.wrap_err("Failed while waiting for node exit")
    })
}