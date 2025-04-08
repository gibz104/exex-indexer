mod utils;
mod indexer;
mod datasets;
mod table_definitions;
mod db_writer;
use utils::{Config, connect_to_postgres, create_tables};
use indexer::Indexer;
use alloy_consensus::{BlockHeader, Block, Header};
use eyre::{Result, WrapErr};
use futures::{Future, TryStreamExt};
use reth::builder::NodeTypes;
use reth::primitives::{EthereumHardforks, NodePrimitives};
use reth_exex::{ExExContext, ExExEvent, ExExNotification};
use reth_primitives::{TransactionSigned, Receipt};
use reth_node_api::FullNodeComponents;
use reth_node_ethereum::EthereumNode;
use reth_tracing::tracing::{info, warn};
use std::sync::Arc;
use tokio_postgres::Client;

/// Initializes the ExEx.
///
/// Connects to the PostgreSQL database and creates the tables.
async fn init<Node: FullNodeComponents>(
    ctx: ExExContext<Node>,
) -> Result<impl Future<Output = Result<()>>>
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
    let config = Config::load().wrap_err("Failed to load configuration")?;

    let client = Arc::new(connect_to_postgres().await?);
    create_tables(&client).await?;

    // Create indexer with all processors initialized internally
    let indexer = Indexer::new(config);

    Ok(indexer_exex(ctx, client, indexer))
}

/// An ExEx that follows ETH head and stores data in a Postgres database.
async fn indexer_exex<Node: FullNodeComponents>(
    mut ctx: ExExContext<Node>,
    client: Arc<Client>,
    indexer: Indexer<Node>,
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
    // Process all new chain state notifications
    while let Some(notification) = ctx.notifications.try_next().await? {
        match &notification {
            ExExNotification::ChainReverted { old } => {
                let blocks: Vec<_> = old.blocks_iter().collect();
                let block_numbers: Vec<i64> = blocks.iter().map(|b| b.block.header().number() as i64).collect();

                // Revert all events for the given blocks
                if let Err(e) = indexer.revert_blocks(&block_numbers, &client).await {
                    warn!("Failed to revert blocks: {}", e);
                }

                info!(block_range = ?old.range(), "Successfully reverted block data");
            },
            ExExNotification::ChainCommitted { new } => {
                // Process the committed blocks
                if let Err(e) = indexer.process_blocks(
                    new.blocks_and_receipts(),
                    &client,
                    ctx.provider().clone(),
                    Arc::new(ctx.evm_config().clone()),
                    Arc::new(ctx.pool().clone()),
                    Arc::new(ctx.network().clone()),
                ).await {
                    warn!("Failed to process committed blocks: {}", e);
                }

                // Advance ExEx
                ctx.events.send(ExExEvent::FinishedHeight(new.tip().num_hash()))?;
            },
            ExExNotification::ChainReorged { old, new } => {
                info!(from_chain = ?old.range(), to_chain = ?new.range(), "Received reorg");
            },
        }
    }

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