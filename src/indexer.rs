use reth_primitives::{SealedBlockWithSenders, Receipt};
use reth_tracing::tracing::{info, warn};
use tokio_postgres::Client;
use crate::events::Event;
use crate::config::Config;
use std::time::{Instant, Duration};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use std::sync::Arc;

pub struct Indexer {
    events: Vec<Event>,
    config: Config,
    stats: Mutex<ProcessingStats>,
    last_health_check: Mutex<Instant>,
}

#[derive(Default)]
struct ProcessingStats {
    blocks_processed: u64,
    records_written: u64,
    processing_time: Duration,
}

impl Indexer {
    pub fn new(config: Config) -> Self {
        Self {
            events: Vec::new(),
            config,
            stats: Mutex::new(ProcessingStats::default()),
            last_health_check: Mutex::new(Instant::now()),
        }
    }

    pub fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    pub fn list_events(&self) -> Vec<&str> {
        self.events.iter().map(|event| event.name()).collect()
    }

    pub fn events(&self) -> &[Event] {
        &self.events
    }

    pub async fn process_block(&self, block_data: Arc<(SealedBlockWithSenders, Vec<Option<Receipt>>)>, client: Arc<Client>) -> eyre::Result<()> {
        let block_number = block_data.0.block.header.header().number;
        let start_time = Instant::now();

        let mut join_set = JoinSet::new();

        for event in &self.events {
            if self.config.is_event_enabled(event.name()) {
                let event = event.clone();
                let block_data = Arc::clone(&block_data);
                let client = Arc::clone(&client);
                join_set.spawn(async move {
                    let event_start_time = Instant::now();
                    let result = event.process(&block_data, &client).await;
                    (event.name().to_string(), result, event_start_time.elapsed())
                });
            }
        }

        let mut total_records = 0;
        let mut event_results = Vec::new();
        let mut failed_events = Vec::new();

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok((event_name, Ok(result), event_processing_time)) => {
                    total_records += result.records_written;
                    event_results.push((event_name, result.records_written, event_processing_time));
                },
                Ok((event_name, Err(e), _)) => {
                    failed_events.push((event_name, e.to_string()));
                },
                Err(e) => {
                    warn!(
                    "exex{{id=\"exex-indexer\"}}: Task join error for block {}: {}",
                    block_number,
                    e
                );
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

        let processing_time = start_time.elapsed();

        let mut stats = self.stats.lock().await;
        stats.blocks_processed += 1;
        stats.records_written += total_records as u64;
        stats.processing_time += processing_time;

        Ok(())
    }

    pub async fn health_check(&self) -> eyre::Result<()> {
        let now = Instant::now();
        let mut last_check = self.last_health_check.lock().await;
        let duration_since_last_check = now.duration_since(*last_check);
        *last_check = now;

        let mut stats = self.stats.lock().await;
        let avg_processing_time = if stats.blocks_processed > 0 {
            stats.processing_time.as_secs_f64() / stats.blocks_processed as f64
        } else {
            0.0
        };

        info!(
            "exex{{id=\"exex-indexer\"}}: Health Check: Blocks processed: {}, Records written: {}, Avg processing time: {:.2}s, Time since last check: {:.2}s",
            stats.blocks_processed,
            stats.records_written,
            avg_processing_time,
            duration_since_last_check.as_secs_f64()
        );

        // Reset stats after health check
        *stats = ProcessingStats::default();

        Ok(())
    }
}