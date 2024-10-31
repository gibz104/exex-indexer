use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub enabled_events: HashMap<String, bool>,
}

impl Config {
    pub fn load() -> eyre::Result<Self> {
        // The path is relative to this source file (config.rs)
        let config_str = include_str!("../config.yaml");
        let config: Config = serde_yaml::from_str(config_str)?;
        Ok(config)
    }

    pub fn is_event_enabled(&self, event_name: &str) -> bool {
        self.enabled_events.get(event_name).cloned().unwrap_or(true)
    }
}