use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use thiserror::Error;

const DEFAULT_CONFIG_FILE_PATH: &str = "segment.conf.json";

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to parse config file")]
    Parse(#[from] serde_json::Error),
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// Server port
    port: u16,

    /// Max memory limit in megabytes
    max_memory: u64,
}

/// Resolves the config by combining the command line args and the
/// config present in the config file indicated by `config_file_path` parameter.
/// Command line args take precedence over the config file
pub fn resolve(
    port: Option<u16>,
    max_memory: Option<u64>,
    config_file_path: Option<String>,
) -> Result<Config> {
    let contents: String;
    if let Some(path) = config_file_path {
        contents = read_from_file(&path)?;
    } else {
        contents = read_from_file(DEFAULT_CONFIG_FILE_PATH)?;
    }
    let mut config = parse(&contents)?;
    _resolve(port, max_memory, &mut config);
    Ok(config)
}

/// Reads the config from the given file path
fn read_from_file(path: &str) -> Result<String> {
    Ok(fs::read_to_string(path)?)
}

/// Parses the config as JSON
fn parse(contents: &str) -> Result<Config, ConfigError> {
    Ok(serde_json::from_str(contents)?)
}

/// Resolves the config by giving more precedence to command line args
fn _resolve(port: Option<u16>, max_memory: Option<u64>, config: &mut Config) {
    if let Some(port) = port {
        config.port = port
    }

    if let Some(max_memory) = max_memory {
        config.max_memory = max_memory
    }
}

impl Config {
    pub fn port(&self) -> u16 {
        self.port
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEFAULT_PORT: u16 = 7890;

    #[test]
    fn read_from_file_invalid_file_path_is_err() {
        assert!(read_from_file("test.config.json").is_err())
    }

    #[test]
    fn _resolve_no_args_no_panic() {
        let want = Config {
            port: DEFAULT_PORT,
            max_memory: 1000,
        };

        let mut config = Config {
            port: DEFAULT_PORT,
            max_memory: 1000,
        };

        _resolve(None, None, &mut config);

        assert_eq!(want, config)
    }

    #[test]
    fn _resolve_with_port_no_panic() {
        let want = Config {
            port: 9000,
            max_memory: 1000,
        };

        let mut config = Config {
            port: DEFAULT_PORT,
            max_memory: 1000,
        };

        _resolve(Some(9000), None, &mut config);

        assert_eq!(want, config)
    }

    #[test]
    fn _resolve_with_max_memory_no_panic() {
        let want = Config {
            port: DEFAULT_PORT,
            max_memory: 900,
        };

        let mut config = Config {
            port: DEFAULT_PORT,
            max_memory: 1000,
        };

        _resolve(None, Some(900), &mut config);

        assert_eq!(want, config)
    }

    #[test]
    fn _resolve_with_port_max_memory_no_panic() {
        let want = Config {
            port: 9000,
            max_memory: 900,
        };

        let mut config = Config {
            port: DEFAULT_PORT,
            max_memory: 1000,
        };

        _resolve(Some(9000), Some(900), &mut config);

        assert_eq!(want, config)
    }
}
