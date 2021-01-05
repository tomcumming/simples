use std::env;

pub type ConfigError = String;

const KEY_PREFIX: &str = "SIMPLES_";

const ADDRESS_KEY: &str = "ADDRESS";

pub struct Config {
    pub address: String,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            address: "0.0.0.0:3000".to_string(),
        }
    }
}

fn try_from_env(key: &str) -> Result<Option<String>, ConfigError> {
    match env::var(key) {
        Ok(value) => Ok(Some(value)),
        Err(e) => match e {
            env::VarError::NotPresent => Ok(None),
            env::VarError::NotUnicode(_) => Err(format!("Could not parse config key '{}'", key))
        }
    }
}

impl Config {
    pub fn from_env() -> Result<Config, ConfigError> {
        let mut config = Config::default();

        if let Some(address) = try_from_env(&format!("{}{}", KEY_PREFIX, ADDRESS_KEY))? {
            config.address = address;
        };

        Ok(config)
    }
}
