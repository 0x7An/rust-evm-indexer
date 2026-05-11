//! Structured tracing bootstrap.

use std::{env, sync::Once};

use tracing_subscriber::{EnvFilter, fmt};

static INIT: Once = Once::new();

pub fn init() {
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
        let log_format = env::var("LOG_FORMAT").unwrap_or_else(|_| "pretty".to_string());

        let _ = if log_format.eq_ignore_ascii_case("json") {
            fmt()
                .with_env_filter(filter)
                .json()
                .flatten_event(true)
                .try_init()
        } else {
            fmt().with_env_filter(filter).try_init()
        };
    });
}

#[cfg(test)]
mod tests {
    #[test]
    fn init_is_idempotent() {
        super::init();
        super::init();
    }
}
