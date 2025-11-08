use crate::config::Config;
use crate::db::migrations::migratable::Migratable;
use crate::db::schema::{Channel, OptOut};
use anyhow::Context;
use dashmap::DashMap;
use serde::Deserialize;
use std::collections::HashSet;
use std::fs;

pub struct UserTablesMigration<'a> {
    pub config: &'a Config,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OldConfig {
    channels: HashSet<String>,
    #[serde(default)]
    pub opt_out: DashMap<String, bool>,
}

impl<'a> Migratable<'a> for UserTablesMigration<'a> {
    async fn run(&self, db: &'a clickhouse::Client) -> anyhow::Result<()> {
        db.query(
            "
CREATE TABLE channel
(
    channel_id String CODEC(ZSTD(8))
)
ENGINE ReplacingMergeTree
ORDER BY channel_id;
            ",
        )
        .execute()
        .await?;

        db.query(
            "
CREATE TABLE opt_out
(
    user_id String CODEC(ZSTD(8)),
    state UInt8 CODEC(ZSTD(1))
)
ENGINE ReplacingMergeTree
ORDER BY user_id;
          ",
        )
        .execute()
        .await?;

        let legacy_config = fs::read_to_string(crate::config::CONFIG_FILE_NAME)
            .with_context(|| "Failed to load legacy config values")?;
        let OldConfig { channels, opt_out } = serde_json::from_str::<OldConfig>(&legacy_config)
            .context("Config deserialization error")?;

        if !channels.is_empty() {
            let mut insert = db.insert("channel")?;
            for channel_id in channels.into_iter() {
                insert.write(&Channel { channel_id }).await?;
            }
            insert.end().await?;
        }

        if !opt_out.is_empty() {
            let mut insert = db.insert("opt_out")?;
            for (user_id, state) in opt_out.into_iter() {
                insert.write(&OptOut { user_id, state }).await?;
            }
            insert.end().await?;
        }

        // overwrite config to remove legacy properties
        self.config.save()?;

        Ok(())
    }
}
