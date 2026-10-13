use {
    moltis_channels::{Error, Result},
    serde_json::Value,
    sqlx::SqlitePool,
};

/// Persist poll batches before advancing ts. Interrupted dispatches are reported,
/// never silently re-executed (commands may already have taken effect).
#[derive(Clone)]
pub(crate) struct Journal {
    pool: SqlitePool,
}
impl Journal {
    pub(crate) async fn new(pool: SqlitePool) -> Result<Self> {
        crate::run_migrations(&pool).await?;
        Ok(Self { pool })
    }

    pub(crate) async fn cursor(&self, account: &str) -> Result<Option<String>> {
        sqlx::query_scalar("SELECT ts FROM vk_poll_cursor WHERE account=?")
            .bind(account)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::external("read VK cursor", e))
    }

    pub(crate) async fn stage(&self, account: &str, ts: &str, events: &[Value]) -> Result<()> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| Error::external("begin VK journal", e))?;
        for event in events {
            let id = event
                .get("event_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    event
                        .pointer("/object/message/id")
                        .and_then(Value::as_i64)
                        .map(|id| format!("message:{id}"))
                });
            let Some(id) = id else {
                continue;
            };
            sqlx::query(
                "INSERT OR IGNORE INTO vk_inbox(account,id,payload,created_at) VALUES (?,?,?,?)",
            )
            .bind(account)
            .bind(id)
            .bind(event.to_string())
            .bind(time::OffsetDateTime::now_utc().unix_timestamp())
            .execute(&mut *tx)
            .await
            .map_err(|e| Error::external("stage VK message", e))?;
        }
        sqlx::query("INSERT INTO vk_poll_cursor VALUES (?,?) ON CONFLICT(account) DO UPDATE SET ts=excluded.ts").bind(account).bind(ts).execute(&mut *tx).await.map_err(|e|Error::external("store VK cursor",e))?;
        tx.commit()
            .await
            .map_err(|e| Error::external("commit VK journal", e))?;
        // Keep a week of deduplication IDs; older completed payloads are not retained.
        sqlx::query("DELETE FROM vk_inbox WHERE state='done' AND created_at < ?")
            .bind((time::OffsetDateTime::now_utc() - time::Duration::days(7)).unix_timestamp())
            .execute(&self.pool)
            .await
            .map_err(|e| Error::external("prune VK journal", e))?;
        Ok(())
    }

    pub(crate) async fn next(&self, account: &str) -> Result<Option<(String, String, String)>> {
        sqlx::query_as("SELECT id,payload,state FROM vk_inbox WHERE account=? AND state != 'done' ORDER BY created_at,rowid LIMIT 1").bind(account).fetch_optional(&self.pool).await.map_err(|e|Error::external("read VK inbox",e))
    }

    pub(crate) async fn state(&self, account: &str, id: &str, state: &str) -> Result<()> {
        sqlx::query("UPDATE vk_inbox SET state=?, payload=CASE WHEN ?='done' THEN '{}' ELSE payload END WHERE account=? AND id=?").bind(state).bind(state).bind(account).bind(id).execute(&self.pool).await.map_err(|e|Error::external("update VK inbox",e))?;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn cursor_and_deduplication_survive_reopening() {
        let path =
            std::env::temp_dir().join(format!("moltis-vk-journal-{}.db", rand::random::<u64>()));
        let url = format!("sqlite:{}?mode=rwc", path.display());
        let pool = SqlitePool::connect(&url).await.unwrap();
        let journal = Journal::new(pool.clone()).await.unwrap();
        let event = serde_json::json!({"event_id":"unique","type":"message_new","object":{"message":{"text":"command"}}});
        journal
            .stage("a", "12", &[event.clone(), event.clone()])
            .await
            .unwrap();
        let (id, _, state) = journal.next("a").await.unwrap().unwrap();
        assert_eq!(state, "pending");
        journal.state("a", &id, "dispatching").await.unwrap();
        pool.close().await;
        let reopened = Journal::new(SqlitePool::connect(&url).await.unwrap())
            .await
            .unwrap();
        assert_eq!(reopened.cursor("a").await.unwrap().as_deref(), Some("12"));
        assert_eq!(reopened.next("a").await.unwrap().unwrap().2, "dispatching");
        reopened.state("a", &id, "done").await.unwrap();
        reopened.stage("a", "13", &[event]).await.unwrap();
        assert!(reopened.next("a").await.unwrap().is_none());
        reopened.pool.close().await;
        std::fs::remove_file(path).unwrap();
    }
}
