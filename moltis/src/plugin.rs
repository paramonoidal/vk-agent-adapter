use {
    crate::{
        client::VkClient,
        config::VkAccountConfig,
        inbound,
        journal::Journal,
        outbound::VkOutbound,
        state::{Account, Accounts},
    },
    async_trait::async_trait,
    moltis_channels::{
        ChannelConfigView, ChannelEventSink, ChannelHealthSnapshot, ChannelOutbound, ChannelPlugin,
        ChannelStatus, ChannelStreamOutbound, Error, Result, message_log::MessageLog,
    },
    serde_json::Value,
    std::{
        collections::HashMap,
        sync::{Arc, RwLock},
        time::Duration,
    },
    tokio_util::sync::CancellationToken,
};

pub struct VkPlugin {
    accounts: Accounts,
    outbound: VkOutbound,
    sink: Option<Arc<dyn ChannelEventSink>>,
    log: Option<Arc<dyn MessageLog>>,
    pool: sqlx::SqlitePool,
}
impl VkPlugin {
    #[must_use]
    pub fn new(pool: sqlx::SqlitePool) -> Self {
        let accounts = Arc::new(RwLock::new(HashMap::new()));
        Self {
            outbound: VkOutbound {
                accounts: Arc::clone(&accounts),
            },
            accounts,
            sink: None,
            log: None,
            pool,
        }
    }

    #[must_use]
    pub fn with_event_sink(mut self, sink: Arc<dyn ChannelEventSink>) -> Self {
        self.sink = Some(sink);
        self
    }

    #[must_use]
    pub fn with_message_log(mut self, log: Arc<dyn MessageLog>) -> Self {
        self.log = Some(log);
        self
    }

    fn account(&self, id: &str) -> Option<Arc<Account>> {
        self.accounts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned()
    }
}
#[async_trait]
impl ChannelPlugin for VkPlugin {
    fn id(&self) -> &str {
        "vk"
    }

    fn name(&self) -> &str {
        "VKontakte"
    }

    async fn start_account(&mut self, id: &str, config: Value) -> Result<()> {
        let cfg: VkAccountConfig = serde_json::from_value(config)?;
        cfg.validate()?;
        if !cfg.enabled {
            return Ok(());
        }
        let sink = self
            .sink
            .clone()
            .ok_or_else(|| Error::unavailable("VK event sink missing"))?;
        let client = VkClient::new(cfg.token.clone(), cfg.group_id)?;
        // Probe first so invalid credentials fail visibly in Settings rather than a background loop.
        client.configure().await?;
        let server = client.server().await?;
        let journal = Journal::new(self.pool.clone()).await?;
        self.stop_account(id).await?;
        let account = Arc::new(Account {
            client,
            config: RwLock::new(cfg),
            cancel: CancellationToken::new(),
            health: RwLock::new(None),
        });
        self.accounts
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(id.into(), Arc::clone(&account));
        tokio::spawn(poll(
            account,
            journal,
            id.into(),
            sink,
            self.log.clone(),
            server,
        ));
        Ok(())
    }

    async fn stop_account(&mut self, id: &str) -> Result<()> {
        if let Some(state) = self
            .accounts
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(id)
        {
            state.cancel.cancel();
        }
        Ok(())
    }

    fn outbound(&self) -> Option<&dyn ChannelOutbound> {
        Some(&self.outbound)
    }

    fn status(&self) -> Option<&dyn ChannelStatus> {
        Some(self)
    }

    fn has_account(&self, id: &str) -> bool {
        self.account(id).is_some()
    }

    fn account_ids(&self) -> Vec<String> {
        self.accounts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    async fn account_config(&self, id: &str) -> Option<Box<dyn ChannelConfigView>> {
        self.account(id).map(|s| {
            Box::new(s.config.read().unwrap_or_else(|e| e.into_inner()).clone())
                as Box<dyn ChannelConfigView>
        })
    }

    async fn account_config_json(&self, id: &str) -> Option<Value> {
        self.account(id).and_then(|s| {
            s.config
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .redacted()
                .ok()
        })
    }

    async fn update_account_config(&self, id: &str, patch: Value) -> Result<()> {
        use secrecy::ExposeSecret;
        let account = self.account(id).ok_or_else(|| Error::unknown_account(id))?;
        let mut cfg = account.config.write().unwrap_or_else(|e| e.into_inner());
        let merged = cfg.merge(patch)?;
        if merged.group_id != cfg.group_id
            || merged.token.expose_secret() != cfg.token.expose_secret()
        {
            return Err(Error::invalid_input(
                "restart the account to change group_id or token",
            ));
        }
        *cfg = merged;
        Ok(())
    }

    fn shared_outbound(&self) -> Arc<dyn ChannelOutbound> {
        Arc::new(self.outbound.clone())
    }

    fn shared_stream_outbound(&self) -> Arc<dyn ChannelStreamOutbound> {
        Arc::new(self.outbound.clone())
    }
}
#[async_trait]
impl ChannelStatus for VkPlugin {
    async fn probe(&self, id: &str) -> Result<ChannelHealthSnapshot> {
        let account = self.account(id).ok_or_else(|| Error::unknown_account(id))?;
        let details = account
            .health
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        Ok(ChannelHealthSnapshot {
            connected: details.is_none() && !account.cancel.is_cancelled(),
            account_id: id.into(),
            details,
            extra: None,
        })
    }
}

fn cursor(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

async fn poll(
    state: Arc<Account>,
    journal: Journal,
    id: String,
    sink: Arc<dyn ChannelEventSink>,
    log: Option<Arc<dyn MessageLog>>,
    mut server: crate::client::PollServer,
) {
    let mut ts = journal
        .cursor(&id)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| cursor(&server.ts));
    let mut backoff = Duration::from_secs(1);
    loop {
        let result = async {
            inbound::drain(&state, &journal, &id, &sink, log.as_ref()).await?;
            let batch = state.client.check(&server, &ts).await?;
            match batch.failed {
                Some(1) => {
                    if let Some(next) = batch.ts {
                        ts = cursor(&next);
                        journal.stage(&id, &ts, &[]).await?;
                    }
                },
                Some(2 | 3) => {
                    server = state.client.server().await?;
                    ts = cursor(&server.ts);
                    journal.stage(&id, &ts, &[]).await?;
                },
                Some(code) => {
                    return Err(Error::unavailable(format!("VK poll protocol error {code}")));
                },
                None => {
                    if let Some(next) = batch.ts {
                        let next = cursor(&next);
                        journal.stage(&id, &next, &batch.updates).await?;
                        ts = next;
                    }
                },
            }
            Ok::<_, Error>(())
        };
        let result = tokio::select! {_=state.cancel.cancelled()=>break,result=result=>result};
        match result {
            Ok(()) => {
                *state.health.write().unwrap_or_else(|e| e.into_inner()) = None;
                backoff = Duration::from_secs(1);
            },
            Err(error) => {
                #[cfg(feature = "tracing")]
                tracing::warn!(account_id=%id,error=%error,"VK polling retry");
                *state.health.write().unwrap_or_else(|e| e.into_inner()) = Some(error.to_string());
                tokio::select! {_=state.cancel.cancelled()=>break,_=tokio::time::sleep(backoff)=>{}}
                backoff = (backoff * 2).min(Duration::from_secs(60));
            },
        }
    }
}
