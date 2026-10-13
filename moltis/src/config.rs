use {
    moltis_channels::{
        Error, Result,
        config_view::ChannelConfigView,
        gating::{DmPolicy, GroupPolicy, MentionMode},
    },
    moltis_common::secret_serde,
    secrecy::{ExposeSecret, Secret},
    serde::{Deserialize, Serialize},
};

/// VK community Bots Long Poll settings. User tokens are never accepted here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct VkAccountConfig {
    pub enabled: bool,
    #[serde(serialize_with = "secret_serde::serialize_secret")]
    pub token: Secret<String>,
    pub group_id: i64,
    pub dm_policy: DmPolicy,
    pub allowlist: Vec<String>,
    pub operators: Vec<String>,
    pub group_policy: GroupPolicy,
    pub group_allowlist: Vec<String>,
    pub mention_mode: MentionMode,
    pub model: Option<String>,
    pub model_provider: Option<String>,
    pub agent_id: Option<String>,
    pub text_chunk_limit: usize,
    pub attachment_limit_bytes: usize,
    pub tool_details: bool,
}

impl Default for VkAccountConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            token: Secret::new(String::new()),
            group_id: 0,
            dm_policy: DmPolicy::Allowlist,
            allowlist: Vec::new(),
            operators: Vec::new(),
            group_policy: GroupPolicy::Disabled,
            group_allowlist: Vec::new(),
            mention_mode: MentionMode::Mention,
            model: None,
            model_provider: None,
            agent_id: None,
            text_chunk_limit: 3500,
            attachment_limit_bytes: 20 * 1024 * 1024,
            tool_details: true,
        }
    }
}

impl VkAccountConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.group_id <= 0 || self.token.expose_secret().trim().is_empty() {
            return Err(Error::invalid_input(
                "positive group_id and community token required",
            ));
        }
        if !(64..=4000).contains(&self.text_chunk_limit)
            || self.attachment_limit_bytes > 50 * 1024 * 1024
        {
            return Err(Error::invalid_input(
                "text limit must be 64..4000; attachment limit <= 50 MiB",
            ));
        }
        for id in self.allowlist.iter().chain(&self.operators) {
            if !id
                .parse::<i64>()
                .is_ok_and(|id| id > 0 && id < 2_000_000_000)
            {
                return Err(Error::invalid_input(
                    "allowlist/operators require exact positive VK user IDs",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn allows(&self, sender: i64, peer: i64, text: &str) -> bool {
        if sender <= 0 {
            return false;
        }
        let sender = sender.to_string();
        if peer < 2_000_000_000 {
            return peer.to_string() == sender
                && match self.dm_policy {
                    DmPolicy::Open => true,
                    DmPolicy::Allowlist => self.allowlist.contains(&sender),
                    DmPolicy::Disabled => false,
                };
        }
        let allowed = match self.group_policy {
            GroupPolicy::Open => true,
            GroupPolicy::Allowlist => self.group_allowlist.contains(&peer.to_string()),
            GroupPolicy::Disabled => false,
        };
        allowed
            && match self.mention_mode {
                MentionMode::Always => true,
                MentionMode::None => false,
                MentionMode::Mention => text.contains(&format!("[club{}|", self.group_id)),
            }
    }

    pub(crate) fn redacted(&self) -> Result<serde_json::Value> {
        let mut value = serde_json::to_value(self)?;
        value["token"] = secret_serde::REDACTED.into();
        Ok(value)
    }

    /// Runtime patches preserve omitted values and the status API's redacted token.
    pub(crate) fn merge(&self, mut patch: serde_json::Value) -> Result<Self> {
        if let Some(object) = patch.as_object_mut() {
            if object.get("token").is_none_or(|v| {
                v.as_str()
                    .is_none_or(|s| s.is_empty() || s == secret_serde::REDACTED)
            }) {
                object.remove("token");
            }
        } else {
            return Err(Error::invalid_input("config patch must be an object"));
        }
        let mut value = serde_json::to_value(self)?;
        if let (Some(base), Some(patch)) = (value.as_object_mut(), patch.as_object()) {
            base.extend(patch.clone());
        }
        let merged: Self = serde_json::from_value(value)?;
        merged.validate()?;
        Ok(merged)
    }
}

impl ChannelConfigView for VkAccountConfig {
    fn allowlist(&self) -> &[String] {
        &self.allowlist
    }

    fn operators(&self) -> &[String] {
        &self.operators
    }

    fn group_allowlist(&self) -> &[String] {
        &self.group_allowlist
    }

    fn dm_policy(&self) -> DmPolicy {
        self.dm_policy.clone()
    }

    fn group_policy(&self) -> GroupPolicy {
        self.group_policy.clone()
    }

    fn model(&self) -> Option<&str> {
        self.model.as_deref()
    }

    fn model_provider(&self) -> Option<&str> {
        self.model_provider.as_deref()
    }

    fn agent_id(&self) -> Option<&str> {
        self.agent_id.as_deref()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    fn configured() -> VkAccountConfig {
        VkAccountConfig {
            token: Secret::new("private-token".into()),
            group_id: 42,
            allowlist: vec!["123".into()],
            operators: vec!["123".into()],
            ..Default::default()
        }
    }
    #[test]
    fn access_fails_closed() {
        let c = configured();
        assert!(c.allows(123, 123, "hello"));
        for (sender, peer) in [(1, 1), (123, 2_000_000_001), (123, 456), (-42, 123)] {
            assert!(!c.allows(sender, peer, "hello"));
        }
        assert!(!VkAccountConfig::default().allows(123, 123, "hello"));
    }
    #[test]
    fn groups_need_explicit_activation() {
        let mut c = configured();
        c.group_policy = GroupPolicy::Allowlist;
        c.group_allowlist = vec!["2000000001".into()];
        assert!(!c.allows(123, 2_000_000_001, "hello"));
        assert!(c.allows(123, 2_000_000_001, "[club42|bot] hello"));
        assert!(!c.allows(123, 2_000_000_002, "[club42|bot] hello"));
    }
    #[test]
    fn redaction_and_patch_preserve_secrets() {
        let c = configured();
        assert!(!format!("{c:?}").contains("private-token"));
        assert!(!c.redacted().unwrap().to_string().contains("private-token"));
        for p in [
            serde_json::json!({"tool_details":false}),
            serde_json::json!({"token":"[REDACTED]"}),
            serde_json::json!({"token":""}),
        ] {
            let m = c.merge(p).unwrap();
            assert_eq!(m.token.expose_secret(), "private-token");
            assert_eq!(m.allowlist, c.allowlist);
        }
        assert!(c.merge(serde_json::json!({"allowlist":["*"]})).is_err());
    }
}
