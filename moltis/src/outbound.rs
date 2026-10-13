use {
    crate::state::{Account, Accounts},
    async_trait::async_trait,
    moltis_channels::{
        ChannelOutbound, ChannelStreamOutbound, Error, Result, StreamReceiver,
        plugin::{ChannelTaskStatus, StreamEvent},
    },
    moltis_common::types::ReplyPayload,
    std::sync::Arc,
};

#[derive(Clone)]
pub(crate) struct VkOutbound {
    pub accounts: Accounts,
}
impl VkOutbound {
    fn resolve(&self, id: &str) -> Result<Arc<Account>> {
        self.accounts
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned()
            .ok_or_else(|| Error::unknown_account(id))
    }

    async fn send(
        &self,
        account: &str,
        to: &str,
        text: &str,
        reply: Option<&str>,
    ) -> Result<Vec<String>> {
        let state = self.resolve(account)?;
        let limit = state
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .text_chunk_limit;
        let mut ids = Vec::new();
        for chunk in chunks(text, limit) {
            let random_id = rand::random::<i32>();
            // A retry must keep the same random_id to let VK deduplicate an uncertain send.
            let mut retries = 0;
            let id = loop {
                match state
                    .client
                    .send_command_reply(to, chunk, random_id, reply)
                    .await
                {
                    Ok(id) => break id,
                    Err(error) if error.is_retryable() && retries < 3 => {
                        retries += 1;
                        tokio::time::sleep(std::time::Duration::from_secs(1 << retries)).await;
                    },
                    Err(error) => return Err(error),
                }
            };
            ids.push(id.to_string());
        }
        Ok(ids)
    }
}

/// VK counts Unicode characters, not UTF-8 bytes. Never split a code point.
pub(crate) fn chunks(text: &str, limit: usize) -> Vec<&str> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    let mut count = 0;
    for (index, _) in text.char_indices() {
        if count == limit {
            chunks.push(&text[start..index]);
            start = index;
            count = 0;
        }
        count += 1;
    }
    chunks.push(&text[start..]);
    chunks
}

#[async_trait]
impl ChannelOutbound for VkOutbound {
    async fn send_text(&self, a: &str, to: &str, text: &str, reply: Option<&str>) -> Result<()> {
        self.send(a, to, text, reply).await?;
        Ok(())
    }

    async fn send_text_reporting_ids(
        &self,
        a: &str,
        to: &str,
        text: &str,
        reply: Option<&str>,
    ) -> Result<Vec<String>> {
        self.send(a, to, text, reply).await
    }

    async fn send_media(
        &self,
        a: &str,
        to: &str,
        payload: &ReplyPayload,
        reply: Option<&str>,
    ) -> Result<()> {
        let Some(media) = &payload.media else {
            return self.send_text(a, to, &payload.text, reply).await;
        };
        let state = self.resolve(a)?;
        let cfg = state
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let bytes = decode_media(&media.url, cfg.attachment_limit_bytes.min(5 * 1024 * 1024))?;
        let filename = media
            .filename
            .as_deref()
            .unwrap_or(if media.mime_type == "image/png" {
                "image.png"
            } else {
                "file.bin"
            });
        let photo = matches!(media.mime_type.as_str(), "image/png" | "image/jpeg");
        let attachment = state
            .client
            .upload_media(to, bytes, filename, &media.mime_type, photo)
            .await?;
        let chunks = chunks(&payload.text, cfg.text_chunk_limit);
        let random_id = rand::random();
        let mut retries = 0;
        loop {
            match state
                .client
                .send_uploaded(
                    to,
                    chunks.first().copied().unwrap_or(""),
                    &attachment,
                    random_id,
                    reply.or(payload.reply_to_id.as_deref()),
                    payload.silent,
                )
                .await
            {
                Ok(_) => break,
                Err(error) if error.is_retryable() && retries < 3 => {
                    retries += 1;
                    tokio::time::sleep(std::time::Duration::from_secs(1 << retries)).await;
                },
                Err(error) => return Err(error),
            }
        }
        for text in chunks.iter().skip(1) {
            self.send_text(a, to, text, reply).await?;
        }
        Ok(())
    }

    async fn send_typing(&self, a: &str, to: &str) -> Result<()> {
        let state = self.resolve(a)?;
        let _: serde_json::Value = state
            .client
            .call("messages.setActivity", vec![
                ("peer_id", to.into()),
                ("type", "typing".into()),
            ])
            .await?;
        Ok(())
    }
}

fn decode_media(url: &str, limit: usize) -> Result<Vec<u8>> {
    use base64::Engine;
    let (header, data) = url
        .split_once(',')
        .ok_or_else(|| Error::invalid_input("send_media requires a base64 data URI"))?;
    if !header.starts_with("data:")
        || !header.ends_with(";base64")
        || data.len() > limit.saturating_add(2) / 3 * 4
    {
        return Err(Error::invalid_input(
            "unsupported media URI or file exceeds upload limit (5 MiB)",
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| Error::invalid_input("invalid media base64"))?;
    if bytes.len() > limit {
        return Err(Error::invalid_input("file exceeds upload limit (5 MiB)"));
    }
    Ok(bytes)
}

#[async_trait]
impl ChannelStreamOutbound for VkOutbound {
    async fn send_stream(
        &self,
        a: &str,
        to: &str,
        reply: Option<&str>,
        stream: StreamReceiver,
    ) -> Result<()> {
        self.send_stream_reporting_ids(a, to, reply, stream).await?;
        Ok(())
    }

    async fn send_stream_reporting_ids(
        &self,
        a: &str,
        to: &str,
        reply: Option<&str>,
        mut stream: StreamReceiver,
    ) -> Result<Vec<String>> {
        let state = self.resolve(a)?;
        let details = state
            .config
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .tool_details;
        let mut text = String::new();
        let mut ids = Vec::new();
        while let Some(event) = stream.recv().await {
            match event {
                StreamEvent::Delta(delta) => {
                    text.push_str(&delta);
                },
                StreamEvent::ProgressDelta(delta) if details => {
                    if !delta.trim().is_empty() {
                        ids.extend(self.send(a, to, &delta, reply).await?);
                    }
                },
                StreamEvent::TaskUpdate(task) if details => {
                    let status = match task.status {
                        ChannelTaskStatus::InProgress => "▶",
                        ChannelTaskStatus::Complete => "✓",
                        ChannelTaskStatus::Error => "✗",
                    };
                    ids.extend(
                        self.send(a, to, &format!("{status} {}", task.title), reply)
                            .await?,
                    );
                },
                StreamEvent::Error(error) => {
                    ids.extend(
                        self.send(a, to, &format!("Agent error: {error}"), reply)
                            .await?,
                    );
                },
                StreamEvent::Done => break,
                _ => {},
            }
        }
        ids.extend(self.send(a, to, &text, reply).await?);
        Ok(ids)
    }

    async fn receives_progress_deltas(&self, _: &str) -> bool {
        true
    }

    async fn receives_task_updates(&self, _: &str) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unicode_chunks_preserve_text_and_limits() {
        let text = "😀Привет世界".repeat(42);
        let parts = chunks(&text, 64);
        assert_eq!(parts.concat(), text);
        assert!(parts.iter().all(|p| p.chars().count() <= 64));
        assert!(chunks("", 64).is_empty());
    }
    #[test]
    fn media_decoding_is_bounded_and_never_fetches_urls_or_local_paths() {
        assert_eq!(
            decode_media("data:text/plain;base64,aGVsbG8=", 5)
                .ok()
                .as_deref(),
            Some(b"hello".as_slice())
        );
        for uri in [
            "data:text/plain;base64,aGVsbG8=",
            "https://example.com/private",
            "file:///etc/passwd",
            "data:text/plain,hello",
            "data:text/plain;base64,%%%%",
        ] {
            assert!(decode_media(uri, 4).is_err());
        }
    }
}
