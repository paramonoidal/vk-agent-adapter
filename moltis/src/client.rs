use {
    moltis_channels::{Error, Result},
    reqwest::Client,
    secrecy::{ExposeSecret, Secret},
    serde::{Deserialize, de::DeserializeOwned},
    serde_json::Value,
    std::{sync::Arc, time::Duration},
    tokio::sync::Mutex,
};

const API_VERSION: &str = "5.199";

enum UploadFailure {
    Transient(Error),
    Permanent(Error),
}

async fn retry_upload<F, Fut>(mut upload: F) -> Result<Value>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<Value, UploadFailure>>,
{
    let mut attempt = 0;
    loop {
        match upload().await {
            Ok(value) => return Ok(value),
            Err(UploadFailure::Transient(_)) if attempt < 2 => {
                attempt += 1;
                tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
            },
            Err(UploadFailure::Transient(error) | UploadFailure::Permanent(error)) => {
                return Err(error);
            },
        }
    }
}

#[derive(Clone)]
pub(crate) struct VkClient {
    http: Client,
    poll: Client,
    token: Arc<Secret<String>>,
    group: i64,
    next_request: Arc<Mutex<tokio::time::Instant>>,
}

#[derive(Deserialize)]
struct Envelope<T> {
    response: Option<T>,
    error: Option<ApiError>,
}
#[derive(Deserialize)]
struct ApiError {
    error_code: i64,
}
#[derive(Clone, Deserialize)]
pub(crate) struct PollServer {
    pub server: String,
    pub key: String,
    pub ts: Value,
}
#[derive(Deserialize)]
pub(crate) struct PollBatch {
    pub ts: Option<Value>,
    pub failed: Option<u8>,
    #[serde(default)]
    pub updates: Vec<Value>,
}

impl VkClient {
    pub(crate) fn new(token: Secret<String>, group: i64) -> Result<Self> {
        let make = |seconds| {
            Client::builder()
                .timeout(Duration::from_secs(seconds))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| Error::unavailable("VK HTTP client initialization failed"))
        };
        Ok(Self {
            http: make(20)?,
            poll: make(35)?,
            token: Arc::new(token),
            group,
            next_request: Arc::new(Mutex::new(tokio::time::Instant::now())),
        })
    }

    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, fields(method)))]
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        mut fields: Vec<(&str, String)>,
    ) -> Result<T> {
        let delay = {
            let mut next = self.next_request.lock().await;
            let now = tokio::time::Instant::now();
            let delay = next.saturating_duration_since(now);
            *next = (*next).max(now) + Duration::from_millis(350);
            delay
        };
        tokio::time::sleep(delay).await;
        fields.push(("access_token", self.token.expose_secret().clone()));
        fields.push(("v", API_VERSION.into()));
        let response = self
            .http
            .post(format!("https://api.vk.com/method/{method}"))
            .form(&fields)
            .send()
            .await
            .map_err(|_| Error::unavailable("VK API network failure"))?;
        if !response.status().is_success() {
            return Err(Error::unavailable(format!(
                "VK API HTTP {}",
                response.status()
            )));
        }
        let envelope: Envelope<T> = response
            .json()
            .await
            .map_err(|_| Error::unavailable("invalid VK API response"))?;
        // Never include VK request_params, which may echo credentials.
        if let Some(error) = envelope.error {
            return Err(Error::unavailable(format!(
                "VK API {method} error {}",
                error.error_code
            )));
        }
        #[cfg(feature = "metrics")]
        metrics::counter!("vk_api_requests_total").increment(1);
        envelope
            .response
            .ok_or_else(|| Error::unavailable("VK API response missing"))
    }

    pub(crate) async fn configure(&self) -> Result<()> {
        let settings: Value = self
            .call("groups.getLongPollSettings", vec![(
                "group_id",
                self.group.to_string(),
            )])
            .await?;
        if settings["enabled"] == serde_json::json!(1)
            && settings["events"]["message_new"] == serde_json::json!(1)
        {
            return Ok(());
        }
        let mut fields = vec![
            ("group_id", self.group.to_string()),
            ("enabled", "1".into()),
            ("api_version", API_VERSION.into()),
        ];
        if let Some(events) = settings["events"].as_object() {
            for (name, value) in events {
                if name != "message_new" {
                    fields.push((
                        name.as_str(),
                        if value == &serde_json::json!(1) || value == &serde_json::json!(true) {
                            "1"
                        } else {
                            "0"
                        }
                        .into(),
                    ));
                }
            }
        }
        fields.push(("message_new", "1".into()));
        let _: Value = self.call("groups.setLongPollSettings", fields).await?;
        Ok(())
    }

    pub(crate) async fn server(&self) -> Result<PollServer> {
        let server: PollServer = self
            .call("groups.getLongPollServer", vec![(
                "group_id",
                self.group.to_string(),
            )])
            .await?;
        let url = url::Url::parse(&server.server)
            .map_err(|_| Error::invalid_input("invalid VK poll URL"))?;
        if url.scheme() != "https"
            || !url.host_str().is_some_and(|h| {
                h == "vk.com"
                    || h.ends_with(".vk.com")
                    || h == "vk.ru"
                    || h.ends_with(".vk.ru")
                    || h.ends_with(".vkuser.net")
            })
        {
            return Err(Error::invalid_input(
                "VK returned an untrusted poll endpoint",
            ));
        }
        Ok(server)
    }

    pub(crate) async fn check(&self, server: &PollServer, ts: &str) -> Result<PollBatch> {
        // The URL contains a short-lived poll key. Discard reqwest's URL-bearing errors.
        self.poll
            .get(&server.server)
            .query(&[
                ("act", "a_check"),
                ("key", &server.key),
                ("ts", ts),
                ("wait", "25"),
            ])
            .send()
            .await
            .map_err(|_| Error::unavailable("VK poll network failure"))?
            .error_for_status()
            .map_err(|_| Error::unavailable("VK poll HTTP failure"))?
            .json()
            .await
            .map_err(|_| Error::unavailable("invalid VK poll response"))
    }

    pub(crate) async fn send(
        &self,
        peer: &str,
        text: &str,
        random_id: i32,
        reply: Option<&str>,
    ) -> Result<i64> {
        self.send_with_keyboard(peer, text, random_id, reply, None)
            .await
    }

    pub(crate) async fn send_command_reply(
        &self,
        peer: &str,
        text: &str,
        random_id: i32,
        reply: Option<&str>,
    ) -> Result<i64> {
        self.send_with_keyboard(
            peer,
            text,
            random_id,
            reply,
            Some(crate::keyboard::command_keyboard()),
        )
        .await
    }

    async fn send_with_keyboard(
        &self,
        peer: &str,
        text: &str,
        random_id: i32,
        reply: Option<&str>,
        keyboard: Option<&str>,
    ) -> Result<i64> {
        let mut fields = vec![
            ("peer_id", peer.into()),
            ("message", text.into()),
            ("random_id", random_id.to_string()),
        ];
        if let Some(keyboard) = keyboard {
            fields.push(("keyboard", keyboard.into()));
        }
        if let Some(reply) = reply
            && reply.parse::<i64>().is_ok_and(|id| id > 0)
        {
            fields.push(("reply_to", reply.into()));
        }
        self.call("messages.send", fields).await
    }

    pub(crate) async fn send_uploaded(
        &self,
        peer: &str,
        text: &str,
        attachment: &str,
        random_id: i32,
        reply: Option<&str>,
        silent: bool,
    ) -> Result<i64> {
        let mut fields = vec![
            ("peer_id", peer.into()),
            ("message", text.into()),
            ("attachment", attachment.into()),
            ("random_id", random_id.to_string()),
            ("keyboard", crate::keyboard::command_keyboard().into()),
        ];
        if silent {
            fields.push(("disable_mentions", "1".into()));
        }
        if let Some(reply) = reply
            && reply.parse::<i64>().is_ok_and(|id| id > 0)
        {
            fields.push(("reply_to", reply.into()));
        }
        self.call("messages.send", fields).await
    }

    pub(crate) async fn upload_media(
        &self,
        peer: &str,
        bytes: Vec<u8>,
        filename: &str,
        mime: &str,
        photo: bool,
    ) -> Result<String> {
        // Retry only the staging upload. No message is sent until a file has
        // been saved, so an uncertain upload cannot duplicate a chat reply.
        // Each attempt obtains a fresh server URL, avoiding an unhealthy lease.
        let (uploaded, photo) = stage_media(photo, |as_photo| {
            self.upload_once(
                peer,
                &bytes,
                filename,
                if as_photo || !photo {
                    mime
                } else {
                    "application/octet-stream"
                },
                as_photo,
            )
        })
        .await?;
        let saved: Value = if photo {
            let photo = uploaded["photo"]
                .as_str()
                .filter(|p| *p != "[]" && !p.is_empty())
                .ok_or_else(|| Error::unavailable("VK did not accept the image"))?;
            let hash = uploaded["hash"]
                .as_str()
                .ok_or_else(|| Error::unavailable("VK image upload hash missing"))?;
            let server = uploaded["server"]
                .as_i64()
                .ok_or_else(|| Error::unavailable("VK image upload server missing"))?;
            self.call("photos.saveMessagesPhoto", vec![
                ("photo", photo.into()),
                ("hash", hash.into()),
                ("server", server.to_string()),
            ])
            .await?
        } else {
            let file = uploaded["file"]
                .as_str()
                .filter(|f| !f.is_empty())
                .ok_or_else(|| Error::unavailable("VK did not accept the document"))?;
            self.call("docs.save", vec![
                ("file", file.into()),
                ("title", filename.into()),
            ])
            .await?
        };
        uploaded_attachment_id(&saved, photo)
    }

    async fn upload_once(
        &self,
        peer: &str,
        bytes: &[u8],
        filename: &str,
        mime: &str,
        photo: bool,
    ) -> std::result::Result<Value, UploadFailure> {
        let method = if photo {
            "photos.getMessagesUploadServer"
        } else {
            "docs.getMessagesUploadServer"
        };
        let mut fields = vec![("peer_id", peer.into())];
        if !photo {
            fields.push(("type", "doc".into()));
        }
        let server: Value = self
            .call(method, fields)
            .await
            .map_err(UploadFailure::Permanent)?;
        let upload_url = server["upload_url"].as_str().ok_or_else(|| {
            UploadFailure::Permanent(Error::unavailable("VK upload server missing"))
        })?;
        validate_upload_url(upload_url).map_err(UploadFailure::Permanent)?;
        self.post_upload(upload_url, bytes, filename, mime, photo)
            .await
    }

    async fn post_upload(
        &self,
        upload_url: &str,
        bytes: &[u8],
        filename: &str,
        mime: &str,
        photo: bool,
    ) -> std::result::Result<Value, UploadFailure> {
        let part = reqwest::multipart::Part::bytes(bytes.to_vec())
            .file_name(filename.to_owned())
            .mime_str(mime)
            .map_err(|_| {
                UploadFailure::Permanent(Error::invalid_input("invalid file MIME type"))
            })?;
        let form = reqwest::multipart::Form::new().part(
            if photo {
                "photo"
            } else {
                "file"
            },
            part,
        );
        let response = self
            .http
            .post(upload_url)
            .multipart(form)
            .send()
            .await
            .map_err(|_| {
                UploadFailure::Transient(Error::unavailable("VK file upload network failure"))
            })?;
        if !response.status().is_success() {
            let error = Error::unavailable(format!("VK file upload HTTP {}", response.status()));
            return Err(
                if response.status().is_server_error()
                    || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                {
                    UploadFailure::Transient(error)
                } else {
                    UploadFailure::Permanent(error)
                },
            );
        }
        use futures::StreamExt;
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| {
                UploadFailure::Transient(Error::unavailable("VK upload response unavailable"))
            })?;
            if body.len().saturating_add(chunk.len()) > 512 * 1024 {
                return Err(UploadFailure::Permanent(Error::unavailable(
                    "VK upload response too large",
                )));
            }
            body.extend_from_slice(&chunk);
        }
        serde_json::from_slice(&body).map_err(|_| {
            UploadFailure::Permanent(Error::unavailable("invalid VK file upload response"))
        })
    }

    pub(crate) async fn download(&self, raw: &str, limit: usize) -> Result<Vec<u8>> {
        use futures::StreamExt;
        let mut url = attachment_url(raw)?;
        // Document links start at vk.com and redirect to a VK CDN. Validate
        // every hop; the API/upload clients still disable automatic redirects.
        let mut final_response = None;
        for redirects in 0..=4 {
            let response = self
                .http
                .get(url.clone())
                .send()
                .await
                .map_err(|_| Error::unavailable("VK attachment download failed"))?;
            if !response.status().is_redirection() {
                final_response = Some(response);
                break;
            }
            if redirects == 4 {
                return Err(Error::invalid_input("too many VK attachment redirects"));
            }
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|value| value.to_str().ok())
                .ok_or_else(|| Error::invalid_input("VK attachment redirect missing"))?;
            let next = url
                .join(location)
                .map_err(|_| Error::invalid_input("invalid VK attachment redirect"))?;
            url = attachment_url(next.as_str())?;
        }
        let response = final_response
            .ok_or_else(|| Error::unavailable("VK attachment response unavailable"))?;
        if !response.status().is_success()
            || response
                .content_length()
                .is_some_and(|size| size > limit as u64)
        {
            return Err(Error::invalid_input("attachment unavailable or too large"));
        }
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|_| Error::unavailable("attachment transfer failed"))?;
            if bytes.len().saturating_add(chunk.len()) > limit {
                return Err(Error::invalid_input("attachment exceeds size limit"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

fn attachment_url(raw: &str) -> Result<url::Url> {
    let url = url::Url::parse(raw).map_err(|_| Error::invalid_input("invalid attachment URL"))?;
    let trusted = url.host_str().is_some_and(|host| {
        [
            "vk.com",
            "vk.ru",
            "vkuser.net",
            "userapi.com",
            "vkuserphoto.ru",
            "vk-cdn.net",
            "vkuseraudio.net",
        ]
        .iter()
        .any(|domain| host == *domain || host.ends_with(&format!(".{domain}")))
    });
    if url.scheme() != "https"
        || !trusted
        || !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some_and(|port| port != 443)
    {
        return Err(Error::invalid_input("untrusted VK attachment host"));
    }
    Ok(url)
}

fn validate_upload_url(raw: &str) -> Result<()> {
    let url = url::Url::parse(raw).map_err(|_| Error::invalid_input("invalid VK upload URL"))?;
    let trusted = url.host_str().is_some_and(|host| {
        ["vk.com", "vk.ru", "vkuser.net", "userapi.com", "vk-cdn.net"]
            .iter()
            .any(|domain| host == *domain || host.ends_with(&format!(".{domain}")))
    });
    if url.scheme() != "https" || !trusted || !url.username().is_empty() || url.password().is_some()
    {
        return Err(Error::invalid_input("untrusted VK upload endpoint"));
    }
    Ok(())
}

fn uploaded_attachment_id(saved: &Value, photo: bool) -> Result<String> {
    let kind = if photo {
        "photo"
    } else {
        "doc"
    };
    let item = if photo {
        saved.get(0)
    } else {
        saved.get("doc").or_else(|| saved.get(0))
    }
    .ok_or_else(|| Error::unavailable("VK saved file missing"))?;
    let owner = item["owner_id"]
        .as_i64()
        .filter(|id| *id != 0)
        .ok_or_else(|| Error::unavailable("VK saved file owner missing"))?;
    let id = item["id"]
        .as_i64()
        .filter(|id| *id > 0)
        .ok_or_else(|| Error::unavailable("VK saved file ID missing"))?;
    let mut attachment = format!("{kind}{owner}_{id}");
    if let Some(key) = item["access_key"].as_str().filter(|k| !k.is_empty()) {
        attachment.push('_');
        attachment.push_str(key);
    }
    Ok(attachment)
}

/// Stage before saving/sending, so the fallback cannot duplicate a message.
async fn stage_media<F, Fut>(photo: bool, mut upload: F) -> Result<(Value, bool)>
where
    F: FnMut(bool) -> Fut,
    Fut: Future<Output = std::result::Result<Value, UploadFailure>>,
{
    let uploaded = retry_upload(|| upload(photo)).await?;
    if photo && photo_upload_rejected(&uploaded) {
        // VK sometimes rejects valid screenshots. Preserve their exact bytes.
        let document = retry_upload(|| upload(false)).await?;
        Ok((document, false))
    } else {
        Ok((uploaded, photo))
    }
}

/// Only an explicit empty photo result permits a document fallback. Missing or
/// malformed fields remain errors, and save/send failures never restage media.
fn photo_upload_rejected(uploaded: &Value) -> bool {
    uploaded
        .get("photo")
        .and_then(Value::as_str)
        .is_some_and(|photo| {
            photo.trim().is_empty()
                || serde_json::from_str::<Vec<Value>>(photo).is_ok_and(|photos| photos.is_empty())
        })
}

#[cfg(test)]
mod media_tests {
    #![allow(clippy::unwrap_used)]
    use {
        super::*,
        wiremock::{
            Mock, MockServer, ResponseTemplate,
            matchers::{method, path},
        },
    };

    #[tokio::test]
    async fn rejected_photo_falls_back_once_but_documents_and_errors_do_not() {
        for (photo, first, expected) in [
            (true, serde_json::json!({"photo":"[]"}), vec![true, false]),
            (true, serde_json::json!({"photo":"accepted"}), vec![true]),
            (true, serde_json::json!({"error":"malformed"}), vec![true]),
            (false, serde_json::json!({"photo":"[]"}), vec![false]),
        ] {
            let mut attempts = Vec::new();
            let (value, actual_photo) = stage_media(photo, |as_photo| {
                attempts.push(as_photo);
                std::future::ready(Ok(if attempts.len() == 1 {
                    first.clone()
                } else {
                    serde_json::json!({"file":"original-document"})
                }))
            })
            .await
            .unwrap();
            assert_eq!(attempts, expected);
            assert_eq!(actual_photo, photo && expected.len() == 1);
            if expected.len() == 2 {
                assert_eq!(value["file"], "original-document");
            }
        }
        let mut attempts = 0;
        assert!(
            stage_media(true, |_| {
                attempts += 1;
                std::future::ready(Err(UploadFailure::Permanent(Error::unavailable(
                    "rejected",
                ))))
            })
            .await
            .is_err()
        );
        assert_eq!(attempts, 1);
    }

    #[test]
    fn only_explicit_empty_photo_results_allow_document_fallback() {
        for photo in ["[]", " [ ] ", "", "  "] {
            assert!(photo_upload_rejected(&serde_json::json!({"photo":photo})));
        }
        for response in [
            serde_json::json!({"photo": serde_json::json!([{"id":1}]).to_string()}),
            serde_json::json!({"photo":"invalid"}),
            serde_json::json!({"photo":[]}),
            serde_json::json!({"file":"staged"}),
            serde_json::json!({"error":"rejected"}),
        ] {
            assert!(!photo_upload_rejected(&response));
        }
    }

    #[tokio::test]
    async fn transient_upload_is_retried_with_the_same_file_bytes() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&attempts);
        Mock::given(method("POST"))
            .and(path("/upload"))
            .respond_with(move |_: &wiremock::Request| {
                if observed.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(504)
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({"file":"staged"}))
                }
            })
            .mount(&server)
            .await;
        let client = VkClient::new(Secret::new("test-no-live-token".into()), 1).unwrap();
        let url = format!("{}/upload", server.uri());
        let uploaded = retry_upload(|| {
            client.post_upload(&url, b"exact file bytes", "test.txt", "text/plain", false)
        })
        .await
        .unwrap();
        assert_eq!(uploaded["file"], "staged");
        let requests = server.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests {
            let body = String::from_utf8(request.body).unwrap();
            assert!(body.contains("filename=\"test.txt\""));
            assert!(body.contains("exact file bytes"));
        }
    }

    #[tokio::test]
    async fn unavailable_upload_stops_after_three_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(504))
            .expect(3)
            .mount(&server)
            .await;
        let client = VkClient::new(Secret::new("test-no-live-token".into()), 1).unwrap();
        let url = format!("{}/upload", server.uri());
        let error =
            retry_upload(|| client.post_upload(&url, b"test", "test.txt", "text/plain", false))
                .await
                .unwrap_err();
        assert!(error.to_string().contains("504"));
        assert_eq!(server.received_requests().await.unwrap().len(), 3);
    }

    #[tokio::test]
    async fn rejected_and_malformed_uploads_are_not_retried() {
        for response in [
            ResponseTemplate::new(403),
            ResponseTemplate::new(200).set_body_string("invalid JSON"),
            ResponseTemplate::new(200).set_body_string("x".repeat(512 * 1024 + 1)),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(response)
                .expect(1)
                .mount(&server)
                .await;
            let client = VkClient::new(Secret::new("test-no-live-token".into()), 1).unwrap();
            let url = format!("{}/upload", server.uri());
            assert!(
                retry_upload(|| client.post_upload(&url, b"test", "test.txt", "text/plain", false))
                    .await
                    .is_err()
            );
            assert_eq!(server.received_requests().await.unwrap().len(), 1);
        }
    }

    #[test]
    fn attachment_cdn_and_redirect_targets_are_restricted() {
        for raw in [
            "https://sun9-77.vkuserphoto.ru/photo.jpg",
            "https://vk.com/doc123_456?key=private",
            "https://psv4.userapi.com/document",
        ] {
            assert!(attachment_url(raw).is_ok());
        }
        let source = attachment_url("https://vk.com/document").unwrap();
        for raw in [
            "http://sun9-77.vkuserphoto.ru/photo.jpg",
            "https://vkuserphoto.ru.attacker.example/photo.jpg",
            "https://notvkuserphoto.ru/photo.jpg",
            "https://127.0.0.1/private",
            "https://user:password@vk.com/photo.jpg",
            "https://vk.com:8443/private",
            "//attacker.example/file",
        ] {
            let target = source.join(raw).unwrap();
            assert!(attachment_url(target.as_str()).is_err());
        }
        let target = source.join("//psv4.userapi.com/file").unwrap();
        assert!(attachment_url(target.as_str()).is_ok());
    }

    #[test]
    fn upload_endpoints_reject_unsafe_origins() {
        for raw in [
            "http://pu.vk.com/upload",
            "https://vk.com.evil.example/upload",
            "https://127.0.0.1/upload",
            "https://secret@pu.vk.com/upload",
        ] {
            assert!(validate_upload_url(raw).is_err());
        }
        assert!(validate_upload_url("https://pu.vk.com/upload?key=private").is_ok());
        assert!(validate_upload_url("https://ps.userapi.com/upload").is_ok());
    }
    #[test]
    fn photo_and_document_save_responses_keep_private_access_key() {
        assert_eq!(
            uploaded_attachment_id(
                &serde_json::json!([{"id":1,"owner_id":-2,"access_key":"abc"}]),
                true
            )
            .unwrap(),
            "photo-2_1_abc"
        );
        assert_eq!(
            uploaded_attachment_id(
                &serde_json::json!({"type":"doc","doc":{"id":3,"owner_id":-2}}),
                false
            )
            .unwrap(),
            "doc-2_3"
        );
        assert!(uploaded_attachment_id(&serde_json::json!({"doc":{}}), false).is_err());
    }
}
