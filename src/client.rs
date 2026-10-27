use crate::{
    Config, DocumentContent, Error, ErrorKind, Manifest, Result, RootState, StatusError,
    load_config, parse_manifest,
};
use reqwest::{Method, header::HeaderMap};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::{Mutex, RwLock};

pub const DEFAULT_AUTH_BASE_URL: &str = "https://webapp-prod.cloud.remarkable.engineering";
pub const DEFAULT_DEVICE_DESC: &str = "desktop-macos";
pub const DEFAULT_DISCOVERY_URL: &str = "https://eu.tectonic.remarkable.com/discovery/v1/endpoints";
pub const DEFAULT_RAW_HOST: &str = "https://eu.tectonic.remarkable.com";
pub const DEFAULT_STORAGE_HOST: &str = "https://internal.cloud.remarkable.com";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Endpoints {
    pub raw_host: String,
    pub webapp_host: String,
    pub storage_host: String,
}
impl Default for Endpoints {
    fn default() -> Self {
        Self {
            raw_host: DEFAULT_RAW_HOST.into(),
            webapp_host: DEFAULT_AUTH_BASE_URL.into(),
            storage_host: DEFAULT_STORAGE_HOST.into(),
        }
    }
}

pub struct ClientOptions {
    pub config: Config,
    pub config_path: Option<PathBuf>,
    pub load_default_config: bool,
    pub endpoints: Endpoints,
    pub auth_base_url: String,
    pub auto_renew: bool,
    pub save_on_renew: bool,
    pub cache_dir: Option<PathBuf>,
    /// Optional native reqwest request deadline, in addition to the attempt bound.
    pub request_timeout: Option<std::time::Duration>,
    pub transport_policy: crate::TransportPolicy,
}
impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            config: Config::default(),
            config_path: None,
            load_default_config: false,
            endpoints: Endpoints::default(),
            auth_base_url: DEFAULT_AUTH_BASE_URL.into(),
            auto_renew: true,
            save_on_renew: true,
            cache_dir: None,
            request_timeout: None,
            transport_policy: crate::TransportPolicy::default(),
        }
    }
}
/// Builds a native client with retries and redirects controlled by this library.
/// Use this client for standalone authentication and discovery helpers.
pub fn new_http_client(request_timeout: Option<std::time::Duration>) -> Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never());
    if let Some(timeout) = request_timeout {
        builder = builder.timeout(timeout);
    }
    Ok(builder.build()?)
}

struct Inner {
    http: reqwest::Client,
    config: RwLock<Config>,
    renew_lock: Mutex<()>,
    config_path: Option<PathBuf>,
    endpoints: Endpoints,
    auth_base_url: String,
    auto_renew: bool,
    save_on_renew: bool,
    cache_dir: Option<PathBuf>,
    transport_policy: crate::TransportPolicy,
}
#[derive(Clone)]
pub struct Client {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("endpoints", &self.inner.endpoints)
            .finish_non_exhaustive()
    }
}
pub(crate) struct Response {
    pub status: u16,
    pub body: Vec<u8>,
    body_error: Option<reqwest::Error>,
    pub(crate) retry_after: Option<String>,
    pub(crate) headers: HeaderMap,
}
impl Response {
    pub(crate) fn headers_only(response: &reqwest::Response) -> Self {
        Self {
            status: response.status().as_u16(),
            body: Vec::new(),
            body_error: None,
            headers: response.headers().clone(),
            retry_after: response
                .headers()
                .get("retry-after")
                .and_then(|h| h.to_str().ok())
                .map(str::to_owned),
        }
    }
    pub(crate) async fn read(mut response: reqwest::Response) -> Self {
        let mut result = Self::headers_only(&response);
        loop {
            match response.chunk().await {
                Ok(Some(chunk)) => result.body.extend_from_slice(&chunk),
                Ok(None) => break,
                Err(error) => {
                    result.body_error = Some(error);
                    break;
                }
            }
        }
        result
    }
    pub(crate) fn body_error(&self) -> Option<&reqwest::Error> {
        self.body_error.as_ref()
    }
    pub(crate) fn into_body(self) -> Result<Vec<u8>> {
        match self.body_error {
            Some(error) => Err(error.into()),
            None => Ok(self.body),
        }
    }
    pub(crate) fn status_error(self, op: impl Into<String>, kind: Option<ErrorKind>) -> Error {
        StatusError {
            op: op.into(),
            status_code: self.status,
            body: String::from_utf8_lossy(&self.body).into_owned(),
            body_error: self.body_error,
            kind: kind
                .or_else(|| matches!(self.status, 401 | 403).then_some(ErrorKind::Unauthorized)),
        }
        .into()
    }
}

impl Client {
    pub fn new(mut options: ClientOptions) -> Result<Self> {
        if let Some(path) = &options.config_path {
            match Config::read_file(path) {
                Ok(config) => options.config = config,
                Err(error) if error.kind() == Some(ErrorKind::ConfigNotFound) => (),
                Err(error) => return Err(error),
            }
        } else if options.load_default_config
            && options.config.device_token.is_empty()
            && options.config.user_token.is_empty()
            && let Ok((config, path)) = load_config(None)
        {
            options.config = config;
            options.config_path = Some(path);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                http: new_http_client(options.request_timeout)?,
                config: RwLock::new(options.config),
                renew_lock: Mutex::new(()),
                config_path: options.config_path,
                endpoints: options.endpoints,
                auth_base_url: options.auth_base_url.trim_end_matches('/').into(),
                auto_renew: options.auto_renew,
                save_on_renew: options.save_on_renew,
                cache_dir: options.cache_dir,
                transport_policy: options.transport_policy,
            }),
        })
    }
    pub async fn config(&self) -> Config {
        self.inner.config.read().await.clone()
    }
    pub fn endpoints(&self) -> &Endpoints {
        &self.inner.endpoints
    }
    pub fn http_client(&self) -> &reqwest::Client {
        &self.inner.http
    }
    pub async fn pair_device(&self, code: &str) -> Result<String> {
        let token = pair_device_with_policy(
            &self.inner.http,
            &self.inner.auth_base_url,
            code,
            DEFAULT_DEVICE_DESC,
            None,
            self.inner.transport_policy,
        )
        .await?;
        self.inner.config.write().await.device_token = token;
        self.renew_token().await
    }
    pub async fn renew_token(&self) -> Result<String> {
        let _guard = self.inner.renew_lock.lock().await;
        self.renew_locked().await
    }
    async fn renew_locked(&self) -> Result<String> {
        let device = self.inner.config.read().await.device_token.clone();
        let token = renew_user_token_with_policy(
            &self.inner.http,
            &self.inner.auth_base_url,
            &device,
            self.inner.transport_policy,
        )
        .await?;
        let mut config = self.inner.config.write().await;
        config.user_token = token.clone();
        if self.inner.save_on_renew
            && let Some(path) = &self.inner.config_path
        {
            config.write_file(path)?;
        }
        Ok(token)
    }
    async fn user_token(&self) -> Result<String> {
        let token = self.inner.config.read().await.user_token.clone();
        if !token.is_empty() {
            return Ok(token);
        }
        let _guard = self.inner.renew_lock.lock().await;
        let token = self.inner.config.read().await.user_token.clone();
        if !token.is_empty() {
            return Ok(token);
        }
        self.renew_locked().await
    }
    pub(crate) async fn request(
        &self,
        method: Method,
        path: &str,
        headers: HeaderMap,
        body: Vec<u8>,
    ) -> Result<Response> {
        let token = self.user_token().await?;
        let send = |token: String| {
            self.inner
                .http
                .request(
                    method.clone(),
                    format!(
                        "{}{}",
                        self.inner.endpoints.storage_host.trim_end_matches('/'),
                        path
                    ),
                )
                .bearer_auth(token)
                .headers(headers.clone())
                .body(body.clone())
        };
        let response =
            crate::transport::execute(send(token.clone()), self.inner.transport_policy).await?;
        let status = response.status;
        if self.inner.auto_renew
            && (matches!(status, 401 | 403)
                || (status == 400 && response.body.windows(5).any(|w| w == b"token")))
        {
            let _guard = self.inner.renew_lock.lock().await;
            let current = self.inner.config.read().await.user_token.clone();
            let renewed = if current != token && !current.is_empty() {
                current
            } else {
                self.renew_locked().await?
            };
            return crate::transport::execute(send(renewed), self.inner.transport_policy).await;
        }
        Ok(response)
    }
    pub async fn get_root_state(&self) -> Result<RootState> {
        let response = self
            .request(Method::GET, "/sync/v3/root", HeaderMap::new(), Vec::new())
            .await?;
        if response.status != 200 {
            return Err(response.status_error("get root state", None));
        }
        Ok(serde_json::from_slice(&response.into_body()?)?)
    }
    pub async fn get_blob(&self, hash: &str, filename: &str) -> Result<Vec<u8>> {
        self.blob(hash, filename, true).await
    }
    pub async fn get_blob_fresh(&self, hash: &str, filename: &str) -> Result<Vec<u8>> {
        self.blob(hash, filename, false).await
    }
    async fn blob(&self, hash: &str, filename: &str, cache: bool) -> Result<Vec<u8>> {
        // Hashes are URL path components and cache names, never filesystem paths.
        if hash.is_empty() || hash.contains(['/', '\\', '?', '#']) || hash == "." || hash == ".." {
            return Err(Error::domain(
                ErrorKind::InvalidSchema,
                "invalid blob hash path component",
            ));
        }
        let cache_path = self
            .inner
            .cache_dir
            .as_ref()
            .map(|p| p.join("blobs").join(hash));
        if cache
            && let Some(path) = &cache_path
            && let Ok(bytes) = std::fs::read(path)
            && !bytes.is_empty()
        {
            return Ok(bytes);
        }
        let mut headers = HeaderMap::new();
        if !filename.is_empty() {
            insert_header(&mut headers, "rm-filename", filename)?;
        }
        let response = self
            .request(
                Method::GET,
                &format!("/sync/v3/files/{hash}"),
                headers,
                Vec::new(),
            )
            .await?;
        if response.status != 200 {
            let kind = (response.status == 404).then_some(ErrorKind::ItemNotFound);
            return Err(response.status_error(format!("get blob {hash} ({filename})"), kind));
        }
        let bytes = response.into_body()?;
        if cache && let Some(path) = cache_path {
            let _ = cache_bytes(&path, &bytes);
        }
        Ok(bytes)
    }
    pub async fn get_manifest(&self, hash: &str, filename: &str) -> Result<Manifest> {
        let filename = if filename.is_empty() || filename == "root" {
            "root.docSchema"
        } else {
            filename
        };
        let bytes = self.get_blob(hash, filename).await?;
        crate::read_manifest(std::io::Cursor::new(bytes), hash)
    }
    pub(crate) async fn manifest_fresh(&self, hash: &str, filename: &str) -> Result<Manifest> {
        let bytes = self.get_blob_fresh(hash, filename).await?;
        // Manifests are ASCII; invalid UTF-8 is a schema failure rather than silently repaired text.
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| Error::domain(ErrorKind::InvalidSchema, e.to_string()))?;
        parse_manifest(text, hash)
    }
    pub async fn get_content<T: DeserializeOwned>(&self, hash: &str, filename: &str) -> Result<T> {
        Ok(serde_json::from_slice(
            &self.get_blob(hash, filename).await?,
        )?)
    }
    pub async fn get_document_content(
        &self,
        hash: &str,
        filename: &str,
    ) -> Result<DocumentContent> {
        self.get_content(hash, filename).await
    }
    pub async fn discover_endpoints(&self, url: Option<&str>) -> Discovery {
        discover_endpoints_with_policy(&self.inner.http, url, self.inner.transport_policy).await
    }
}

fn cache_bytes(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::message("cache path has no parent"))?;
    std::fs::create_dir_all(parent)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(bytes)?;
    file.persist(path).map_err(|e| Error::Io(e.error))?;
    Ok(())
}
pub(crate) fn insert_header(
    headers: &mut HeaderMap,
    name: &'static str,
    value: &str,
) -> Result<()> {
    headers.insert(
        name,
        value
            .parse()
            .map_err(|e| Error::message(format!("invalid {name} header: {e}")))?,
    );
    Ok(())
}
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PairDeviceRequest {
    pub code: String,
    pub device_desc: String,
    #[serde(rename = "deviceID")]
    pub device_id: String,
    pub secret: String,
}
pub fn new_device_id() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub async fn pair_device(
    http: &reqwest::Client,
    base: &str,
    code: &str,
    device_desc: &str,
    device_id: Option<&str>,
) -> Result<String> {
    pair_device_with_policy(
        http,
        base,
        code,
        device_desc,
        device_id,
        crate::TransportPolicy::default(),
    )
    .await
}
async fn pair_device_with_policy(
    http: &reqwest::Client,
    base: &str,
    code: &str,
    device_desc: &str,
    device_id: Option<&str>,
    policy: crate::TransportPolicy,
) -> Result<String> {
    let base = if base.is_empty() {
        DEFAULT_AUTH_BASE_URL
    } else {
        base
    };
    let payload = PairDeviceRequest {
        code: code.trim().into(),
        device_desc: if device_desc.is_empty() {
            DEFAULT_DEVICE_DESC.into()
        } else {
            device_desc.into()
        },
        device_id: device_id
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .unwrap_or_else(new_device_id),
        secret: String::new(),
    };
    auth_response(
        http.post(format!(
            "{}/token/json/2/device/new",
            base.trim_end_matches('/')
        ))
        .json(&payload),
        "pair device",
        policy,
    )
    .await
}
pub async fn renew_user_token(
    http: &reqwest::Client,
    base: &str,
    device_token: &str,
) -> Result<String> {
    renew_user_token_with_policy(http, base, device_token, crate::TransportPolicy::default()).await
}
async fn renew_user_token_with_policy(
    http: &reqwest::Client,
    base: &str,
    device_token: &str,
    policy: crate::TransportPolicy,
) -> Result<String> {
    if device_token.trim().is_empty() {
        return Err(Error::domain(
            ErrorKind::Unauthorized,
            "missing device token",
        ));
    }
    let base = if base.is_empty() {
        DEFAULT_AUTH_BASE_URL
    } else {
        base
    };
    auth_response(
        http.post(format!(
            "{}/token/json/2/user/new",
            base.trim_end_matches('/')
        ))
        .bearer_auth(device_token.trim()),
        "renew user token",
        policy,
    )
    .await
}
async fn auth_response(
    request: reqwest::RequestBuilder,
    op: &str,
    policy: crate::TransportPolicy,
) -> Result<String> {
    let response = crate::transport::execute(request, policy).await?;
    if response.status != 200 {
        return Err(response.status_error(op, None));
    }
    let body = response.into_body()?;
    let token = std::str::from_utf8(&body)
        .map_err(|e| Error::message(e.to_string()))?
        .trim()
        .trim_matches('"');
    if token.is_empty() {
        return Err(Error::message(format!("empty token received from {op}")));
    }
    Ok(token.into())
}

/// Discovery always supplies usable fallback hosts; inspect `error` before using them.
#[derive(Debug)]
pub struct Discovery {
    pub endpoints: Endpoints,
    pub error: Option<Error>,
}
pub async fn discover_endpoints(http: &reqwest::Client, url: Option<&str>) -> Discovery {
    discover_endpoints_with_policy(http, url, crate::TransportPolicy::default()).await
}
async fn discover_endpoints_with_policy(
    http: &reqwest::Client,
    url: Option<&str>,
    policy: crate::TransportPolicy,
) -> Discovery {
    async fn query(
        http: &reqwest::Client,
        url: &str,
        policy: crate::TransportPolicy,
    ) -> Result<Endpoints> {
        let response = crate::transport::execute(http.get(url), policy).await?;
        if response.status != 200 {
            return Err(
                response.status_error("discover endpoints", Some(ErrorKind::DiscoveryFailed))
            );
        }
        let body = response.into_body()?;
        let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_slice(&body)?;
        let string = |key: &str| {
            raw.get(key)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(|s| s.trim_end_matches('/').to_owned())
        };
        let mut endpoints = Endpoints::default();
        if let Some(raw) =
            string("raw_host").or_else(|| string("service-manager").map(|s| format!("https://{s}")))
        {
            endpoints.raw_host = raw;
        }
        if let Some(webapp) = string("webapp_host") {
            endpoints.webapp_host = webapp;
        }
        if let Some(storage) = string("storage_host") {
            endpoints.storage_host = storage;
        }
        Ok(endpoints)
    }
    match query(
        http,
        url.filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_DISCOVERY_URL),
        policy,
    )
    .await
    {
        Ok(endpoints) => Discovery {
            endpoints,
            error: None,
        },
        Err(error) => Discovery {
            endpoints: Endpoints::default(),
            error: Some(error),
        },
    }
}
