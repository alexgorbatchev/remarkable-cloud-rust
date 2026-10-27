`remarkable-cloud` provides asynchronous reMarkable Cloud Sync v3 access for Rust applications that browse documents, decode manifests, and perform snapshot-checked document writes.

# What It Does

- **Authentication**: Pairs devices, renews sessions, and reads or atomically writes rmapi credential files.
- **Browsing**: Lists documents and collections and resolves IDs, names, and folder paths with typed ambiguity errors.
- **Content**: Downloads manifests and blobs, optionally caches immutable bytes, and preserves unknown document-content fields.
- **Writes**: Stages and downloads every uploaded file before committing a generation-checked root; returns progress on failure.

# How It Works

1. Construct a client with credentials or enable loading an existing rmapi configuration.
2. Read the root snapshot, browse documents, and download their content.
3. For a write, provide encoded files and the snapshot used during preflight.
4. Inspect the returned progress before deciding how to recover any failed operation.

# How it Really Works

1. Client clones share credentials and serialize renewal. HTTP calls are asynchronous; small configuration and cache operations use blocking filesystem access.
2. Each physical request has a 90-second deadline, including reading its body. Hash-addressed immutable blob GETs and PUTs recover from selected transient failures with at most three attempts. Recovery waits 250ms then 500ms, unless `Retry-After` supplies seconds or an HTTP date. Other requests run once. Reads and replayable blob uploads can follow redirects; each hop gets its own deadline and immutable recovery. Root commits and authentication writes never follow redirects. An explicit 401, 403, or token-related 400 can renew authentication and replay the rejected request once.
3. Cache entries live under `<cache_dir>/blobs/<hash>`. `get_blob_fresh` bypasses both cache reads and writes. The cache trusts existing bytes; write verification always downloads fresh bytes.
4. Creation and snapshot-bound updates reject a changed root hash or generation before uploads. Root commits run once, and errors distinguish `staged`, `commit-unknown`, `committed`, and `verified`. Optional creation and update progress callbacks run synchronously before staging, before commit, after commit, and after verification.
5. Dropping a network future cancels local waiting. After a commit might have been sent, cancellation cannot establish the server outcome; retain callback progress outside the future before commit and inspect cloud state before retrying.

# Prerequisites

- A [Tokio](https://docs.rs/tokio/latest/tokio/) runtime for network calls.
- reMarkable Cloud credentials for cloud access. Offline parsing needs no credentials or network.

# Installation

From a sibling Rust project in this local collection:

```sh
cargo add remarkable-cloud --path ../remarkable-cloud
```

# Quick Start

This complete offline example parses a document manifest:

```rust
use remarkable_cloud::parse_manifest;

fn main() -> remarkable_cloud::Result<()> {
    let manifest = parse_manifest("3\nfilehash:0:document.content:0:2\n", "documenthash")?;
    println!("{}", manifest.find_suffix(".content").unwrap().id);
    Ok(())
}
```

For a read-only cloud call using existing rmapi credentials:

```rust,no_run
use remarkable_cloud::{Client, ClientOptions};

#[tokio::main]
async fn main() -> remarkable_cloud::Result<()> {
    let client = Client::new(ClientOptions {
        load_default_config: true,
        ..ClientOptions::default()
    })?;
    let root = client.get_root_state().await?;
    println!("{} {}", root.hash, root.generation);
    Ok(())
}
```

# API

| Entry point | Result | Purpose |
| :--- | :--- | :--- |
| `Config::{read, parse, read_file, write, write_file}` | `Result` | Parse rmapi credentials and persist them atomically |
| `Client::{pair_device, renew_token}` | `Result<String>` | Obtain and retain credentials |
| `new_http_client` | `Result<reqwest::Client>` | Build the native client for standalone authentication/discovery helpers |
| `Client::discover_endpoints` | `Discovery` | Discover hosts while retaining defaults alongside failures |
| `Client::{get_root_state, get_manifest, get_blob, get_blob_fresh}` | `Result` | Read sync snapshots and file bytes |
| `Client::{get_content, get_document_content}` | `Result` | Decode JSON content |
| `Client::{list_items, resolve, resolve_by_id, resolve_by_name, resolve_by_path}` | `Result` | Browse and select cloud items |
| `Client::update_document_files_at_root` | `MutationResult<UpdateResult>` | Update against an explicit root and destination snapshot |
| `Client::update_document_files` | `MutationResult<UpdateResult>` | Update against the currently fetched root and expected destination hash |
| `Client::{update_document_files_with_progress, update_document_files_at_root_with_progress}` | `MutationResult<UpdateResult>` | Retain update progress outside a cancellable future |
| `Client::{create_document, create_document_with_progress}` | `MutationResult<CreateResult>` | Create a new ID and optionally persist progress |
| `encode_cloud_manifest` | `Result<EncodedManifest>` | Produce document-v3 or root-v4 bytes and hashes |

`MutationFailure` retains both `progress` and `error`. Match `Error::Status` for HTTP status/body, including a truncated rejection's native `body_error` and standard error source; `Error::AttemptTimeout` for the per-attempt deadline; `Error::RedirectLimit` for ten consecutive redirect responses; `Error::Http` for transport and successful-response body failures; `Error::Ambiguous` for sorted candidates; and `Error::kind()` for domain classification. `Error::Multiple` retains every item-listing failure. Pure resolution helpers operate on the supplied item set; supply live items when resolving visible folder paths.

Immutable recovery applies only to GET or PUT paths `/sync/v3/files/<64 hexadecimal characters>`. It retries HTTP 408, 429, 500, 502, 503, and 504; timeouts; connection refusal, reset, broken pipe, and unexpected EOF; and incomplete or closed HTTP connections. Complete successful bytes are returned only after body reading finishes. Malformed protocol messages and other rejections return immediately. Dropping the future cancels a pending attempt or retry delay. Root commits and authentication writes do not recover from transport failures.

Native [tower-http redirect middleware](https://docs.rs/tower-http/latest/tower_http/follow_redirect/struct.FollowRedirect.html) follows redirects only when the original request is GET, HEAD, or a replayable immutable blob PUT. HTTP 301/302/303 drops the body and converts PUT to GET; 307/308 preserves the current method and body. The tenth redirect response returns `Error::RedirectLimit`. Sensitive headers are removed permanently when leaving the original host and its subdomains; hostname case and port changes preserve credentials. The client owns outgoing headers and disables reqwest's automatic retries and redirects. Standalone authentication and discovery helpers require a client built with `new_http_client`, or the client returned by `Client::http_client()`.

For diagnostics, enable the `remarkable_cloud::http` tracing target at DEBUG. Each physical attempt emits its method, path, filename, attempt number, received status when available, elapsed milliseconds, and outcome. Events include failed and cancelled attempts; they omit authorization headers, request bodies, response bodies, URL query strings, and user information.

# Configuration

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `config` | `Config` | Empty tokens | In-memory credentials |
| `config_path` | `Option<PathBuf>` | None | Load a config and save renewed credentials there |
| `load_default_config` | `bool` | `false` | Search rmapi configuration locations when credentials are empty |
| `endpoints` | `Endpoints` | reMarkable service hosts | Configure raw, webapp, and storage hosts |
| `auth_base_url` | `String` | Production webapp host | Pairing and renewal host |
| `auto_renew` | `bool` | `true` | Renew after explicit credential rejection |
| `save_on_renew` | `bool` | `true` | Persist renewed tokens when a config path is known; failures are returned |
| `cache_dir` | `Option<PathBuf>` | None | Store downloaded immutable blobs |
| `request_timeout` | `Option<Duration>` | `None` | Additional native reqwest timeout for each physical request; the attempt deadline still applies |
| `transport_policy.attempt_timeout` | `Duration` | 90 seconds | Deadline for each physical request and its full response body |
| `transport_policy.retry_delay` | `Duration` | 250 milliseconds | Base exponential delay for immutable recovery |
| `RMAPI_CONFIG` | Environment | Unset | Existing config file or directory containing rmapi candidates |
| `XDG_CONFIG_HOME` | Environment | `~/.config` | Additional rmapi config directory |

Config discovery also checks `~/.rmapi`, `~/.rmapi.conf`, and macOS Application Support locations. Unix credential files use mode `0600`; newly created parent directories use `0700`.

# License

[MIT](LICENSE), copyright 2026 Alex Gorbatchev and attributed upstream contributors.
