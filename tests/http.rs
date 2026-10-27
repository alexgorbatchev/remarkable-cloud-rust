use remarkable_cloud::*;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tiny_http::{Header, Response, Server};

#[tokio::test]
async fn truncated_rejection_keeps_status_while_success_reports_read_failure() {
    for status in [400, 404, 503, 200] {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let thread = thread::spawn(move || {
            use std::io::{BufRead, Write};
            let (mut stream, _) = listener.accept().unwrap();
            let mut reader = std::io::BufReader::new(&stream);
            loop {
                let mut line = String::new();
                assert!(reader.read_line(&mut line).unwrap() > 0);
                if line == "\r\n" {
                    break;
                }
            }
            write!(stream, "HTTP/1.1 {status} response\r\nContent-Length: 40\r\nConnection: close\r\n\r\npartial").unwrap();
        });
        let options = ClientOptions {
            config: Config {
                user_token: "synthetic".into(),
                ..Config::default()
            },
            endpoints: Endpoints {
                storage_host: url,
                ..Endpoints::default()
            },
            ..ClientOptions::default()
        };
        let error = Client::new(options)
            .unwrap()
            .get_blob_fresh("hash", "name")
            .await
            .unwrap_err();
        if status != 200 {
            use std::error::Error as _;
            let Error::Status(rejection) = error else {
                panic!("expected typed rejection");
            };
            assert_eq!(rejection.status_code, status);
            assert_eq!(rejection.body, "partial");
            assert_eq!(
                rejection.kind,
                (status == 404).then_some(ErrorKind::ItemNotFound)
            );
            assert!(
                rejection
                    .source()
                    .and_then(|error| error.downcast_ref::<reqwest::Error>())
                    .is_some(),
                "truncated rejection must retain its typed body read error"
            );
        } else {
            assert!(matches!(error, Error::Http(_)));
        }
        thread.join().unwrap();
    }
}

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}
struct Reply {
    status: u16,
    body: Vec<u8>,
    headers: Vec<(&'static str, String)>,
}
impl Reply {
    fn bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            status: 200,
            body: bytes.into(),
            headers: Vec::new(),
        }
    }
    fn json(value: serde_json::Value) -> Self {
        Self::bytes(serde_json::to_vec(&value).unwrap())
    }
    fn status(status: u16, body: &str) -> Self {
        Self {
            status,
            ..Self::bytes(body.as_bytes())
        }
    }
}
struct Http {
    url: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Http {
    fn new(handler: impl Fn(Request) -> Reply + Send + 'static) -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let url = format!("http://{}", server.server_addr());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let logged = requests.clone();
        let stopping = stop.clone();
        let thread = thread::spawn(move || {
            while !stopping.load(Ordering::SeqCst) {
                let Some(mut incoming) = server.recv_timeout(Duration::from_millis(20)).unwrap()
                else {
                    continue;
                };
                let mut body = Vec::new();
                incoming.as_reader().read_to_end(&mut body).unwrap();
                let request = Request {
                    method: incoming.method().as_str().into(),
                    path: incoming.url().into(),
                    headers: incoming
                        .headers()
                        .iter()
                        .map(|h| {
                            (
                                h.field.as_str().to_string().to_ascii_lowercase(),
                                h.value.as_str().into(),
                            )
                        })
                        .collect(),
                    body,
                };
                logged.lock().unwrap().push(request.clone());
                let reply = handler(request);
                if reply.status == 0 {
                    drop(incoming.into_writer());
                    continue;
                }
                let mut response = Response::from_data(reply.body).with_status_code(reply.status);
                for (name, value) in reply.headers {
                    response.add_header(Header::from_bytes(name, value).unwrap());
                }
                let _ = incoming.respond(response);
            }
        });
        Self {
            url,
            requests,
            stop,
            thread: Some(thread),
        }
    }
    fn options(&self) -> ClientOptions {
        ClientOptions {
            config: Config {
                device_token: "synthetic-device".into(),
                user_token: "synthetic-user".into(),
            },
            endpoints: Endpoints {
                storage_host: self.url.clone(),
                ..Endpoints::default()
            },
            auth_base_url: self.url.clone(),
            ..ClientOptions::default()
        }
    }
    fn client(&self) -> Client {
        Client::new(self.options()).unwrap()
    }
    fn count(&self, method: &str, path: &str) -> usize {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == method && r.path == path)
            .count()
    }
}
impl Drop for Http {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn immutable_blob_recovers_transient_responses_without_changing_request() {
    let seen = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = seen.clone();
    let server = Http::new(move |_| {
        if count.fetch_add(1, Ordering::SeqCst) < 2 {
            Reply {
                headers: vec![("Retry-After", "0".into())],
                ..Reply::status(503, "retry")
            }
        } else {
            Reply::bytes(b"complete".to_vec())
        }
    });
    let hash = "a".repeat(64);
    let body = server
        .client()
        .get_blob_fresh(&hash, "document.rm")
        .await
        .unwrap();
    assert_eq!(body, b"complete");
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in &requests[1..] {
        assert_eq!(request.method, requests[0].method);
        assert_eq!(request.path, requests[0].path);
        assert_eq!(request.body, requests[0].body);
        assert_eq!(request.headers, requests[0].headers);
    }
}

#[tokio::test]
async fn credential_persistence_failure_is_reported_after_renewal() {
    let server = Http::new(|_| Reply::bytes("synthetic-new"));
    let dir = tempfile::tempdir().unwrap();
    let blocker = dir.path().join("file");
    let mut options = server.options();
    options.config_path = Some(blocker.join("config"));
    let client = Client::new(options).unwrap();
    std::fs::write(&blocker, "occupied").unwrap();
    assert!(matches!(
        client.renew_token().await.unwrap_err(),
        Error::Io(_)
    ));
    assert_eq!(client.config().await.user_token, "synthetic-new");
}

#[test]
fn explicitly_selected_unreadable_config_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let options = ClientOptions {
        config_path: Some(dir.path().into()),
        ..ClientOptions::default()
    };
    assert!(Client::new(options).is_err());
}

#[tokio::test]
async fn rejected_auth_renews_once_and_replays_exact_bytes_and_headers() {
    let server = Http::new(|r| {
        if r.path.ends_with("/user/new") {
            assert_eq!(r.headers["authorization"], "Bearer synthetic-device");
            assert!(r.body.is_empty());
            Reply::bytes("\"synthetic-new\"\n")
        } else if r.headers["authorization"] == "Bearer synthetic-user" {
            Reply::status(400, "bad token")
        } else {
            assert_eq!(r.headers["rm-filename"], "doc.content");
            Reply::bytes("payload")
        }
    });
    assert_eq!(
        server
            .client()
            .get_blob_fresh("hash", "doc.content")
            .await
            .unwrap(),
        b"payload"
    );
    assert_eq!(server.count("POST", "/token/json/2/user/new"), 1);
    assert_eq!(server.count("GET", "/sync/v3/files/hash"), 2);
}

#[tokio::test]
async fn pairing_defaults_uuid_and_token_errors_preserve_classification() {
    let server = Http::new(|r| {
        if r.path.ends_with("/device/new") {
            let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
            assert_eq!(v["code"], "abcdefgh");
            assert_eq!(v["deviceDesc"], DEFAULT_DEVICE_DESC);
            assert_eq!(v["secret"], "");
            assert_eq!(
                uuid::Uuid::parse_str(v["deviceID"].as_str().unwrap())
                    .unwrap()
                    .get_version_num(),
                4
            );
            Reply::bytes("\"synthetic-paired\"")
        } else {
            Reply::bytes("synthetic-session")
        }
    });
    let client = server.client();
    assert_eq!(
        client.pair_device(" abcdefgh ").await.unwrap(),
        "synthetic-session"
    );
    assert_eq!(client.config().await.device_token, "synthetic-paired");
    let empty = Http::new(|_| Reply::bytes(" \"\"\n"));
    assert!(
        pair_device(
            empty.client().http_client(),
            &empty.url,
            "c",
            "custom",
            Some("custom-id")
        )
        .await
        .is_err()
    );
    assert_eq!(
        renew_user_token(client.http_client(), &server.url, " ")
            .await
            .unwrap_err()
            .kind(),
        Some(ErrorKind::Unauthorized)
    );
    let fail = Http::new(|_| Reply::status(503, " unavailable\n"));
    let error = fail.client().renew_token().await.unwrap_err();
    assert_eq!(error.kind(), None);
    assert!(
        matches!(error, Error::Status(StatusError { status_code: 503, body, .. }) if body == " unavailable\n")
    );
}

#[tokio::test]
async fn discovery_preserves_fallbacks_and_rejected_status() {
    let server = Http::new(|r| {
        if r.path == "/good" {
            Reply::json(
                serde_json::json!({"service-manager":"raw.example/", "webapp_host":" https://web.example/ ", "storage_host":"https://storage.example/"}),
            )
        } else if r.path == "/bad-json" {
            Reply::bytes("{")
        } else {
            Reply::status(401, " refused\n")
        }
    });
    let client = server.client();
    let result = client
        .discover_endpoints(Some(&format!("{}/good", server.url)))
        .await;
    assert!(result.error.is_none());
    assert_eq!(result.endpoints.raw_host, "https://raw.example");
    assert_eq!(result.endpoints.webapp_host, "https://web.example");
    let result = client.discover_endpoints(Some(&server.url)).await;
    assert_eq!(result.endpoints, Endpoints::default());
    assert_eq!(
        result.error.unwrap().kind(),
        Some(ErrorKind::DiscoveryFailed)
    );
    assert!(
        client
            .discover_endpoints(Some(&format!("{}/bad-json", server.url)))
            .await
            .error
            .is_some()
    );
}

#[tokio::test]
async fn read_redirects_are_limited_and_other_rejections_do_not_recover() {
    for status in [307, 308, 500, 400, 403, 404] {
        let server = Http::new(move |_| Reply {
            headers: vec![("Location", "/another".into())],
            ..Reply::status(status, " refused\n")
        });
        let mut options = server.options();
        options.auto_renew = false;
        let error = Client::new(options)
            .unwrap()
            .get_blob("hash", "name")
            .await
            .unwrap_err();
        if [307, 308].contains(&status) {
            assert!(matches!(error, Error::RedirectLimit));
            assert_eq!(server.requests.lock().unwrap().len(), 10);
        } else {
            assert!(
                matches!(error, Error::Status(StatusError { status_code, body, .. }) if status_code == status && body == " refused\n")
            );
            assert_eq!(server.requests.lock().unwrap().len(), 1);
        }
    }
    let server = Http::new(|_| Reply::status(400, "non-auth bad request"));
    assert!(server.client().get_root_state().await.is_err());
    assert_eq!(server.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cache_fresh_download_and_item_content_use_correct_filenames() {
    let server = Http::new(|r| match r.path.as_str() {
        "/sync/v3/files/doc" => {
            assert_eq!(r.headers["rm-filename"], "id.docSchema");
            Reply::bytes("3\nbody:0:id.content:0:2\n")
        }
        "/sync/v3/files/body" => {
            assert_eq!(r.headers["rm-filename"], "id.content");
            Reply::json(serde_json::json!({"fileType":"pdf","pageCount":4}))
        }
        _ => Reply::bytes("3\nx:0:file:0:2\n"),
    });
    let dir = tempfile::tempdir().unwrap();
    let mut options = server.options();
    options.cache_dir = Some(dir.path().into());
    let client = Client::new(options).unwrap();
    client.get_manifest("root", "root").await.unwrap();
    client.get_manifest("root", "").await.unwrap();
    assert_eq!(server.count("GET", "/sync/v3/files/root"), 1);
    client
        .get_blob_fresh("root", "root.docSchema")
        .await
        .unwrap();
    assert_eq!(server.count("GET", "/sync/v3/files/root"), 2);
    let mut item = Item::new("id", "doc", ItemMetadata::default(), vec![]);
    assert!(item.get_manifest().await.is_err());
    item.bind_client(client.clone());
    assert_eq!(item.get_content().await.unwrap().page_count, 4);
    assert!(item.client().is_some());
    let mut missing = Item::new("missing", "", ItemMetadata::default(), vec![]);
    missing.bind_client(client.clone());
    assert_eq!(
        missing.get_manifest().await.unwrap_err().kind(),
        Some(ErrorKind::ItemNotFound)
    );
    assert_eq!(
        missing.get_content().await.unwrap_err().kind(),
        Some(ErrorKind::ItemNotFound)
    );
    let mut via_entry = Item::new(
        "id",
        "",
        ItemMetadata::default(),
        vec![
            SchemaEntry::new("doc", "id.docSchema", 1, 1),
            SchemaEntry::new("body", "id.content", 0, 2),
        ],
    );
    via_entry.bind_client(client.clone());
    assert_eq!(via_entry.get_manifest().await.unwrap().entries.len(), 1);
    assert_eq!(via_entry.get_content().await.unwrap().file_type, "pdf");
    for invalid in ["..", "x/y", "x\\y", "x?y", ""] {
        assert!(client.get_blob(invalid, "name").await.is_err());
    }
}

#[derive(Clone)]
struct CloudState {
    root: RootState,
    blobs: HashMap<String, Vec<u8>>,
    commit_status: u16,
    corrupt_verification: bool,
    transient_uploads: usize,
}
fn cloud() -> (Http, Arc<Mutex<CloudState>>, String) {
    let data = br#"{"visibleName":"Old","type":"DocumentType"}"#.to_vec();
    let file_hash = content_hash(&data);
    let doc = encode_cloud_manifest(
        &[SchemaEntry::new(
            &file_hash,
            "old.metadata",
            0,
            data.len() as i64,
        )],
        false,
    )
    .unwrap();
    let doc_hash = doc.hash.clone();
    let root =
        encode_cloud_manifest(&[SchemaEntry::new(&doc.hash, "old", 1, doc.size)], true).unwrap();
    let state = Arc::new(Mutex::new(CloudState {
        root: RootState {
            hash: root.hash.clone(),
            generation: 4,
            schema_version: 4,
        },
        blobs: HashMap::from([
            (file_hash, data),
            (doc.hash, doc.data),
            (root.hash, root.data),
        ]),
        commit_status: 200,
        corrupt_verification: false,
        transient_uploads: 0,
    }));
    let shared = state.clone();
    let server = Http::new(move |r| {
        let mut state = shared.lock().unwrap();
        if r.path == "/sync/v3/root" {
            if r.method == "PUT" {
                assert_eq!(r.headers["rm-filename"], "roothash");
                assert_eq!(r.headers["content-type"], "application/json");
                if state.commit_status != 200 {
                    return Reply::status(state.commit_status, "commit failure");
                }
                let v: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
                assert_eq!(v["broadcast"], true);
                assert_eq!(v["generation"], state.root.generation);
                state.root.hash = v["hash"].as_str().unwrap().into();
                state.root.generation += 1;
                if state.corrupt_verification {
                    let hash = state.root.hash.clone();
                    state.blobs.insert(hash, b"4\n0:.:0:0\n".to_vec());
                }
            }
            return Reply::json(serde_json::to_value(&state.root).unwrap());
        }
        let hash = r.path.strip_prefix("/sync/v3/files/").unwrap();
        if r.method == "PUT" {
            if state.transient_uploads > 0 {
                state.transient_uploads -= 1;
                return Reply {
                    headers: vec![("Retry-After", "0".into())],
                    ..Reply::status(if state.transient_uploads == 0 { 0 } else { 503 }, "retry")
                };
            }
            use base64::Engine;
            assert_eq!(
                r.headers["x-goog-hash"],
                format!(
                    "crc32c={}",
                    base64::engine::general_purpose::STANDARD
                        .encode(crc32c::crc32c(&r.body).to_be_bytes())
                )
            );
            let name = &r.headers["rm-filename"];
            assert_eq!(
                r.headers["content-type"],
                if name == "root.docSchema" {
                    "text/plain; charset=UTF-8"
                } else {
                    "application/octet-stream"
                }
            );
            state.blobs.insert(hash.into(), r.body);
            Reply::status(201, "")
        } else {
            state.blobs.get(hash).map_or_else(
                || Reply::status(404, "missing"),
                |b| Reply::bytes(b.clone()),
            )
        }
    });
    (server, state, doc_hash)
}
fn new_document(root: RootState) -> CreateDocumentOptions {
    CreateDocumentOptions {
        id: "new".into(),
        expected_root: root,
        files: vec![
            FileUpdate {
                name: "new.metadata".into(),
                data: br#"{"visibleName":"New","type":"DocumentType"}"#.to_vec(),
            },
            FileUpdate {
                name: "new.content".into(),
                data: b"{}".to_vec(),
            },
        ],
    }
}

#[tokio::test]
async fn update_progress_tracks_commit_boundaries_and_callback_failures() {
    for at_root in [false, true] {
        for fail_at in [
            None,
            Some(UpdateState::Staged),
            Some(UpdateState::CommitUnknown),
            Some(UpdateState::Committed),
            Some(UpdateState::Verified),
        ] {
            let (server, state, hash) = cloud();
            let options = UpdateDocumentOptions {
                id: "old".into(),
                expected_hash: hash,
                expected_root: state.lock().unwrap().root.clone(),
                files: vec![FileUpdate {
                    name: "old.content".into(),
                    data: b"{}".to_vec(),
                }],
            };
            let mut states = Vec::new();
            let mut callback = |progress: UpdateResult| {
                states.push(progress.state);
                if fail_at == Some(progress.state) {
                    Err(Error::Message("callback rejected progress".into()))
                } else {
                    Ok(())
                }
            };
            let client = server.client();
            let result = if at_root {
                client
                    .update_document_files_at_root_with_progress(&options, &mut callback)
                    .await
            } else {
                client
                    .update_document_files_with_progress(
                        &options.id,
                        &options.expected_hash,
                        &options.files,
                        &mut callback,
                    )
                    .await
            };
            let expected = match fail_at {
                Some(UpdateState::Staged | UpdateState::CommitUnknown) => UpdateState::Staged,
                Some(UpdateState::Committed) => UpdateState::Committed,
                _ => UpdateState::Verified,
            };
            let progress = match result {
                Ok(result) => {
                    assert!(fail_at.is_none());
                    result
                }
                Err(failure) => {
                    assert!(fail_at.is_some());
                    failure.progress
                }
            };
            assert_eq!(progress.state, expected);
            assert_eq!(
                server.count("PUT", "/sync/v3/root"),
                usize::from(matches!(
                    expected,
                    UpdateState::Committed | UpdateState::Verified
                ))
            );
            assert_eq!(states[0], UpdateState::Staged);
            if fail_at.is_none() {
                assert_eq!(
                    states,
                    [
                        UpdateState::Staged,
                        UpdateState::CommitUnknown,
                        UpdateState::Committed,
                        UpdateState::Verified
                    ]
                );
            }
        }
    }
}

#[tokio::test]
async fn update_cancellation_and_deadline_leave_externally_observed_commit_unknown() {
    use std::cell::Cell;
    for cancel in [false, true] {
        let (server, state, hash) = cloud();
        let upstream = server.url.clone();
        let proxy = Http::new(move |request| {
            if request.method == "PUT" && request.path == "/sync/v3/root" {
                thread::sleep(Duration::from_millis(120));
                return Reply::json(serde_json::json!({"generation":5}));
            }
            // The blocking proxy is a real network peer; use a dedicated runtime for upstream I/O.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async {
                let response = reqwest::Client::new()
                    .request(
                        request.method.parse().unwrap(),
                        format!("{upstream}{}", request.path),
                    )
                    .headers(
                        request
                            .headers
                            .iter()
                            .map(|(k, v)| (k.parse().unwrap(), v.parse().unwrap()))
                            .collect(),
                    )
                    .body(request.body)
                    .send()
                    .await
                    .unwrap();
                Reply {
                    status: response.status().as_u16(),
                    body: response.bytes().await.unwrap().to_vec(),
                    headers: Vec::new(),
                }
            })
        });
        let options = UpdateDocumentOptions {
            id: "old".into(),
            expected_hash: hash,
            expected_root: state.lock().unwrap().root.clone(),
            files: vec![FileUpdate {
                name: "old.content".into(),
                data: b"{}".to_vec(),
            }],
        };
        let observed = Cell::new(UpdateState::Staged);
        let client = proxy.client();
        let (sent, received) = tokio::sync::oneshot::channel();
        let mut sent = Some(sent);
        let mut callback = |progress: UpdateResult| {
            observed.set(progress.state);
            if progress.state == UpdateState::CommitUnknown {
                sent.take().unwrap().send(()).unwrap();
            }
            Ok(())
        };
        {
            let operation =
                client.update_document_files_at_root_with_progress(&options, &mut callback);
            tokio::pin!(operation);
            let interrupt = async {
                received.await.unwrap();
                tokio::time::sleep(Duration::from_millis(if cancel { 15 } else { 30 })).await;
            };
            tokio::select! { result = &mut operation => panic!("write unexpectedly completed: {result:?}"), () = interrupt => {} }
        }
        assert_eq!(observed.get(), UpdateState::CommitUnknown);
        assert_eq!(proxy.count("PUT", "/sync/v3/root"), 1);
    }
}

#[tokio::test]
async fn immutable_upload_recovery_preserves_exact_bytes_headers_and_single_root_commit() {
    let (server, state, _) = cloud();
    state.lock().unwrap().transient_uploads = 2;
    let options = new_document(state.lock().unwrap().root.clone());
    let mut client_options = server.options();
    client_options.transport_policy.attempt_timeout = Duration::from_millis(300);
    Client::new(client_options)
        .unwrap()
        .create_document(&options)
        .await
        .unwrap();
    let requests = server.requests.lock().unwrap();
    let uploads = requests
        .iter()
        .filter(|r| r.method == "PUT" && r.path.starts_with("/sync/v3/files/"))
        .collect::<Vec<_>>();
    for request in &uploads[1..3] {
        assert_eq!(request.path, uploads[0].path);
        assert_eq!(request.body, uploads[0].body);
        assert_eq!(request.headers, uploads[0].headers);
    }
    assert_eq!(uploads[0].body, options.files[0].data);
    assert_eq!(
        requests
            .iter()
            .filter(|r| r.method == "PUT" && r.path == "/sync/v3/root")
            .count(),
        1
    );
}

#[tokio::test]
async fn create_reports_all_progress_and_preserves_other_documents() {
    let (server, state, _) = cloud();
    let options = new_document(state.lock().unwrap().root.clone());
    let mut events = Vec::new();
    let result = server
        .client()
        .create_document_with_progress(&options, &mut |p| {
            events.push(p);
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        events.iter().map(|p| p.state).collect::<Vec<_>>(),
        [
            UpdateState::Staged,
            UpdateState::CommitUnknown,
            UpdateState::Committed,
            UpdateState::Verified
        ]
    );
    assert!(events[0].uploaded.is_empty());
    assert_eq!(result.uploaded, ["new.metadata", "new.content"]);
    assert_eq!(result.generation, 4);
    assert_eq!(server.count("PUT", "/sync/v3/root"), 1);
    let items = server
        .client()
        .list_items(ListOptions::default())
        .await
        .unwrap();
    assert_eq!(
        items.iter().map(|i| i.id.as_str()).collect::<Vec<_>>(),
        ["new", "old"]
    );
    let old = server.client().resolve("Old").await.unwrap();
    assert!(old.is_document());
    assert_eq!(old.id, "old");
    assert_eq!(
        server
            .client()
            .resolve("new")
            .await
            .unwrap()
            .metadata
            .visible_name,
        "New"
    );
}

#[tokio::test]
async fn update_replaces_and_adds_files_with_both_snapshot_guards() {
    let (server, state, doc_hash) = cloud();
    let root = state.lock().unwrap().root.clone();
    let options = UpdateDocumentOptions {
        id: "old".into(),
        expected_hash: doc_hash,
        expected_root: root,
        files: vec![
            FileUpdate {
                name: "old.metadata".into(),
                data: b"{}".to_vec(),
            },
            FileUpdate {
                name: "old/new.rm".into(),
                data: b"strokes".to_vec(),
            },
        ],
    };
    let result = server
        .client()
        .update_document_files_at_root(&options)
        .await
        .unwrap();
    assert_eq!(result.state, UpdateState::Verified);
    assert_eq!(result.uploaded.len(), 2);
    let conflict = server
        .client()
        .update_document_files_at_root(&options)
        .await
        .unwrap_err();
    assert_eq!(conflict.progress.state, UpdateState::Staged);
    assert_eq!(conflict.error.kind(), Some(ErrorKind::GenerationConflict));
    assert_eq!(server.count("PUT", "/sync/v3/root"), 1);
    assert!(
        server
            .client()
            .update_document_files("old", "stale", &options.files)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn root_conflict_unknown_and_failed_verification_retain_exact_progress() {
    for status in [409, 412, 500, 307] {
        let (server, state, _) = cloud();
        let options = new_document(state.lock().unwrap().root.clone());
        state.lock().unwrap().commit_status = status;
        let failure = server.client().create_document(&options).await.unwrap_err();
        assert_eq!(
            failure.progress.state,
            if status == 409 || status == 412 {
                UpdateState::Staged
            } else {
                UpdateState::CommitUnknown
            }
        );
        assert_eq!(failure.progress.uploaded.len(), 2);
        assert_eq!(server.count("PUT", "/sync/v3/root"), 1);
    }
    let (server, state, _) = cloud();
    let options = new_document(state.lock().unwrap().root.clone());
    state.lock().unwrap().corrupt_verification = true;
    assert_eq!(
        server
            .client()
            .create_document(&options)
            .await
            .unwrap_err()
            .progress
            .state,
        UpdateState::Committed
    );
}

#[tokio::test]
async fn progress_recording_failure_prevents_or_preserves_commit() {
    for phase in [
        UpdateState::Staged,
        UpdateState::CommitUnknown,
        UpdateState::Committed,
        UpdateState::Verified,
    ] {
        let (server, state, _) = cloud();
        let options = new_document(state.lock().unwrap().root.clone());
        let failure = server
            .client()
            .create_document_with_progress(&options, &mut |p| {
                if p.state == phase {
                    Err(Error::Message("recording unavailable".into()))
                } else {
                    Ok(())
                }
            })
            .await
            .unwrap_err();
        assert_eq!(
            failure.progress.state,
            if phase == UpdateState::CommitUnknown {
                UpdateState::Staged
            } else {
                phase
            }
        );
        assert_eq!(
            server.count("PUT", "/sync/v3/root"),
            usize::from(matches!(
                phase,
                UpdateState::Committed | UpdateState::Verified
            ))
        );
    }
}

#[tokio::test]
async fn creation_preflight_rejects_changed_generation_hash_existing_id_and_invalid_inputs() {
    let (server, state, _) = cloud();
    let root = state.lock().unwrap().root.clone();
    let mut options = new_document(root.clone());
    for different_hash in [false, true] {
        if different_hash {
            options.expected_root.hash = content_hash(b"different");
        } else {
            options.expected_root.generation += 1;
        }
        assert_eq!(
            server
                .client()
                .create_document(&options)
                .await
                .unwrap_err()
                .error
                .kind(),
            Some(ErrorKind::GenerationConflict)
        );
        options.expected_root = root.clone();
    }
    options.id = "old".into();
    options.files[0].name = "old.metadata".into();
    options.files[1].name = "old.content".into();
    assert!(
        server
            .client()
            .create_document(&options)
            .await
            .unwrap_err()
            .error
            .to_string()
            .contains("already exists")
    );
    for invalid in ["", "a/b", "a.b", "a:b", "a\\b"] {
        options.id = invalid.into();
        assert!(server.client().create_document(&options).await.is_err());
    }
    assert_eq!(
        server
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == "PUT")
            .count(),
        0
    );
}

#[tokio::test]
async fn flattened_listing_filters_trash_and_resolves_paths_and_ids() {
    let server = Http::new(|r| {
        if r.path == "/sync/v3/root" {
            return Reply::json(serde_json::json!({"hash":"root","generation":1}));
        }
        let hash = r.path.strip_prefix("/sync/v3/files/").unwrap();
        match hash {
            "root" => Reply::bytes(
                "4\n0:.:9:0\nfmeta:0:f.metadata:0:0\nfschema:0:f.docSchema:0:0\ndmeta:0:d.metadata:0:0\ndschema:0:d.docSchema:0:0\ntmeta:0:t.metadata:0:0\nfallback:0:bare.content:0:0\n",
            ),
            "fmeta" => {
                Reply::json(serde_json::json!({"visibleName":"Folder","type":"CollectionType"}))
            }
            "dmeta" => Reply::json(
                serde_json::json!({"visibleName":"Document","parent":"f","type":"DocumentType"}),
            ),
            "tmeta" => Reply::json(serde_json::json!({"visibleName":"Trash","parent":"trash"})),
            _ => Reply::status(404, "absent"),
        }
    });
    let client = server.client();
    let items = client.list_items(ListOptions::default()).await.unwrap();
    assert_eq!(items.len(), 3);
    let bare = items.iter().find(|i| i.id == "bare").unwrap();
    assert_eq!(bare.hash, "fallback");
    assert_eq!(bare.metadata.visible_name, "bare");
    assert!(bare.is_document());
    assert_eq!(
        client
            .list_items(ListOptions {
                include_deleted: true
            })
            .await
            .unwrap()
            .len(),
        4
    );
    assert_eq!(client.resolve_by_id("d").await.unwrap().id, "d");
    assert_eq!(client.resolve("Folder/Document").await.unwrap().id, "d");
    assert_eq!(client.resolve("Document").await.unwrap().id, "d");
    assert_eq!(client.resolve_by_path("Folder").await.unwrap().id, "f");
    assert_eq!(
        client.resolve("missing").await.unwrap_err().kind(),
        Some(ErrorKind::ItemNotFound)
    );
    assert!(client.resolve(" ").await.is_err());
    assert!(format!("{client:?}").contains("Client"));
    assert_eq!(client.endpoints().storage_host, server.url);
}

#[tokio::test]
async fn listing_collects_all_errors_and_malformed_metadata_is_rejected() {
    let server = Http::new(|r| match r.path.as_str() {
        "/sync/v3/root" => Reply::json(serde_json::json!({"hash":"root"})),
        "/sync/v3/files/root" => Reply::bytes("3\na:0:one:0:0\nb:0:two:0:0\n"),
        _ => Reply::status(500, "server failure"),
    });
    let error = server
        .client()
        .list_items(ListOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(&error, Error::Multiple(errors) if errors.len() == 2));
    assert!(error.to_string().contains("listing failures"));
    let server = Http::new(|r| {
        if r.path.ends_with("/root") {
            Reply::json(serde_json::json!({"hash":"index"}))
        } else if r.path.ends_with("/index") {
            Reply::bytes("3\nmeta:0:id.metadata:0:0\n")
        } else {
            Reply::bytes("{")
        }
    });
    assert!(matches!(
        server
            .client()
            .list_items(ListOptions::default())
            .await
            .unwrap_err(),
        Error::Json(_)
    ));
}

#[tokio::test]
async fn absent_sessions_are_renewed_once_across_concurrent_calls() {
    let server = Http::new(|r| {
        if r.path.ends_with("/user/new") {
            Reply::bytes("synthetic-session")
        } else {
            Reply::json(serde_json::json!({"hash":"root"}))
        }
    });
    let mut options = server.options();
    options.config.user_token.clear();
    let client = Client::new(options).unwrap();
    let (a, b) = tokio::join!(client.get_root_state(), client.get_root_state());
    assert!(a.is_ok() && b.is_ok());
    assert_eq!(server.count("POST", "/token/json/2/user/new"), 1);
    let client = Client::new(ClientOptions::default()).unwrap();
    assert_eq!(
        client.get_root_state().await.unwrap_err().kind(),
        Some(ErrorKind::Unauthorized)
    );
}

#[tokio::test]
async fn config_file_load_and_request_deadlines_are_observable() {
    let server = Http::new(|_| {
        thread::sleep(Duration::from_millis(300));
        Reply::bytes("{}")
    });
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config");
    let config = Config {
        device_token: "synthetic-file-device".into(),
        user_token: "synthetic-file-user".into(),
    };
    config.write_file(&path).unwrap();
    let mut options = server.options();
    options.config_path = Some(path);
    options.request_timeout = Some(Duration::from_millis(100));
    let client = Client::new(options).unwrap();
    assert_eq!(client.config().await, config);
    assert!(
        matches!(client.get_root_state().await.unwrap_err(),Error::Http(error) if error.is_timeout())
    );
    assert_eq!(server.count("GET", "/sync/v3/root"), 1);
}

#[tokio::test]
async fn update_failure_states_and_staging_mismatch_do_not_commit() {
    for (status, corrupt) in [(409, false), (500, false), (200, true)] {
        let (server, state, hash) = cloud();
        {
            let mut state = state.lock().unwrap();
            state.commit_status = status;
            state.corrupt_verification = corrupt;
        }
        let failure = server
            .client()
            .update_document_files(
                "old",
                &hash,
                &[FileUpdate {
                    name: "old.content".into(),
                    data: b"{}".to_vec(),
                }],
            )
            .await
            .unwrap_err();
        assert_eq!(
            failure.progress.state,
            if status == 409 {
                UpdateState::Staged
            } else if corrupt {
                UpdateState::Committed
            } else {
                UpdateState::CommitUnknown
            }
        );
    }
    let (server, _, _) = cloud();
    let failure = server
        .client()
        .update_document_files("", "", &[])
        .await
        .unwrap_err();
    assert!(failure.progress.uploaded.is_empty());
    assert!(failure.to_string().contains("required"));
}

#[tokio::test]
async fn lost_root_commit_response_is_single_attempt_and_unknown() {
    let (server, state, _) = cloud();
    let options = new_document(state.lock().unwrap().root.clone());
    state.lock().unwrap().commit_status = 0;
    let mut client_options = server.options();
    client_options.request_timeout = Some(Duration::from_millis(100));
    let failure = Client::new(client_options)
        .unwrap()
        .create_document(&options)
        .await
        .unwrap_err();
    assert_eq!(failure.progress.state, UpdateState::CommitUnknown);
    assert!(matches!(failure.error, Error::Http(_)));
    assert_eq!(server.count("PUT", "/sync/v3/root"), 1);
}

#[tokio::test]
async fn dropping_read_future_cancels_waiting_without_replaying() {
    let (sent, received) = tokio::sync::oneshot::channel();
    let sent = Mutex::new(Some(sent));
    let server = Http::new(move |_| {
        sent.lock().unwrap().take().unwrap().send(()).unwrap();
        thread::sleep(Duration::from_millis(80));
        Reply::bytes("{}")
    });
    let client = server.client();
    let task = tokio::spawn(async move { client.get_root_state().await });
    received.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(server.count("GET", "/sync/v3/root"), 1);
}
