// Compiled as transport unit tests to exercise private physical-request policy
// with real sockets, including failures that tiny_http intentionally repairs.
use crate::{Client, ClientOptions, Config, Endpoints, Error, TransportPolicy};
use std::{
    io::{Read, Write},
    net::TcpListener,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tracing::instrument::WithSubscriber;

type RecordedRequests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
struct RawServer {
    url: String,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    seen: RecordedRequests,
}
impl RawServer {
    fn new(handler: impl Fn(usize, &mut std::net::TcpStream) + Send + Sync + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let (count, stopping, handler) = (requests.clone(), stop.clone(), Arc::new(handler));
        let thread = thread::spawn(move || {
            let mut children = Vec::new();
            while !stopping.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let handler = handler.clone();
                        let count = count.clone();
                        let recorded = recorded.clone();
                        children.push(thread::spawn(move || {
                            stream
                                .set_read_timeout(Some(Duration::from_secs(2)))
                                .unwrap();
                            let mut headers = Vec::new();
                            let mut byte = [0];
                            while !headers.ends_with(b"\r\n\r\n") {
                                if stream.read(&mut byte).unwrap_or(0) == 0 {
                                    return;
                                }
                                headers.push(byte[0]);
                                assert!(headers.len() < 65536);
                            }
                            let number = count.fetch_add(1, Ordering::SeqCst);
                            let headers = String::from_utf8(headers).unwrap();
                            let size = headers
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            let mut body = vec![0; size];
                            stream.read_exact(&mut body).unwrap();
                            recorded.lock().unwrap().push((headers, body));
                            handler(number, &mut stream);
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(1))
                    }
                    Err(error) => panic!("accept: {error}"),
                }
            }
            for child in children {
                child.join().unwrap();
            }
        });
        Self {
            url,
            requests,
            stop,
            thread: Some(thread),
            seen,
        }
    }
    fn count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
    fn options(&self) -> ClientOptions {
        ClientOptions {
            config: Config {
                user_token: "synthetic-user".into(),
                device_token: "synthetic-device".into(),
            },
            endpoints: Endpoints {
                storage_host: self.url.clone(),
                ..Endpoints::default()
            },
            auth_base_url: self.url.clone(),
            auto_renew: false,
            transport_policy: TransportPolicy {
                retry_delay: Duration::from_millis(1),
                attempt_timeout: Duration::from_secs(2),
            },
            ..ClientOptions::default()
        }
    }
    fn client(&self) -> Client {
        Client::new(self.options()).unwrap()
    }
}
impl Drop for RawServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.thread.take().unwrap().join().unwrap();
    }
}
fn response(stream: &mut std::net::TcpStream, status: u16, headers: &str, body: &[u8]) {
    let _ = write!(
        stream,
        "HTTP/1.1 {status} reply\r\nConnection: close\r\nContent-Length: {}\r\n{headers}\r\n",
        body.len()
    );
    let _ = stream.write_all(body);
}

#[tokio::test]
async fn redirect_policy_preserves_read_and_blob_methods_bodies_and_headers() {
    for method in [
        reqwest::Method::GET,
        reqwest::Method::HEAD,
        reqwest::Method::PUT,
    ] {
        for status in [301, 302, 303, 307, 308] {
            let target = RawServer::new(|_, stream| response(stream, 200, "", b""));
            let location = format!("Location: {}/final\r\n", target.url);
            let origin =
                RawServer::new(move |_, stream| response(stream, status, &location, b"redirect"));
            let client = origin.client();
            let result = super::execute(
                client
                    .http_client()
                    .request(
                        method.clone(),
                        format!("{}/sync/v3/files/{}", origin.url, "a".repeat(64)),
                    )
                    .header("rm-filename", "page.rm")
                    .header("authorization", "Bearer synthetic")
                    .header("content-type", "application/octet-stream")
                    .header("content-language", "en")
                    .header("content-location", "/old")
                    .body(b"exact\0bytes".to_vec()),
                origin.options().transport_policy,
            )
            .await
            .unwrap();
            assert_eq!(result.status, 200, "{method} {status}");
            let seen = target.seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            let drop_body = [301, 302, 303].contains(&status);
            let expected_method = if drop_body && method == reqwest::Method::PUT {
                "GET"
            } else {
                method.as_str()
            };
            assert!(
                seen[0]
                    .0
                    .starts_with(&format!("{expected_method} /final HTTP/1.1")),
                "{}",
                seen[0].0
            );
            assert_eq!(
                seen[0].1,
                if drop_body {
                    Vec::new()
                } else {
                    b"exact\0bytes".to_vec()
                }
            );
            assert!(seen[0].0.contains("rm-filename: page.rm"));
            assert!(seen[0].0.contains("authorization: Bearer synthetic"));
            assert_eq!(seen[0].0.contains("content-type:"), !drop_body);
            assert_eq!(seen[0].0.contains("content-language:"), !drop_body);
            assert_eq!(seen[0].0.contains("content-location:"), !drop_body);
            assert!(
                seen[0]
                    .0
                    .contains(&format!("referer: {}/sync/v3/files/", origin.url))
            );
        }
    }
}

#[tokio::test]
async fn root_and_auth_redirects_return_original_response_without_replay() {
    for status in [301, 302, 303, 307, 308] {
        let target = RawServer::new(|_, stream| response(stream, 200, "", b"unexpected"));
        let location = format!("Location: {}/final\r\n", target.url);
        let origin =
            RawServer::new(move |_, stream| response(stream, status, &location, b"original"));
        for (method, path) in [
            (reqwest::Method::PUT, "/sync/v3/root"),
            (reqwest::Method::POST, "/token/json/2/device/new"),
        ] {
            let result = super::execute(
                origin
                    .client()
                    .http_client()
                    .request(method, format!("{}{path}", origin.url))
                    .body("mutation"),
                origin.options().transport_policy,
            )
            .await
            .unwrap();
            assert_eq!(result.status, status);
            assert_eq!(result.body, b"original");
        }
        assert_eq!(origin.count(), 2);
        assert_eq!(target.count(), 0);
    }
}

#[tokio::test]
async fn redirected_hops_have_independent_deadlines_recovery_and_traces() {
    let target = RawServer::new(|n, stream| {
        thread::sleep(Duration::from_millis(80));
        response(
            stream,
            if n < 2 { 503 } else { 200 },
            "Retry-After: 0\r\n",
            b"ok",
        );
    });
    let location = format!(
        "Location: {}/sync/v3/files/{}\r\n",
        target.url,
        "b".repeat(64)
    );
    let origin = RawServer::new(move |_, stream| {
        thread::sleep(Duration::from_millis(80));
        response(stream, 307, &location, b"redirect");
    });
    let log = Log(Arc::new(Mutex::new(Vec::new())));
    let mut policy = origin.options().transport_policy;
    policy.attempt_timeout = Duration::from_millis(150);
    let result = super::execute(
        origin
            .client()
            .http_client()
            .put(format!("{}/sync/v3/files/{}", origin.url, "a".repeat(64)))
            .header("rm-filename", "name")
            .body("immutable"),
        policy,
    )
    .with_subscriber(subscriber(log.clone()))
    .await
    .unwrap();
    assert_eq!(result.status, 200);
    assert_eq!(origin.count(), 1);
    assert_eq!(target.count(), 3);
    let seen = target.seen.lock().unwrap();
    assert!(seen.windows(2).all(|pair| pair[0] == pair[1]));
    let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.matches("cloud HTTP attempt").count(), 4);
    assert_eq!(output.matches("status=307").count(), 1);
}

#[tokio::test]
async fn redirect_limit_stops_after_ten_requests() {
    let origin =
        RawServer::new(|_, stream| response(stream, 302, "Location: /again\r\n", b"again"));
    let result = super::execute(
        origin
            .client()
            .http_client()
            .get(format!("{}/initial", origin.url)),
        origin.options().transport_policy,
    )
    .await;
    assert!(matches!(result, Err(Error::RedirectLimit)));
    assert_eq!(origin.count(), 10);
}

#[tokio::test]
async fn canonical_hostname_and_port_changes_preserve_credentials() {
    let end = RawServer::new(|_, stream| response(stream, 200, "", b"ok"));
    let location = format!(
        "Location: {}/final\r\n",
        end.url.replace("127.0.0.1", "LOCALHOST")
    );
    let origin = RawServer::new(move |_, stream| response(stream, 307, &location, b"redirect"));
    let client = origin.client();
    let result = super::execute(
        client
            .http_client()
            .get(format!(
                "{}/initial",
                origin.url.replace("127.0.0.1", "localhost")
            ))
            .header("authorization", "synthetic-same-host"),
        origin.options().transport_policy,
    )
    .await
    .unwrap();
    assert_eq!(result.status, 200);
    assert!(
        end.seen.lock().unwrap()[0]
            .0
            .contains("authorization: synthetic-same-host")
    );
}

#[tokio::test]
async fn native_middleware_filters_downgrade_referrers_and_permits_subdomain_credentials() {
    use tower::ServiceExt;
    // Exercise the pure policy through the real middleware. This isolates
    // cross-scheme/domain header rules without requiring TLS or public DNS.
    for (location, retain_credentials, retain_referrer) in [
        ("https://child.example.test/final", true, true),
        ("https://badexample.test/final", false, true),
        ("http://example.test/final", true, false),
    ] {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let service = tower::service_fn(move |request: http::Request<reqwest::Body>| {
            let recorded = recorded.clone();
            async move {
                let mut response = http::Response::new(());
                if request.uri().path() == "/initial" {
                    *response.status_mut() = http::StatusCode::FOUND;
                    response
                        .headers_mut()
                        .insert("location", location.parse().unwrap());
                } else {
                    recorded.lock().unwrap().push(request.headers().clone());
                }
                Ok::<_, Error>(response)
            }
        });
        let request = reqwest::Client::new()
            .get("https://example.test/initial")
            .header("authorization", "synthetic")
            .header("referer", "https://example.test/explicit")
            .build()
            .unwrap();
        let policy = super::CloudRedirect::new(&request);
        tower_http::follow_redirect::FollowRedirect::with_policy(service, policy)
            .oneshot(http::Request::try_from(request).unwrap())
            .await
            .unwrap();
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].contains_key("authorization"), retain_credentials);
        assert_eq!(seen[0].contains_key("referer"), retain_referrer);
    }
}

#[tokio::test]
async fn redirect_drops_slow_body_and_method_change_survives_later_307() {
    let end = RawServer::new(|_, stream| response(stream, 200, "", b"ok"));
    let end_location = format!("Location: {}/final\r\n", end.url);
    let middle = RawServer::new(move |_, stream| response(stream, 307, &end_location, b"again"));
    let location = format!("Location: {}/middle\r\n", middle.url);
    let origin = RawServer::new(move |_, stream| {
        let _ = write!(
            stream,
            "HTTP/1.1 302 Found\r\n{location}Content-Length: 100\r\nConnection: close\r\n\r\n"
        );
        thread::sleep(Duration::from_millis(300));
    });
    let client = origin.client();
    let result = tokio::time::timeout(
        Duration::from_millis(200),
        super::execute(
            client
                .http_client()
                .put(format!("{}/sync/v3/files/{}", origin.url, "a".repeat(64)))
                .body("upload"),
            origin.options().transport_policy,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.status, 200);
    for server in [&middle, &end] {
        let seen = server.seen.lock().unwrap();
        assert!(seen[0].0.starts_with("GET "));
        assert!(seen[0].1.is_empty());
    }
}

#[tokio::test]
async fn explicit_referer_is_retained_and_missing_location_returns_original_body() {
    let end = RawServer::new(|_, stream| response(stream, 200, "", b"ok"));
    let location = format!("Location: {}/final\r\n", end.url);
    let origin = RawServer::new(move |n, stream| {
        response(
            stream,
            301,
            if n == 0 { &location } else { "" },
            b"original",
        )
    });
    let client = origin.client();
    let result = super::execute(
        client
            .http_client()
            .get(format!("{}/initial", origin.url))
            .header("referer", "https://example.test/explicit"),
        origin.options().transport_policy,
    )
    .await
    .unwrap();
    assert_eq!(result.status, 200);
    assert!(
        end.seen.lock().unwrap()[0]
            .0
            .contains("referer: https://example.test/explicit")
    );
    let result = super::execute(
        client.http_client().get(format!("{}/again", origin.url)),
        origin.options().transport_policy,
    )
    .await
    .unwrap();
    assert_eq!(result.status, 301);
    assert_eq!(result.body, b"original");
}

#[tokio::test]
async fn credentials_are_removed_cumulatively_across_unrelated_hosts() {
    let end = RawServer::new(|_, stream| response(stream, 200, "", b"ok"));
    let final_location = format!("Location: {}/final\r\n", end.url);
    let foreign =
        RawServer::new(move |_, stream| response(stream, 307, &final_location, b"redirect"));
    let foreign_location = format!(
        "Location: {}/foreign\r\n",
        foreign.url.replace("127.0.0.1", "localhost")
    );
    let origin =
        RawServer::new(move |_, stream| response(stream, 307, &foreign_location, b"redirect"));
    let sensitive = [
        "authorization",
        "www-authenticate",
        "cookie",
        "cookie2",
        "proxy-authorization",
        "proxy-authenticate",
    ];
    let client = origin.client();
    let mut request = client
        .http_client()
        .get(format!("{}/initial", origin.url))
        .header("x-safe", "kept");
    for header in sensitive {
        request = request.header(header, "synthetic-sensitive");
    }
    assert_eq!(
        super::execute(request, origin.options().transport_policy)
            .await
            .unwrap()
            .status,
        200
    );
    for server in [&foreign, &end] {
        let seen = server.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(!seen[0].0.contains("synthetic-sensitive"), "{}", seen[0].0);
        assert!(seen[0].0.contains("x-safe: kept"));
    }
}

#[tokio::test]
async fn cloud_credentials_cannot_reappear_at_redirect_target() {
    let target = RawServer::new(|_, stream| response(stream, 200, "", b"ok"));
    let location = format!(
        "Location: {}/final\r\n",
        target.url.replace("127.0.0.1", "localhost")
    );
    let origin = RawServer::new(move |_, stream| response(stream, 307, &location, b"redirect"));
    let mut options = origin.options();
    options.config.user_token = "synthetic-owned-secret".into();
    let result = Client::new(options)
        .unwrap()
        .get_blob_fresh(&"a".repeat(64), "name")
        .await
        .unwrap();
    assert_eq!(result, b"ok");
    assert!(
        !target.seen.lock().unwrap()[0]
            .0
            .contains("synthetic-owned-secret")
    );
}

#[tokio::test]
async fn recoverable_status_drops_slow_rejection_body_before_retrying() {
    let server = RawServer::new(|n, stream| {
        if n == 0 {
            let _ = stream.write_all(b"HTTP/1.1 503 Unavailable\r\nRetry-After: 0\r\nContent-Length: 100\r\nConnection: close\r\n\r\n");
            thread::sleep(Duration::from_millis(400));
        } else {
            response(stream, 200, "", b"ok");
        }
    });
    let client = server.client();
    let bytes = tokio::time::timeout(
        Duration::from_millis(200),
        client.get_blob_fresh(&"a".repeat(64), "name"),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(bytes, b"ok");
    assert_eq!(server.count(), 2);
}

#[tokio::test]
async fn complete_immutable_body_is_retried_after_truncation_and_lost_headers() {
    let server = RawServer::new(|n, stream| match n {
        0 => {
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial",
            );
        }
        1 => {}
        _ => response(stream, 200, "", b"complete"),
    });
    assert_eq!(
        server
            .client()
            .get_blob_fresh(&"a".repeat(64), "page.rm")
            .await
            .unwrap(),
        b"complete"
    );
    assert_eq!(server.count(), 3);
}

#[tokio::test]
async fn exhausted_truncated_success_is_an_error_and_never_cached() {
    let server = RawServer::new(|_, stream| {
        let _ = stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial",
        );
    });
    let dir = tempfile::tempdir().unwrap();
    let mut options = server.options();
    options.cache_dir = Some(dir.path().into());
    let hash = "b".repeat(64);
    assert!(matches!(
        Client::new(options)
            .unwrap()
            .get_blob(&hash, "page.rm")
            .await
            .unwrap_err(),
        Error::Http(_)
    ));
    assert_eq!(server.count(), 3);
    assert!(!dir.path().join("blobs").join(hash).exists());
}

#[tokio::test]
async fn deadlines_include_response_body_and_restart_for_each_attempt() {
    let server = RawServer::new(|n, stream| {
        if n < 2 {
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nx");
            thread::sleep(Duration::from_millis(150));
            let _ = stream.write_all(b"y");
        } else {
            response(stream, 200, "", b"ok");
        }
    });
    let mut options = server.options();
    options.transport_policy.attempt_timeout = Duration::from_millis(45);
    let started = Instant::now();
    assert_eq!(
        Client::new(options)
            .unwrap()
            .get_blob_fresh(&"c".repeat(64), "name")
            .await
            .unwrap(),
        b"ok"
    );
    assert!(started.elapsed() >= Duration::from_millis(90));
    assert_eq!(server.count(), 3);
}

#[tokio::test]
async fn mutable_authentication_discovery_and_invalid_hashes_never_retry() {
    let server =
        RawServer::new(|_, stream| response(stream, 503, "Retry-After: 0\r\n", b"unavailable"));
    let client = server.client();
    assert!(client.get_root_state().await.is_err());
    assert!(client.get_blob_fresh("short", "name").await.is_err());
    assert!(
        client
            .get_blob_fresh(&"g".repeat(64), "name")
            .await
            .is_err()
    );
    assert!(client.pair_device("12345678").await.is_err());
    assert!(client.renew_token().await.is_err());
    assert!(
        client
            .discover_endpoints(Some(&server.url))
            .await
            .error
            .is_some()
    );
    let result = super::execute(
        client
            .http_client()
            .put(format!("{}/sync/v3/root", server.url))
            .body("root"),
        TransportPolicy::default(),
    )
    .await
    .unwrap();
    assert_eq!(result.status, 503);
    assert_eq!(server.count(), 7);
}

#[tokio::test]
async fn exact_transient_statuses_retry_three_times_and_other_rejections_do_not() {
    for status in [408, 429, 500, 502, 503, 504, 400, 401, 403, 404, 501, 505] {
        let server = RawServer::new(move |_, stream| {
            response(
                stream,
                status,
                "Retry-After: 0\r\nLocation: http://127.0.0.1:1/redirected\r\n",
                b"rejected",
            )
        });
        let error = server
            .client()
            .get_blob_fresh(&"d".repeat(64), "name")
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Status(ref e) if e.status_code == status));
        assert_eq!(
            server.count(),
            if [408, 429, 500, 502, 503, 504].contains(&status) {
                3
            } else {
                1
            },
            "status {status}"
        );
    }
}

#[tokio::test]
async fn malformed_protocol_and_builder_errors_are_not_retried() {
    let server = RawServer::new(|_, stream| {
        let _ = stream.write_all(b"not HTTP\r\n\r\n");
    });
    assert!(matches!(
        server
            .client()
            .get_blob_fresh(&"e".repeat(64), "name")
            .await
            .unwrap_err(),
        Error::Http(_)
    ));
    assert_eq!(server.count(), 1);
    let http = reqwest::Client::new();
    assert!(
        super::execute(http.get("not a URL"), TransportPolicy::default())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn deadline_failures_of_root_and_auth_are_single_attempts() {
    let server = RawServer::new(|_, stream| {
        thread::sleep(Duration::from_millis(120));
        response(stream, 200, "", b"late");
    });
    let mut options = server.options();
    options.transport_policy.attempt_timeout = Duration::from_millis(30);
    let client = Client::new(options).unwrap();
    assert!(matches!(
        client.get_root_state().await.unwrap_err(),
        Error::AttemptTimeout(_)
    ));
    assert!(matches!(
        client.renew_token().await.unwrap_err(),
        Error::AttemptTimeout(_)
    ));
    assert_eq!(server.count(), 2);
}

#[derive(Clone)]
struct Log(Arc<Mutex<Vec<u8>>>);
impl Write for Log {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn subscriber(log: Log) -> impl tracing::Subscriber + Send + Sync {
    // Keep a stable default alive while tests construct thread-scoped writers.
    // tracing-core's single-dispatch registration optimization otherwise lets
    // an uninstrumented concurrent thread cache this shared callsite as disabled.
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        tracing::subscriber::set_global_default(
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(std::io::sink)
                .finish(),
        )
        .unwrap();
    });
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .without_time()
        .with_ansi(false)
        .with_writer(move || log.clone())
        .finish()
}

#[tokio::test]
async fn trace_records_each_attempt_without_headers_or_credentials() {
    let server = RawServer::new(|n, stream| {
        if n < 2 {
            response(stream, 500, "Retry-After: 0\r\n", b"synthetic-secret")
        } else {
            response(stream, 200, "", b"ok")
        }
    });
    let log = Log(Arc::new(Mutex::new(Vec::new())));
    server
        .client()
        .get_blob_fresh(&"f".repeat(64), "document.rm")
        .with_subscriber(subscriber(log.clone()))
        .await
        .unwrap();
    let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    let lines = output
        .lines()
        .filter(|line| line.contains("cloud HTTP attempt"))
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 3, "{output}");
    assert!(lines[0].contains("status=500") && lines[2].contains("status=200"));
    assert!(lines[0].contains("document.rm"));
    assert!(!output.contains("synthetic-user") && !output.contains("synthetic-secret"));
}

#[tokio::test]
async fn cancellation_during_request_and_retry_after_prevents_further_attempts() {
    for in_backoff in [false, true] {
        let server = RawServer::new(move |_, stream| {
            if in_backoff {
                response(stream, 503, "Retry-After: 20\r\n", b"retry")
            } else {
                thread::sleep(Duration::from_millis(160));
                response(stream, 200, "", b"late")
            }
        });
        let client = server.client();
        let log = Log(Arc::new(Mutex::new(Vec::new())));
        let task = tokio::spawn(
            async move { client.get_blob_fresh(&"a".repeat(64), "name").await }
                .with_subscriber(subscriber(log.clone())),
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.count() == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(server.count(), 1);
        let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
        assert_eq!(output.matches("cloud HTTP attempt").count(), 1);
        if !in_backoff {
            assert!(output.contains("cancelled"));
        }
    }
}

#[tokio::test]
async fn retry_after_seconds_and_dates_override_fallback_and_invalid_values_use_backoff() {
    for (header, delay) in [
        ("1".to_string(), Duration::from_millis(5)),
        (
            httpdate::fmt_http_date(std::time::SystemTime::UNIX_EPOCH),
            Duration::from_secs(10),
        ),
        ("invalid".to_string(), Duration::from_millis(30)),
        ("4294967296".to_string(), Duration::from_millis(30)),
        ("+1".to_string(), Duration::from_millis(30)),
    ] {
        let expected_zero = header.contains("1970");
        let expected_second = header == "1";
        let server = RawServer::new(move |n, stream| {
            if n == 0 {
                response(stream, 429, &format!("Retry-After: {header}\r\n"), b"retry")
            } else {
                response(stream, 200, "", b"ok")
            }
        });
        let mut options = server.options();
        options.transport_policy.retry_delay = delay;
        let started = Instant::now();
        assert_eq!(
            Client::new(options)
                .unwrap()
                .get_blob_fresh(&"a".repeat(64), "name")
                .await
                .unwrap(),
            b"ok"
        );
        if expected_second {
            assert!(started.elapsed() >= Duration::from_secs(1));
        } else if expected_zero {
            assert!(started.elapsed() < Duration::from_secs(2));
        } else {
            assert!(started.elapsed() >= delay);
        }
        assert_eq!(server.count(), 2);
    }
}

#[tokio::test]
async fn future_retry_after_date_and_exponential_fallback_are_observed() {
    let date = httpdate::fmt_http_date(std::time::SystemTime::now() + Duration::from_secs(3));
    let server = RawServer::new(move |n, stream| {
        if n == 0 {
            response(stream, 503, &format!("Retry-After: {date}\r\n"), b"retry")
        } else {
            response(stream, 200, "", b"ok")
        }
    });
    let start = Instant::now();
    server
        .client()
        .get_blob_fresh(&"a".repeat(64), "name")
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(server.count(), 2);
    let server = RawServer::new(|n, stream| {
        if n < 2 {
            response(stream, 503, "", b"retry")
        } else {
            response(stream, 200, "", b"ok")
        }
    });
    let mut options = server.options();
    options.transport_policy.retry_delay = Duration::from_millis(25);
    let start = Instant::now();
    Client::new(options)
        .unwrap()
        .get_blob_fresh(&"a".repeat(64), "name")
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(75));
    assert_eq!(server.count(), 3);
}

#[tokio::test]
async fn connection_refusal_is_retried_but_traced_without_error_url_secrets() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let client = Client::new(ClientOptions {
        config: Config {
            user_token: "synthetic-user".into(),
            ..Config::default()
        },
        endpoints: Endpoints {
            storage_host: url,
            ..Endpoints::default()
        },
        transport_policy: TransportPolicy {
            retry_delay: Duration::ZERO,
            ..TransportPolicy::default()
        },
        ..ClientOptions::default()
    })
    .unwrap();
    let log = Log(Arc::new(Mutex::new(Vec::new())));
    assert!(matches!(
        client
            .get_blob_fresh(&"a".repeat(64), "name")
            .with_subscriber(subscriber(log.clone()))
            .await
            .unwrap_err(),
        Error::Http(_)
    ));
    let output = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
    assert_eq!(output.matches("cloud HTTP attempt").count(), 3);
    assert_eq!(output.matches("transport-error").count(), 3);
    assert!(!output.contains("synthetic-user"));
}
