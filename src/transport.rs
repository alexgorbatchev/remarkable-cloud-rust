use crate::{Error, Result, client::Response};
use reqwest::{Method, Request};
use std::{
    error::Error as _,
    io,
    time::{Duration, Instant, SystemTime},
};
use tower::{ServiceExt, service_fn};
use tower_http::follow_redirect::{
    FollowRedirect,
    policy::{Action, Attempt, FilterCredentials, Policy},
};

/// Bounds each physical request, including its complete response body.
#[derive(Clone, Copy, Debug)]
pub struct TransportPolicy {
    pub attempt_timeout: Duration,
    /// Exponential fallback delay; a valid Retry-After takes precedence.
    pub retry_delay: Duration,
}
impl Default for TransportPolicy {
    fn default() -> Self {
        Self {
            attempt_timeout: Duration::from_secs(90),
            retry_delay: Duration::from_millis(250),
        }
    }
}

fn replayable(request: &Request) -> bool {
    let Some(hash) = request.url().path().strip_prefix("/sync/v3/files/") else {
        return false;
    };
    hash.len() == 64
        && hash.bytes().all(|b| b.is_ascii_hexdigit())
        && (request.method() == Method::GET || request.method() == Method::PUT)
        && request.try_clone().is_some()
}

fn transient_status(status: u16) -> bool {
    matches!(status, 408 | 429 | 500 | 502 | 503 | 504)
}

fn transient_error(error: &reqwest::Error) -> bool {
    if error.is_timeout() {
        return true;
    }
    let mut cause = error.source();
    while let Some(error) = cause {
        if let Some(error) = error.downcast_ref::<io::Error>()
            && matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionRefused
                    | io::ErrorKind::BrokenPipe
                    | io::ErrorKind::TimedOut
            )
        {
            return true;
        }
        if let Some(error) = error.downcast_ref::<hyper::Error>()
            && (error.is_incomplete_message() || error.is_closed() || error.is_timeout())
        {
            return true;
        }
        cause = error.source();
    }
    false
}

fn retry_after(value: Option<&str>, fallback: Duration) -> Duration {
    let Some(value) = value else {
        return fallback;
    };
    if !value.is_empty()
        && value.bytes().all(|b| b.is_ascii_digit())
        && let Ok(seconds) = value.parse::<u32>()
    {
        return Duration::from_secs(seconds.into());
    }
    httpdate::parse_http_date(value)
        .map(|time| time.duration_since(SystemTime::now()).unwrap_or_default())
        .unwrap_or(fallback)
}

struct AttemptTrace {
    dispatcher: tracing::Dispatch,
    method: Method,
    path: String,
    filename: String,
    number: u32,
    started: Instant,
    status: Option<u16>,
    outcome: &'static str,
}
impl AttemptTrace {
    fn new(request: &Request, number: u32) -> Self {
        Self {
            dispatcher: tracing::dispatcher::get_default(Clone::clone),
            method: request.method().clone(),
            path: request.url().path().into(),
            filename: request
                .headers()
                .get("rm-filename")
                .and_then(|h| h.to_str().ok())
                .unwrap_or_default()
                .into(),
            number,
            started: Instant::now(),
            status: None,
            outcome: "cancelled",
        }
    }
}
impl Drop for AttemptTrace {
    fn drop(&mut self) {
        tracing::dispatcher::with_default(&self.dispatcher, || {
            tracing::debug!(target: "remarkable_cloud::http", method = %self.method, path = %self.path,
            filename = %self.filename, attempt = self.number, status = self.status,
            elapsed_ms = self.started.elapsed().as_millis() as u64, outcome = self.outcome,
            "cloud HTTP attempt")
        });
    }
}

pub(crate) async fn execute(
    builder: reqwest::RequestBuilder,
    policy: TransportPolicy,
) -> Result<Response> {
    let (http, request) = builder.build_split();
    let request = request?;
    let redirect_policy = CloudRedirect::new(&request);
    let follow = redirect_policy.allowed;
    let service = service_fn(move |request: http::Request<reqwest::Body>| {
        let http = http.clone();
        async move {
            let request = Request::try_from(request)?;
            let response = execute_hop(&http, request, policy, follow).await?;
            let status =
                http::StatusCode::from_u16(response.status).expect("reqwest supplied valid status");
            let headers = response.headers.clone();
            let mut wrapped = http::Response::new(response);
            *wrapped.status_mut() = status;
            *wrapped.headers_mut() = headers;
            Ok::<_, Error>(wrapped)
        }
    });
    let request = http::Request::try_from(request)?;
    Ok(FollowRedirect::with_policy(service, redirect_policy)
        .oneshot(request)
        .await?
        .into_body())
}

#[derive(Clone)]
struct CloudRedirect {
    allowed: bool,
    initial_host: String,
    explicit_referer: Option<http::HeaderValue>,
    hops: usize,
    strip_credentials: bool,
    credentials: FilterCredentials,
    drop_body: bool,
    force_get: bool,
    referer: Option<http::HeaderValue>,
}
impl CloudRedirect {
    fn new(request: &Request) -> Self {
        Self {
            allowed: request.method() == Method::GET
                || request.method() == Method::HEAD
                || replayable(request),
            initial_host: request.url().host_str().unwrap_or_default().to_owned(),
            explicit_referer: request.headers().get(http::header::REFERER).cloned(),
            hops: 0,
            strip_credentials: false,
            credentials: FilterCredentials::new().block_none(),
            drop_body: false,
            force_get: false,
            referer: None,
        }
    }
}
impl Policy<reqwest::Body, Error> for CloudRedirect {
    fn redirect(&mut self, attempt: &Attempt<'_>) -> Result<Action> {
        if !self.allowed {
            return Ok(Action::Stop);
        }
        self.hops += 1;
        if self.hops >= 10 {
            return Err(Error::RedirectLimit);
        }
        let location = reqwest::Url::parse(&attempt.location().to_string()).ok();
        let host = location
            .as_ref()
            .and_then(reqwest::Url::host_str)
            .unwrap_or_default();
        let same_domain = host == self.initial_host
            || (!host.contains([':', '%'])
                && host
                    .strip_suffix(&self.initial_host)
                    .is_some_and(|prefix| prefix.ends_with('.')));
        self.strip_credentials |= !same_domain;
        self.credentials = if self.strip_credentials {
            FilterCredentials::new().block_any()
        } else {
            FilterCredentials::new().block_none()
        };
        Policy::<reqwest::Body, Error>::redirect(&mut self.credentials, attempt)?;
        if matches!(attempt.status().as_u16(), 301..=303) {
            self.drop_body = true;
            self.force_get |= attempt.previous_method() != Method::GET
                && attempt.previous_method() != Method::HEAD;
        }
        let downgrade = attempt.previous().scheme_str() == Some("https")
            && attempt.location().scheme_str() == Some("http");
        self.referer = if downgrade {
            None
        } else {
            self.explicit_referer.clone().or_else(|| {
                let mut url = reqwest::Url::parse(&attempt.previous().to_string()).ok()?;
                let _ = url.set_username("");
                let _ = url.set_password(None);
                http::HeaderValue::from_str(url.as_str()).ok()
            })
        };
        Ok(Action::Follow)
    }
    fn on_request(&mut self, request: &mut http::Request<reqwest::Body>) {
        if self.hops == 0 {
            return;
        }
        Policy::<reqwest::Body, Error>::on_request(&mut self.credentials, request);
        if self.strip_credentials {
            // Go's sensitive set additionally includes these challenge and
            // legacy-cookie headers beyond tower-http's native blocklist.
            for header in ["www-authenticate", "cookie2", "proxy-authenticate"] {
                request.headers_mut().remove(header);
            }
        }
        if self.force_get {
            *request.method_mut() = Method::GET;
        }
        if self.drop_body {
            *request.body_mut() = reqwest::Body::default();
            for header in [
                "content-length",
                "transfer-encoding",
                "content-type",
                "content-encoding",
                "content-language",
                "content-location",
            ] {
                request.headers_mut().remove(header);
            }
        }
        request.headers_mut().remove(http::header::HOST);
        request.headers_mut().remove(http::header::REFERER);
        if let Some(referer) = &self.referer {
            request
                .headers_mut()
                .insert(http::header::REFERER, referer.clone());
        }
    }
    fn clone_body(&self, body: &reqwest::Body) -> Option<reqwest::Body> {
        body.as_bytes().map(|bytes| bytes.to_vec().into())
    }
}

async fn execute_hop(
    http: &reqwest::Client,
    request: Request,
    policy: TransportPolicy,
    follow: bool,
) -> Result<Response> {
    let attempts = if replayable(&request) { 3 } else { 1 };
    let mut original = Some(request);
    let template = original.as_ref().and_then(Request::try_clone);
    for number in 1..=attempts {
        let request = original
            .take()
            .or_else(|| template.as_ref().and_then(Request::try_clone))
            .ok_or_else(|| Error::message("immutable request body cannot be replayed"))?;
        let mut trace = AttemptTrace::new(&request, number);
        let result = tokio::time::timeout(policy.attempt_timeout, async {
            let response = http.execute(request).await?;
            trace.status = Some(response.status().as_u16());
            if (number < attempts && transient_status(response.status().as_u16()))
                || (follow
                    && matches!(response.status().as_u16(), 301..=303 | 307 | 308)
                    && response
                        .headers()
                        .get(http::header::LOCATION)
                        .and_then(|h| h.to_str().ok())
                        .is_some_and(|location| response.url().join(location).is_ok()))
            {
                // Recovery needs headers, not the rejected body's potentially
                // stalled stream. Drop it before backing off, as the Go policy does.
                return Ok(Response::headers_only(&response));
            }
            Ok::<_, Error>(Response::read(response).await)
        })
        .await
        .unwrap_or_else(|_| Err(Error::AttemptTimeout(policy.attempt_timeout)));
        trace.outcome = match &result {
            Ok(response) if response.body_error().is_some() => "body-error",
            Ok(_) => "response",
            Err(Error::AttemptTimeout(_)) => "timeout",
            Err(_) => "transport-error",
        };
        drop(trace);
        let recover = match &result {
            Ok(response) if transient_status(response.status) => true,
            Ok(response) if (200..300).contains(&response.status) => {
                response.body_error().is_some_and(transient_error)
            }
            Err(Error::Http(error)) => transient_error(error),
            Err(Error::AttemptTimeout(_)) => true,
            _ => false,
        };
        if number == attempts || !recover {
            return result;
        }
        let delay = retry_after(
            result.as_ref().ok().and_then(|r| r.retry_after.as_deref()),
            policy.retry_delay.saturating_mul(1 << (number - 1)),
        );
        // Drop response bytes and its error before waiting. Dropping this future
        // cancels both a pending physical attempt and this backoff.
        drop(result);
        tokio::time::sleep(delay).await;
    }
    unreachable!("every nonempty attempt sequence returns its final result")
}

#[cfg(test)]
#[path = "../tests/transport/mod.rs"]
mod tests;
