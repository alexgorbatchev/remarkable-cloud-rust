---
created_on: 2026-10-14 05:45
last_modified: 2026-10-14 09:30
status: current
---

# Cloud contracts

This library implements the authentication, config, discovery, Sync v3 manifests, document content, listing/resolution, caching, document creation, and file-update contracts of `go-remarkable-cloud`.

## Rust interfaces

Network methods are asynchronous and use Tokio. Client clones share credentials. `ClientOptions.request_timeout` optionally adds a native reqwest request timeout to the attempt deadline. The library constructs its own native client with automatic retry and redirect disabled. Standalone authentication and discovery helpers accept a reqwest client built with `new_http_client`, or borrowed from `Client::http_client()`; supplying an independently configured client transfers responsibility for its hidden headers, retries, and redirects to the caller.

Errors are enums and typed structs. Mutation errors box `MutationFailure<T>` so its progress and underlying typed error survive without a large inline error representation. Creation and both update variants expose synchronous progress callbacks separately from their mutation options. Callbacks observe `staged`, `commit-unknown`, `committed`, and `verified` boundaries. Rejecting either pre-commit callback prevents the root request; rejecting the commit-unknown callback restores the returned state to staged.

`Discovery` carries both fallback endpoints and an optional error. `ItemMetadata.item_type` preserves arbitrary service strings. `DocumentContent.extra` retains unknown JSON properties during both decoding and encoding. Native page deletion and redirection values are integers.

## Write safety

Document hashes concatenate binary SHA-256 hashes in ID order and hash that concatenation. Root hashes cover serialized v4 index bytes. Root indexes include the aggregate record; document indexes use v3 without the aggregate.

Snapshot-bound writes compare both root hash and generation. All staging downloads and post-commit verification bypass cache. A failed progress callback before the commit prevents the commit; callbacks receive owned snapshots so they cannot mutate operational state. A 409 or 412 root response restores `staged`; other root failures retain `commit-unknown`. Post-commit failures retain `committed` or `verified` as applicable.

Root commits and authentication writes never recover from transport errors or redirects. Explicit authentication rejection can renew and replay the rejected request once. Immutable recovery lives in this library so all consumers share the CLI's safety policy.

## Physical HTTP attempts

`TransportPolicy` bounds each request and full body read with a 90-second Tokio deadline. Immutable GET/PUT paths must contain exactly one 64-character hexadecimal hash after `/sync/v3/files/`; buffered request bodies are cloned through reqwest's native `Request::try_clone`, preserving bytes and headers. Only these requests receive up to three physical attempts. HTTP 408, 429, 500, 502, 503, and 504, deadlines, selected native I/O failures, and hyper incomplete/closed connections recover. Malformed protocol errors, other statuses, root requests, discovery, and authentication do not recover.

The fallback delay starts at 250ms and doubles; valid unsigned 32-bit decimal `Retry-After` seconds or standard HTTP dates override it. Past dates wait zero. Dropping a future cancels network waiting and backoff. A successful partial body is never exposed or cached. A truncated rejection retains its HTTP status, received prefix, and native read error in `StatusError.body_error` and its standard error source. Consumers can classify the incomplete response independently of the status. A shorter `request_timeout` remains effective.

Native tower-http `FollowRedirect` wraps the physical-attempt service. The original method must be GET/HEAD or a replayable immutable blob PUT. Root commits and authentication writes return the original redirect response, including its body, without contacting the destination. The tenth redirect response returns typed `Error::RedirectLimit`. Each redirected hop independently receives the attempt deadline and, when its current method/path is immutable, three-attempt recovery.

HTTP 301/302/303 drops body headers and bytes, preserving GET/HEAD and converting other eligible methods to GET. HTTP 307/308 preserves the current method and body. Credential filtering permits the canonical original hostname and its subdomains, independent of port, and strips authorization, authentication challenge, and cookie headers cumulatively elsewhere. The middleware generates a referrer without URL user information and suppresses referrers on HTTPS-to-HTTP redirects. Every physical hop produces its own trace.

Native tracing target `remarkable_cloud::http` at DEBUG emits one event for every physical attempt, including failures and cancellation. Fields are method, URL path, filename, attempt number, received status if available, elapsed milliseconds, and a categorical outcome. Headers, tokens, bodies, query strings, and URL user information are excluded. Each attempt retains its tracing dispatcher so task abortion also emits under the correct subscriber.

## Corrected source defects and deliberate differences

- Credential renewal returns persistence errors after storing the minted token in memory. The Go implementation discards every persistence error, allowing apparent success with stale on-disk credentials. `credential_persistence_failure_is_reported_after_renewal` covers a blocked parent directory.
- Selecting an unreadable config explicitly returns the read failure. An absent explicit config remains valid for first-time pairing. The source discards both failures and can silently continue with unrelated in-memory credentials.
- Credentials and cache writes use unique temporary files with atomic rename. Credential writes sync the file before rename. Fixed-name temporary credentials in the source can collide across writers.
- Blob path components reject traversal and URL delimiters before network or cache access. The Go implementation joins unchecked caller hashes into filesystem paths.
- Manifest encoding checks signed size overflow rather than allowing an overflowing aggregate.
- Concurrent initial authentication and rejection-driven renewal share a renewal mutex; overlapping requests reuse an already renewed token.
- Status errors preserve a rejected response's status, received body prefix, and typed body read error when the body is truncated. The Go library's `readStatusError` discards the read error, hiding incomplete rejected responses from consumers. Successful responses with truncated bodies return transport errors.
- Immutable recovery and the Go CLI's read/blob redirect policy are shared by the library rather than implemented separately in each CLI.
- Arbitrary reqwest builder injection was replaced by the timeout option used by consumers. Reqwest merges default headers and applies them after outer redirect filtering; hidden authorization defaults can otherwise reappear at an unrelated redirect destination. Cloud headers are now owned explicitly by the library.
- After 301/302/303 removes a body, later 307/308 hops preserve the resulting bodyless request, matching Go 1.26. Referrers are suppressed on HTTPS-to-HTTP redirects, including an explicit referrer.
- Listings return a deterministic ID order instead of relying on map iteration or concurrent completion order.

## Limits

Local HTTP tests establish request, state, cache, and error contracts; they do not establish live Cloud compatibility or tablet behavior. The library does not encode PDFs, stroke files, native page initialization, or title-collision policy. Callers provide valid encoded files. Existing nonempty cache bytes are trusted rather than hashed, as in the source.

Config and cache filesystem operations are synchronous. A dropped mutation future provides no returned progress; retain callback snapshots outside that future before commit and inspect the server before retrying an uncertain operation. A callback returning an error after a commit cannot undo that commit.

Reqwest's underlying proxy tunnel implementation retains a special 407 error but discards other unsuccessful CONNECT response statuses. Those failures retain a native transport error, so the Go CLI's distinct 408/429/5xx proxy-CONNECT classification is unavailable through this transport.

## Development

Use `just check` for formatting, Clippy, tests, and at least 90% line coverage. `just build` builds all targets. Behavioral HTTP tests use local servers and synthetic credential values; they make no live Cloud writes. Test evidence belongs in `.tmp/`.

Official transport references: [reqwest ClientBuilder](https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html), [retry never](https://docs.rs/reqwest/latest/reqwest/retry/fn.never.html), [Response chunks](https://docs.rs/reqwest/latest/reqwest/struct.Response.html#method.chunk), [tower-http FollowRedirect](https://docs.rs/tower-http/latest/tower_http/follow_redirect/struct.FollowRedirect.html), [redirect Policy hooks](https://docs.rs/tower-http/latest/tower_http/follow_redirect/policy/trait.Policy.html).
