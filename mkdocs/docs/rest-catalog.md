<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# REST catalog transport

The transport and retry properties follow the Java `HTTPClient` and
`ExponentialHttpRequestRetryStrategy`. They apply to configuration discovery,
OAuth token requests, catalog operations, and storage credential refresh requests.
Initial requests use client properties; subsequent requests use merged catalog
properties.

| Property | Default | Notes |
| --- | --- | --- |
| `rest.client.connection-timeout-ms` | 180000 | Non-negative integer; zero disables the configured deadline |
| `rest.client.socket-timeout-ms` | Unbounded | Non-negative integer; zero disables the response inactivity timeout |
| `rest.client.max-retries` | 5 | Positive integer; excludes the first attempt; does not bound total wait time |
| `rest.client.user-agent` | `iceberg-cpp/<version>` | Used when the request has no explicit `User-Agent` header |
| `rest.client.idempotency-key-lifetime` | Key-based retries disabled | Set from the top-level `/v1/config` field `idempotency-key-lifetime`, an ISO-8601 duration such as `PT30M`; absent, malformed, out-of-range or non-positive values do not enable key-based retries |

Only the top-level `idempotency-key-lifetime` field advertises server support for
idempotency keys. The catalog passes that value to the transport after discovery;
client properties and server `defaults` or `overrides` cannot enable support or
extend the advertised window. A standalone `HttpClient` must be given a positive
lifetime only when the server supports idempotency keys.

The socket timeout measures response inactivity rather than average transfer
speed or total request duration. It does not bound request-body uploads. The
legacy `HttpClient` constructor's third argument remains a total timeout per
attempt. Transfers run synchronously in the calling thread; no executor is
required for HTTP retries.

HTTP response retries are decided after receiving the headers, before reading
the response body:

- HTTP 429, and HTTP 503 carrying `Retry-After`, permit retries for any method.
- GET, HEAD and DELETE, and requests carrying an `Idempotency-Key` header with a
  positive server-advertised lifetime, also
  permit retries on HTTP 408, 500, 502, 503 and 504.
- Positive numeric and HTTP-date `Retry-After` values are honored. All three
  HTTP-date formats are accepted; other date syntax uses the normal backoff. The
  normal wait doubles from 1 second up to 64 seconds, with up to 10% jitter. Invalid or
  non-positive values use this backoff; numeric durations beyond the millisecond
  range saturate at the maximum representable duration.

Response trailers do not change the retry decision made at the end of the final
headers.

Recoverable I/O failures before the final response headers are complete permit
retries for idempotent methods or supported requests carrying `Idempotency-Key`.
This includes resets after receiving a status line or partial headers. As in Java's
I/O-exception retry path, these retries have no delay. Name resolution,
connection, TLS, timeout, cancellation and premature-response-closure failures
are not retried. Failures while reading a final response body are propagated,
including when its HTTP status was successful.

For keyed requests, the lifetime starts at the first send. A retry is abandoned
when elapsed time plus the maximum next retry interval reaches the configured
lifetime. The bound includes `Retry-After` or the largest possible HTTP backoff
jitter. As in Java, the I/O-error lifetime check uses the exponential interval
even though the exception retry itself has no delay. Unkeyed requests are not
constrained by this property.
The client checks expiry again before sending a retry, so response cleanup or an
oversleep cannot send a retry after the advertised window expires.

Authentication runs once before the first send. Retries replay the same
headers, body and idempotency key. A subsequent logical request authenticates
again and can use an updated token.

The client does not generate idempotency keys. Supply a distinct key for each
logical mutation through per-request headers or an `AuthSession`. A static
`header.Idempotency-Key` is unsuitable for unrelated mutations.

Errors are classified from the `code` reported in the error body, falling back
to the HTTP status when the body cannot be parsed. Service diagnostics remain
available.

Other Java transport options, including `rest.client.max-connections`,
`rest.client.connections-per-route`, `rest.client.proxy.*` and
`rest.client.tls.configurer-impl`, are not yet supported by this transport.

For temporary storage credentials and their lifetime, see [FileIO](file-io.md#storage-credentials).
