/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "iceberg/catalog/rest/http_client.h"

#include <algorithm>
#include <charconv>
#include <chrono>
#include <ctime>
#include <limits>
#include <map>
#include <optional>
#include <random>
#include <regex>
#include <thread>
#include <vector>

#include <cpr/cpr.h>
#include <curl/curl.h>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/constant.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client_internal.h"
#include "iceberg/catalog/rest/rest_util.h"
#include "iceberg/logging/log_macros.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

// Property names follow the Java HTTPClient.
constexpr std::string_view kRestMaxRetries = "rest.client.max-retries";
constexpr std::string_view kRestUserAgent = "rest.client.user-agent";
constexpr std::string_view kRestKeyLifetime = "rest.client.idempotency-key-lifetime";

// Parse an integer property. Error messages name the property but never
// echo its value, since server-provided configuration may reach these options.
template <typename Int>
Status ParseIntegerProperty(
    const std::unordered_map<std::string, std::string>& properties, std::string_view name,
    std::optional<Int>& target, Int minimum = 0) {
  auto it = properties.find(std::string(name));
  if (it == properties.end()) return {};
  std::string_view value = it->second;
  // Integer.parseInt/parseLong accept an explicit leading plus sign.
  if (value.starts_with('+')) {
    value.remove_prefix(1);
    if (value.starts_with('-')) {
      return InvalidArgument("Invalid integer HTTP client property '{}'", name);
    }
  }
  Int parsed{};
  auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), parsed);
  if (ec != std::errc{} || end != value.data() + value.size() || parsed < minimum) {
    return InvalidArgument("Invalid HTTP client property '{}': must be {} integer", name,
                           minimum == 0 ? "a non-negative" : "a positive");
  }
  target = parsed;
  return {};
}

bool CheckedAdd(int64_t left, int64_t right, int64_t& result) {
  if ((right > 0 && left > std::numeric_limits<int64_t>::max() - right) ||
      (right < 0 && left < std::numeric_limits<int64_t>::min() - right)) {
    return false;
  }
  result = left + right;
  return true;
}

bool CheckedSubtract(int64_t left, int64_t right, int64_t& result) {
  if ((right > 0 && left < std::numeric_limits<int64_t>::min() + right) ||
      (right < 0 && left > std::numeric_limits<int64_t>::max() + right)) {
    return false;
  }
  result = left - right;
  return true;
}

// Match Java Duration.parse's day/time syntax, including signed components and
// fractional seconds. Keep seconds and nanoseconds separate to avoid narrowing
// an otherwise valid duration before checking the millisecond bound.
std::optional<std::chrono::milliseconds> ParseKeyLifetime(const std::string& value) {
  static const std::regex pattern(
      R"(([+-]?)P(?:([+-]?[0-9]+)D)?(?:T(?:([+-]?[0-9]+)H)?(?:([+-]?[0-9]+)M)?(?:([+-]?[0-9]+)(?:[.,]([0-9]{0,9}))?S)?)?)",
      std::regex::icase);
  std::smatch match;
  if (!std::regex_match(value, match, pattern)) return std::nullopt;
  const bool has_time = match[3].matched || match[4].matched || match[5].matched;
  if ((!match[2].matched && !has_time) ||
      ((value.find('T') != std::string::npos || value.find('t') != std::string::npos) &&
       !has_time)) {
    return std::nullopt;
  }
  constexpr int64_t multipliers[] = {86400, 3600, 60, 1};
  int64_t components[4]{};
  for (size_t i = 0; i < 4; ++i) {
    if (!match[i + 2].matched) continue;
    const auto text = match[i + 2].str();
    auto view = std::string_view(text);
    if (view.starts_with('+')) view.remove_prefix(1);
    int64_t number{};
    auto [end, ec] = std::from_chars(view.data(), view.data() + view.size(), number);
    if (ec != std::errc{} || end != view.data() + view.size() ||
        number > std::numeric_limits<int64_t>::max() / multipliers[i] ||
        number < std::numeric_limits<int64_t>::min() / multipliers[i]) {
      return std::nullopt;
    }
    components[i] = number * multipliers[i];
  }
  int64_t seconds = components[3];
  for (int i = 2; i >= 0; --i) {
    if (!CheckedAdd(seconds, components[i], seconds)) return std::nullopt;
  }
  int64_t nanos = 0;
  const auto fraction = match[6].str();
  for (char digit : fraction) nanos = nanos * 10 + (digit - '0');
  if (!fraction.empty()) {
    for (size_t i = fraction.size(); i < 9; ++i) nanos *= 10;
    if (match[5].str().starts_with('-')) nanos = -nanos;
  }
  if (nanos < 0) {
    if (!CheckedAdd(seconds, -1, seconds)) return std::nullopt;
    nanos += 1000000000;
  }
  if (match[1].str() == "-") {
    if (nanos == 0) {
      if (seconds == std::numeric_limits<int64_t>::min()) return std::nullopt;
      seconds = -seconds;
    } else {
      seconds = seconds >= 0 ? -seconds - 1 : -(seconds + 1);
      nanos = 1000000000 - nanos;
    }
  }
  if (seconds < 0 || (seconds == 0 && nanos == 0)) {
    return std::chrono::milliseconds::zero();
  }
  constexpr auto kMaxMillis = std::chrono::milliseconds::max().count();
  if (seconds > kMaxMillis / 1000 ||
      (seconds == kMaxMillis / 1000 && nanos > (kMaxMillis % 1000) * 1000000)) {
    return std::nullopt;
  }
  return std::chrono::milliseconds(seconds * 1000 + nanos / 1000000);
}

}  // namespace

Result<HttpClientOptions> HttpClientOptions::FromProperties(
    const std::unordered_map<std::string, std::string>& properties) {
  HttpClientOptions options;
  ICEBERG_RETURN_UNEXPECTED(ParseIntegerProperty(
      properties, RestCatalogProperties::kClientConnectionTimeoutMs.key(),
      options.connection_timeout_ms));
  ICEBERG_RETURN_UNEXPECTED(ParseIntegerProperty(
      properties, RestCatalogProperties::kClientSocketTimeoutMs.key(),
      options.socket_timeout_ms));
  std::optional<int> max_retries;
  ICEBERG_RETURN_UNEXPECTED(
      ParseIntegerProperty(properties, kRestMaxRetries, max_retries, 1));
  if (max_retries.has_value()) options.max_retries = *max_retries;
  if (auto it = properties.find(std::string(kRestUserAgent)); it != properties.end()) {
    options.user_agent = it->second;
  }
  if (auto it = properties.find(std::string(kRestKeyLifetime)); it != properties.end()) {
    options.idempotency_key_lifetime = ParseKeyLifetime(it->second);
    if (!options.idempotency_key_lifetime.has_value()) {
      ICEBERG_LOG_WARN("Ignoring malformed or out-of-range HTTP client property '{}'",
                       kRestKeyLifetime);
    }
  }
  return options;
}

class HttpResponse::Impl {
 public:
  explicit Impl(cpr::Response&& response) : response_(std::move(response)) {}
  ~Impl() = default;

  int32_t status_code() const { return static_cast<int32_t>(response_.status_code); }

  std::string body() const { return response_.text; }

  std::unordered_map<std::string, std::string> headers() const {
    return {response_.header.begin(), response_.header.end()};
  }

 private:
  cpr::Response response_;
};

HttpResponse::HttpResponse() = default;
HttpResponse::HttpResponse(int32_t status_code, std::string body) {
  cpr::Response cpr_response;
  cpr_response.status_code = status_code;
  cpr_response.text = std::move(body);
  impl_ = std::make_unique<HttpResponse::Impl>(std::move(cpr_response));
}

HttpResponse::~HttpResponse() = default;
HttpResponse::HttpResponse(HttpResponse&&) noexcept = default;
HttpResponse& HttpResponse::operator=(HttpResponse&&) noexcept = default;

int32_t HttpResponse::status_code() const { return impl_->status_code(); }

std::string HttpResponse::body() const { return impl_->body(); }

std::unordered_map<std::string, std::string> HttpResponse::headers() const {
  return impl_->headers();
}

namespace {

// The retry rules below mirror
// org.apache.iceberg.rest.ExponentialHttpRequestRetryStrategy.

constexpr std::string_view kHeaderRetryAfter = "Retry-After";

/// \brief HTTP methods whose repetition is safe (RFC 9110 idempotent methods).
bool IsIdempotent(HttpMethod method) {
  switch (method) {
    case HttpMethod::kGet:
    case HttpMethod::kHead:
    case HttpMethod::kDelete:
    case HttpMethod::kPut:
      return true;
    default:
      return false;
  }
}

/// \brief Request-execution I/O failures eligible for Java-style retries.
/// An empty reply corresponds to NoHttpResponseException. Premature entity
/// closure, malformed responses, DNS, connect, TLS and timeout errors are excluded.
bool IsRetriableTransportError(cpr::ErrorCode code) {
  switch (code) {
    case cpr::ErrorCode::GOT_NOTHING:
    case cpr::ErrorCode::SEND_ERROR:
    case cpr::ErrorCode::RECV_ERROR:
    case cpr::ErrorCode::HTTP2:
    case cpr::ErrorCode::HTTP2_STREAM:
      return true;
    default:
      return false;
  }
}

}  // namespace

namespace {

std::chrono::milliseconds Backoff(int64_t exec_count) {
  if (exec_count <= 0) return std::chrono::milliseconds::zero();
  return std::chrono::milliseconds(
      1000 * (exec_count > 7 ? int64_t{64} : int64_t{1} << (exec_count - 1)));
}

std::optional<std::chrono::milliseconds> RetryAfter(
    const cpr::Response& response, std::chrono::system_clock::time_point now) {
  auto it = response.header.find(std::string(kHeaderRetryAfter));
  if (it == response.header.end()) return std::nullopt;
  auto value = std::string_view(it->second);
  if (value.starts_with('+')) value.remove_prefix(1);
  int64_t seconds = 0;
  auto [end, ec] = std::from_chars(value.data(), value.data() + value.size(), seconds);
  if (ec == std::errc{} && end == value.data() + value.size()) {
    if (seconds <= 0) return std::nullopt;
    constexpr auto kMaxSeconds = std::chrono::milliseconds::max().count() / 1000;
    if (seconds > kMaxSeconds) return std::chrono::milliseconds::max();
    return std::chrono::milliseconds(seconds * 1000);
  }
  const auto date = curl_getdate(it->second.c_str(), nullptr);
  if (date < 0) return std::nullopt;
  const auto now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch())
          .count();
  const auto date_seconds = static_cast<int64_t>(date);
  if (date_seconds > std::chrono::milliseconds::max().count() / 1000) {
    return std::chrono::milliseconds::max();
  }
  const auto date_ms = date_seconds * 1000;
  if (date_ms <= now_ms) return std::nullopt;
  int64_t delay;
  if (!CheckedSubtract(date_ms, now_ms, delay)) return std::chrono::milliseconds::max();
  return std::chrono::milliseconds(delay);
}

}  // namespace

namespace detail {

bool ShouldRetry(const cpr::Response& response, const HttpRequest& request,
                 int64_t exec_count, const HttpClientOptions& options,
                 std::optional<std::chrono::milliseconds> elapsed,
                 std::chrono::system_clock::time_point now) {
  if (exec_count > options.max_retries) return false;
  const bool has_key = request.headers.contains("Idempotency-Key");
  const bool safe = IsIdempotent(request.method) || has_key;
  if (response.error) {
    // Java reads the final response entity outside HttpRequestRetryExec. A body
    // read failure is propagated, even for a successful status or keyed request.
    if (response.status_code != 0 || !safe ||
        !IsRetriableTransportError(response.error.code))
      return false;
  } else {
    const auto code = response.status_code;
    if (code != 429 &&
        !(code == 503 && response.header.contains(std::string(kHeaderRetryAfter))) &&
        !(safe &&
          (code == 408 || code == 500 || code == 502 || code == 503 || code == 504)))
      return false;
  }
  if (has_key && options.idempotency_key_lifetime.has_value() && elapsed.has_value()) {
    const auto lifetime = options.idempotency_key_lifetime->count();
    if (elapsed->count() >= lifetime) return false;
    int64_t remaining;
    if (!CheckedSubtract(lifetime, elapsed->count(), remaining)) {
      remaining = std::numeric_limits<int64_t>::max();
    }
    auto upper_delay = Backoff(exec_count);
    if (!response.error) {
      if (auto retry_after = RetryAfter(response, now)) {
        upper_delay = *retry_after;
      } else {
        upper_delay += upper_delay / 10;
      }
    }
    if (upper_delay.count() >= remaining) return false;
  }
  return true;
}

std::chrono::milliseconds RetryInterval(const cpr::Response& response, int64_t exec_count,
                                        std::chrono::system_clock::time_point now) {
  // HttpRequestRetryStrategy's I/O-exception overload defaults to no delay.
  if (response.error) return std::chrono::milliseconds::zero();
  if (auto retry_after = RetryAfter(response, now)) return *retry_after;
  const auto delay = Backoff(exec_count);
  thread_local std::mt19937 generator{std::random_device{}()};
  std::uniform_int_distribution<int64_t> jitter(
      0, std::max<int64_t>(1, delay.count() / 10) - 1);
  return delay + std::chrono::milliseconds(jitter(generator));
}

RetryTiming::RetryTiming()
    : now(std::chrono::system_clock::now),
      sleep([](std::chrono::milliseconds delay) { std::this_thread::sleep_for(delay); }) {
}

Result<cpr::Response> SendWithRetry(const HttpClientOptions& options,
                                    const HttpRequest& request, const SendAttempt& send,
                                    const RetryTiming& timing) {
  // Java records FIRST_ATTEMPT_EPOCH with System.currentTimeMillis().
  const auto first_attempt =
      std::chrono::time_point_cast<std::chrono::milliseconds>(timing.now());
  for (int64_t exec_count = 1;; ++exec_count) {
    ICEBERG_ASSIGN_OR_RAISE(auto attempt, send(exec_count, first_attempt));
    const auto now = timing.now();
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::time_point_cast<std::chrono::milliseconds>(now) - first_attempt);
    if (!attempt.retry_approved &&
        !ShouldRetry(attempt.response, request, exec_count, options, elapsed, now)) {
      return std::move(attempt.response);
    }
    const auto delay = RetryInterval(attempt.response, exec_count, timing.now());
    if (delay > std::chrono::milliseconds::zero()) timing.sleep(delay);
  }
}

bool SocketIdleTimeout::Check(int64_t downloaded, int64_t uploaded, int64_t header_bytes,
                              Clock::time_point now) {
  if (!last_progress_.has_value() || downloaded != downloaded_ || uploaded != uploaded_ ||
      header_bytes != header_bytes_) {
    last_progress_ = now;
    downloaded_ = downloaded;
    uploaded_ = uploaded;
    header_bytes_ = header_bytes;
  }
  return now - *last_progress_ < timeout_;
}

std::optional<std::chrono::milliseconds> SocketIdleTimeout::Remaining(
    Clock::time_point now) const {
  if (!last_progress_.has_value()) return std::nullopt;
  return std::max(
      std::chrono::milliseconds::zero(),
      std::chrono::ceil<std::chrono::milliseconds>(timeout_ - (now - *last_progress_)));
}

}  // namespace detail

namespace {

void ApplyOptions(cpr::Session& session, const HttpClientOptions& options) {
  const auto timeout =
      options.connection_timeout_ms.value_or(kDefaultConnectionTimeoutMs);
  // libcurl interprets zero as its own five-minute default. Use its largest
  // native deadline when Java's disabled connection timeout is requested.
  session.SetConnectTimeout(cpr::ConnectTimeout{std::chrono::milliseconds(
      timeout == 0 ? std::numeric_limits<long>::max()
                   : std::min<int64_t>(timeout, std::numeric_limits<long>::max()))});
}

// Drive transfers synchronously through libcurl's socket API. Observe socket
// readability before libcurl consumes a fragment: header callbacks only report
// complete lines, while the read deadline must include partial header progress.
template <typename CheckIdle>
Result<cpr::Response> PerformSession(cpr::Session& session,
                                     detail::SocketIdleTimeout* idle_timeout,
                                     CheckIdle check_idle) {
  auto* curl = session.GetCurlHolder()->handle;
  using SocketInterests = std::map<curl_socket_t, int>;
  // Keep callback data alive until remove_handle/cleanup finish invoking it.
  SocketInterests sockets;
  const auto cleanup = [curl](CURLM* multi) {
    curl_multi_remove_handle(multi, curl);
    curl_multi_cleanup(multi);
  };
  std::unique_ptr<CURLM, decltype(cleanup)> multi(curl_multi_init(), cleanup);
  if (!multi) return IOError("Failed to initialize HTTP transfer");
  const auto socket_callback =
      +[](CURL*, curl_socket_t socket, int action, void* data, void*) -> int {
    auto& interests = *static_cast<SocketInterests*>(data);
    try {
      if (action == CURL_POLL_REMOVE) {
        interests.erase(socket);
      } else {
        interests.insert_or_assign(socket, action);
      }
    } catch (...) {
      // Do not let an allocation failure escape through libcurl's C callback.
      return -1;
    }
    return 0;
  };
  if (curl_multi_setopt(multi.get(), CURLMOPT_SOCKETFUNCTION, socket_callback) !=
          CURLM_OK ||
      curl_multi_setopt(multi.get(), CURLMOPT_SOCKETDATA, &sockets) != CURLM_OK) {
    return IOError("Failed to initialize HTTP socket monitoring");
  }
  if (const auto result = curl_multi_add_handle(multi.get(), curl); result != CURLM_OK) {
    return IOError("Failed to prepare HTTP transfer: {}", curl_multi_strerror(result));
  }
  int running = 0;
  auto result = curl_multi_socket_action(multi.get(), CURL_SOCKET_TIMEOUT, 0, &running);
  CURLcode transfer_result = CURLE_OK;
  while (true) {
    if (result != CURLM_OK) {
      return IOError("HTTP transfer failed: {}", curl_multi_strerror(result));
    }
    if (running == 0) {
      int remaining = 0;
      const auto* message = curl_multi_info_read(multi.get(), &remaining);
      if (message == nullptr || message->msg != CURLMSG_DONE) {
        return IOError("HTTP transfer ended without a completion result");
      }
      transfer_result = message->data.result;
      break;
    }
    if (idle_timeout != nullptr && !check_idle()) {
      transfer_result = CURLE_OPERATION_TIMEDOUT;
      break;
    }
    auto wait_ms = std::chrono::milliseconds(1000);
    long curl_timeout = -1;
    if (const auto status = curl_multi_timeout(multi.get(), &curl_timeout);
        status != CURLM_OK) {
      return IOError("HTTP timer query failed: {}", curl_multi_strerror(status));
    }
    if (curl_timeout >= 0) {
      wait_ms = std::min(wait_ms, std::chrono::milliseconds(curl_timeout));
    }
    if (idle_timeout != nullptr) {
      if (auto remaining = idle_timeout->Remaining()) {
        wait_ms = std::min(wait_ms, *remaining);
      }
    }
    std::vector<curl_waitfd> watched;
    watched.reserve(sockets.size());
    for (const auto& [socket, action] : sockets) {
      short events = 0;
      if (action & CURL_POLL_IN) events |= CURL_WAIT_POLLIN;
      if (action & CURL_POLL_OUT) events |= CURL_WAIT_POLLOUT;
      if (events != 0) watched.push_back({socket, events, 0});
    }
    int activity = 0;
#if LIBCURL_VERSION_NUM >= 0x074200
    const auto poll_result = curl_multi_poll(
        multi.get(), watched.data(), static_cast<unsigned int>(watched.size()),
        static_cast<int>(wait_ms.count()), &activity);
#else
    const auto poll_started = std::chrono::steady_clock::now();
    const auto poll_result = curl_multi_wait(
        multi.get(), watched.data(), static_cast<unsigned int>(watched.size()),
        static_cast<int>(wait_ms.count()), &activity);
    if (activity == 0) std::this_thread::sleep_until(poll_started + wait_ms);
#endif
    if (poll_result != CURLM_OK) {
      return IOError("HTTP transfer poll failed: {}", curl_multi_strerror(poll_result));
    }
    bool handled_socket = false;
    for (const auto& socket : watched) {
      if (socket.revents == 0 || !sockets.contains(socket.fd)) continue;
      int events = 0;
      if (socket.revents & CURL_WAIT_POLLIN) {
        events |= CURL_CSELECT_IN;
        if (idle_timeout != nullptr) idle_timeout->RecordReadActivity();
      }
      if (socket.revents & CURL_WAIT_POLLOUT) events |= CURL_CSELECT_OUT;
      if (socket.revents & CURL_WAIT_POLLPRI) events |= CURL_CSELECT_ERR;
      result = curl_multi_socket_action(multi.get(), socket.fd, events, &running);
      handled_socket = true;
      if (result != CURLM_OK || running == 0) break;
    }
    if (!handled_socket) {
      result = curl_multi_socket_action(multi.get(), CURL_SOCKET_TIMEOUT, 0, &running);
    }
  }
  return session.Complete(transfer_result);
}

/// \brief Default error type for unparseable REST responses.
constexpr std::string_view kRestExceptionType = "RESTException";

/// \brief Do not follow HTTP redirects on catalog requests.
///
/// cpr follows redirects by default. Signed requests are bound to the exact
/// request URL and cannot be re-signed for a redirect target, and headers set
/// on the request (e.g. the relocated `Original-Authorization` header used by
/// SigV4) would be sent unchanged to the new location. A redirect response is
/// therefore surfaced to the caller as an error instead of being followed.
const cpr::Redirect kNoFollowRedirects{/*maximum=*/0L, /*follow=*/false,
                                       /*cont_send_cred=*/false,
                                       cpr::PostRedirectFlags::POST_ALL};

/// \brief Merge default headers with per-request headers (per-request wins).
HttpHeaders MergeHeaders(
    const HttpHeaders& default_headers,
    const std::unordered_map<std::string, std::string>& request_headers) {
  HttpHeaders merged;
  for (const auto& [key, val] : default_headers) {
    merged.try_emplace(key, val);
  }
  for (const auto& [key, val] : request_headers) {
    merged[key] = val;
  }
  return merged;
}

cpr::Header ToCprHeader(const HttpRequest& request) {
  return {request.headers.begin(), request.headers.end()};
}

/// \brief Append URL-encoded query parameters to a URL, sorted by key.
/// \param base_url must not already contain a query string. Callers pass query
///        parameters separately so authentication signs one unambiguous final URL.
Result<std::string> AppendQueryString(
    const std::string& base_url,
    const std::unordered_map<std::string, std::string>& params) {
  if (params.empty()) return base_url;
  if (base_url.find('?') != std::string::npos) {
    return InvalidArgument(
        "HttpClient base URL must not contain a query string when query parameters "
        "are passed separately: {}",
        base_url);
  }
  std::map<std::string, std::string> sorted(params.begin(), params.end());
  std::string url = base_url + "?";
  bool first = true;
  for (const auto& [k, v] : sorted) {
    if (!first) url += "&";
    ICEBERG_ASSIGN_OR_RAISE(auto ek, EncodeString(k));
    ICEBERG_ASSIGN_OR_RAISE(auto ev, EncodeString(v));
    url += ek + "=" + ev;
    first = false;
  }
  return url;
}

Result<HttpRequest> AuthenticateRequest(auth::AuthSession& session, HttpMethod method,
                                        std::string url, HttpHeaders headers,
                                        std::string body = "") {
  return session.Authenticate({.method = method,
                               .url = std::move(url),
                               .headers = std::move(headers),
                               .body = std::move(body)});
}

/// \brief Checks if the HTTP status code indicates a successful response.
bool IsSuccessful(int32_t status_code) {
  return status_code == 200      // OK
         || status_code == 202   // Accepted
         || status_code == 204   // No Content
         || status_code == 304;  // Not Modified
}

/// \brief Builds a default ErrorResponse when the response body cannot be parsed.
ErrorResponse BuildDefaultErrorResponse(const cpr::Response& response) {
  std::string message;
  if (response.status_code == 0 && !response.error.message.empty()) {
    // Transport failure: libcurl's description never contains response content.
    message = response.error.message;
  } else if (!response.reason.empty()) {
    message = response.reason;
  } else {
    message = GetStandardReasonPhrase(response.status_code);
  }
  return {
      .code = static_cast<uint32_t>(response.status_code),
      .type = std::string(kRestExceptionType),
      .message = std::move(message),
  };
}

/// \brief Fails a response whose transfer failed, even after a success status.
Status CheckTransfer(const cpr::Response& response) {
  if (response.error.code == cpr::ErrorCode::OPERATION_TIMEDOUT) {
    return IOError("HTTP request timed out: {}", response.error.message);
  }
  if (response.error) {
    if (IsSuccessful(response.status_code)) {
      return IOError("HTTP request failed after status {}: {}", response.status_code,
                     response.error.message);
    }
    return IOError("HTTP request failed: {}", response.error.message);
  }
  return {};
}

/// \brief Handles failure responses by invoking the provided error handler.
Status HandleFailureResponse(const cpr::Response& response,
                             const ErrorHandler& error_handler) {
  ICEBERG_RETURN_UNEXPECTED(CheckTransfer(response));
  if (IsSuccessful(response.status_code)) {
    return {};
  }
  auto parse_result = error_handler.ParseResponse(
      static_cast<uint32_t>(response.status_code), response.text);
  const auto final_error = parse_result.value_or(BuildDefaultErrorResponse(response));
  auto status = error_handler.Accept(final_error);
  if (status.has_value()) {
    return RestError("Unhandled error (code: {}, type: {}): {}", final_error.code,
                     final_error.type, final_error.message);
  }
  return status;
}

}  // namespace

std::shared_ptr<HttpClient> HttpClient::Make(
    std::unordered_map<std::string, std::string> default_headers,
    HttpClientOptions options, std::optional<std::string> base_uri) {
  return std::shared_ptr<HttpClient>(
      new HttpClient(WithOptions{}, std::move(default_headers), std::move(options), {},
                     std::move(base_uri)));
}

HttpClient::HttpClient(WithOptions,
                       std::unordered_map<std::string, std::string> default_headers,
                       HttpClientOptions options,
                       std::chrono::milliseconds request_timeout,
                       std::optional<std::string> base_uri)
    : base_uri_(std::move(base_uri)),
      options_(std::move(options)),
      default_headers_{default_headers.begin(), default_headers.end()},
      request_timeout_(request_timeout),
      connection_pool_{std::make_unique<cpr::ConnectionPool>()} {
  // Set default Content-Type for all requests (including GET/HEAD/DELETE).
  // Many systems require that content type is set regardless, even on an empty body.
  default_headers_[kHeaderContentType] = kMimeTypeApplicationJson;
  default_headers_.try_emplace(kHeaderAccept, kMimeTypeApplicationJson);
  default_headers_.try_emplace(kHeaderUserAgent,
                               options_.user_agent.value_or(kUserAgent));
}

HttpClient::HttpClient(std::unordered_map<std::string, std::string> default_headers,
                       std::chrono::milliseconds connection_timeout,
                       std::chrono::milliseconds socket_timeout,
                       std::optional<std::string> base_uri)
    : HttpClient(WithOptions{}, std::move(default_headers),
                 HttpClientOptions{.connection_timeout_ms =
                                       std::max<int64_t>(0, connection_timeout.count())},
                 socket_timeout, std::move(base_uri)) {}

HttpClient::~HttpClient() = default;

Result<std::string> HttpClient::RequestUri(const std::string& path) const {
  // Match Java HTTPRequest.check, then requestUri. URI syntax validation remains
  // the transport's responsibility rather than emulating java.net.URI.
  ICEBERG_PRECHECK(!path.starts_with('/'), "Request paths must not start with '/'");
  const bool has_absolute_path =
      path.starts_with("https://") || path.starts_with("http://");
  ICEBERG_PRECHECK(base_uri_.has_value() || has_absolute_path,
                   "Relative request path requires a base URI");
  std::string full_path;
  if (has_absolute_path) {
    full_path = path;
  } else {
    auto base_uri = TrimTrailingSlash(*base_uri_);
    full_path = std::string(TrimTrailingSlash(std::string(base_uri) + "/" + path));
  }
  return full_path;
}

Result<HttpResponse> HttpClient::Execute(HttpMethod method, const std::string& url,
                                         HttpHeaders headers, std::string body,
                                         const ErrorHandler& error_handler,
                                         auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto request,
      AuthenticateRequest(session, method, url, std::move(headers), std::move(body)));
  ICEBERG_ASSIGN_OR_RAISE(
      auto response,
      detail::SendWithRetry(
          options_, request,
          [&](int64_t exec_count, std::chrono::system_clock::time_point first_attempt)
              -> Result<detail::Attempt> {
            detail::SocketIdleTimeout idle_timeout(
                std::chrono::milliseconds(options_.socket_timeout_ms.value_or(0)));
            bool socket_timed_out = false;
            bool retry_approved = false;
            bool final_response_started = false;
            std::string raw_headers;
            cpr::Session cpr_session;
            cpr_session.SetConnectionPool(*connection_pool_);
            ApplyOptions(cpr_session, options_);
            cpr_session.SetRedirect(kNoFollowRedirects);
            cpr_session.SetTimeout(cpr::Timeout{request_timeout_});
            cpr_session.SetUrl(cpr::Url{request.url});
            cpr_session.SetHeader(ToCprHeader(request));
            auto* curl = cpr_session.GetCurlHolder()->handle;
            cpr_session.SetHeaderCallback(cpr::HeaderCallback{[&](std::string_view header,
                                                                  intptr_t /*userdata*/) {
              raw_headers.append(header);
              cpr::Response candidate;
              curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &candidate.status_code);
              if (candidate.status_code >= 200) final_response_started = true;
              if (header != "\r\n" && header != "\n") return true;
              if (candidate.status_code < 200) return true;
              candidate.header = cpr::util::parseHeader(raw_headers);
              const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::time_point_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now()) -
                  first_attempt);
              if (detail::ShouldRetry(candidate, request, exec_count, options_,
                                      elapsed)) {
                // Apache decides HTTP-status retries before reading the entity.
                // Do not wait for an error response body that may never arrive.
                // Aborting before draining the body sacrifices connection reuse
                // for this response; subsequent successful requests still reuse the pool.
                retry_approved = true;
                return false;
              }
              return true;
            }});
            const bool has_socket_timeout = options_.socket_timeout_ms.value_or(0) > 0;
            cpr::cpr_pf_arg_t download_progress = 0;
            cpr::cpr_pf_arg_t upload_progress = 0;
            cpr::cpr_pf_arg_t upload_size = 0;
            const auto check_idle = [&] {
              double pretransfer_seconds = 0;
              long request_bytes = 0;
              curl_easy_getinfo(curl, CURLINFO_PRETRANSFER_TIME, &pretransfer_seconds);
              curl_easy_getinfo(curl, CURLINFO_REQUEST_SIZE, &request_bytes);
              if ((pretransfer_seconds == 0 && request_bytes == 0 &&
                   download_progress == 0 && upload_progress == 0 &&
                   raw_headers.empty()) ||
                  (!final_response_started && upload_progress < upload_size)) {
                idle_timeout.Reset();
                return true;
              }
              if (!idle_timeout.Check(download_progress,
                                      final_response_started ? 0 : upload_progress,
                                      raw_headers.size())) {
                socket_timed_out = true;
                return false;
              }
              return true;
            };
            if (has_socket_timeout) {
              cpr_session.SetProgressCallback(cpr::ProgressCallback{
                  [&](cpr::cpr_pf_arg_t /*download_total*/, cpr::cpr_pf_arg_t downloaded,
                      cpr::cpr_pf_arg_t upload_total, cpr::cpr_pf_arg_t uploaded,
                      intptr_t /*userdata*/) {
                    download_progress = downloaded;
                    upload_size = upload_total;
                    upload_progress = uploaded;
                    return check_idle();
                  }});
            }
            switch (request.method) {
              case HttpMethod::kGet:
                cpr_session.PrepareGet();
                break;
              case HttpMethod::kHead:
                cpr_session.PrepareHead();
                break;
              case HttpMethod::kPost:
                cpr_session.SetBody(cpr::Body{request.body});
                cpr_session.PreparePost();
                break;
              case HttpMethod::kDelete:
                cpr_session.PrepareDelete();
                break;
              default:
                return NotSupported("Unsupported HTTP method");
            }
            ICEBERG_ASSIGN_OR_RAISE(
                auto result,
                PerformSession(cpr_session, has_socket_timeout ? &idle_timeout : nullptr,
                               check_idle));
            result.raw_header = std::move(raw_headers);
            result.header = cpr::util::parseHeader(result.raw_header, &result.status_line,
                                                   &result.reason);
            if (retry_approved) {
              result.error = {};
            } else if (socket_timed_out) {
              result.error.code = cpr::ErrorCode::OPERATION_TIMEDOUT;
              result.error.message =
                  "No response progress within the configured socket timeout";
            }
            return detail::Attempt{.response = std::move(result),
                                   .retry_approved = retry_approved};
          }));

  ICEBERG_RETURN_UNEXPECTED(HandleFailureResponse(response, error_handler));
  HttpResponse http_response;
  http_response.impl_ = std::make_unique<HttpResponse::Impl>(std::move(response));
  return http_response;
}

Result<HttpResponse> HttpClient::Get(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto request_uri, RequestUri(path));
  ICEBERG_ASSIGN_OR_RAISE(auto url, AppendQueryString(request_uri, params));
  return Execute(HttpMethod::kGet, url, MergeHeaders(default_headers_, headers), "",
                 error_handler, session);
}

Result<HttpResponse> HttpClient::Post(
    const std::string& path, const std::string& body,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto request_uri, RequestUri(path));
  return Execute(HttpMethod::kPost, request_uri, MergeHeaders(default_headers_, headers),
                 body, error_handler, session);
}

Result<HttpResponse> HttpClient::PostForm(
    const std::string& path,
    const std::unordered_map<std::string, std::string>& form_data,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto request_uri, RequestUri(path));
  std::unordered_map<std::string, std::string> form_headers(headers);
  form_headers.insert_or_assign(kHeaderContentType, kMimeTypeFormUrlEncoded);
  std::vector<cpr::Pair> pair_list;
  pair_list.reserve(form_data.size());
  for (const auto& [key, val] : form_data) {
    pair_list.emplace_back(key, val);
  }
  // Sign the exact bytes cpr will put on the wire.
  std::string encoded_body =
      cpr::Payload(pair_list.begin(), pair_list.end()).GetContent();
  return Execute(HttpMethod::kPost, request_uri,
                 MergeHeaders(default_headers_, form_headers), std::move(encoded_body),
                 error_handler, session);
}

Result<HttpResponse> HttpClient::Head(
    const std::string& path, const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto request_uri, RequestUri(path));
  return Execute(HttpMethod::kHead, request_uri, MergeHeaders(default_headers_, headers),
                 "", error_handler, session);
}

Result<HttpResponse> HttpClient::Delete(
    const std::string& path, const std::unordered_map<std::string, std::string>& params,
    const std::unordered_map<std::string, std::string>& headers,
    const ErrorHandler& error_handler, auth::AuthSession& session) {
  ICEBERG_ASSIGN_OR_RAISE(auto request_uri, RequestUri(path));
  ICEBERG_ASSIGN_OR_RAISE(auto url, AppendQueryString(request_uri, params));
  return Execute(HttpMethod::kDelete, url, MergeHeaders(default_headers_, headers), "",
                 error_handler, session);
}

}  // namespace iceberg::rest
