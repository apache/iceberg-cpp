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

#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <format>
#include <limits>
#include <mutex>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

#include <cpr/cpr.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#ifndef _WIN32
#  include <poll.h>
#  include <unistd.h>

#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <sys/time.h>
#endif

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

#ifndef _WIN32

namespace {

/// \brief Minimal HTTP/1.1 server on 127.0.0.1 that answers every request with
/// fixed responses, optionally keeping the connection open for the next request.
class LoopbackHttpServer {
 public:
  explicit LoopbackHttpServer(std::string response,
                              std::chrono::milliseconds body_byte_interval = {},
                              std::chrono::milliseconds header_byte_interval = {})
      : response_(std::move(response)),
        body_byte_interval_(body_byte_interval),
        header_byte_interval_(header_byte_interval) {}

  explicit LoopbackHttpServer(std::vector<std::string> responses,
                              bool wait_for_client_close = false, bool keep_alive = false)
      : responses_(std::move(responses)),
        wait_for_client_close_(wait_for_client_close),
        keep_alive_(keep_alive) {}

  ~LoopbackHttpServer() {
    stop_ = true;
    if (thread_.joinable()) {
      thread_.join();
    }
    if (listen_fd_ >= 0) {
      close(listen_fd_);
    }
  }

  LoopbackHttpServer(const LoopbackHttpServer&) = delete;
  LoopbackHttpServer& operator=(const LoopbackHttpServer&) = delete;

  /// \brief Bind an ephemeral port and start serving. Returns false on failure.
  bool Start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
      return false;
    }
    int reuse = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        listen(listen_fd_, 8) != 0) {
      return false;
    }
    socklen_t len = sizeof(addr);
    if (getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
      return false;
    }
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this] { Serve(); });
    return true;
  }

  int port() const { return port_; }
  int request_count() const { return request_count_.load(); }
  int connection_count() const { return connection_count_.load(); }
  std::vector<std::string> requests() const {
    std::lock_guard lock(requests_mutex_);
    return requests_;
  }
  std::string url(std::string_view path) const {
    return std::format("http://127.0.0.1:{}{}", port_, path);
  }

 private:
  void Serve() {
    while (!stop_) {
      pollfd pfd{.fd = listen_fd_, .events = POLLIN, .revents = 0};
      if (poll(&pfd, 1, /*timeout_ms=*/50) <= 0) {
        continue;
      }
      int conn = accept(listen_fd_, nullptr, nullptr);
      if (conn < 0) {
        continue;
      }
      ++connection_count_;
      do {
        auto request = ReadRequest(conn);
        if (request.empty()) break;
        {
          std::lock_guard lock(requests_mutex_);
          requests_.push_back(std::move(request));
        }
        const auto index = static_cast<size_t>(request_count_.fetch_add(1));
        const auto& response = responses_.empty()
                                   ? response_
                                   : responses_[std::min(index, responses_.size() - 1)];
        if (header_byte_interval_ > std::chrono::milliseconds::zero()) {
          const auto line_end = response.find("\r\n") + 2;
          // Fragment one complete header line. Its total transfer time can exceed
          // the idle timeout without delaying every header in the response.
          const auto header_end = response.find("\r\n", line_end) + 2;
          SendAll(conn, response.substr(0, line_end));
          for (size_t i = line_end; i < header_end && !stop_; ++i) {
            std::this_thread::sleep_for(header_byte_interval_);
            SendAll(conn, response.substr(i, 1));
          }
          if (!stop_) SendAll(conn, response.substr(header_end));
        } else if (body_byte_interval_ == std::chrono::milliseconds::zero()) {
          SendAll(conn, response);
        } else {
          const auto body_start = response.find("\r\n\r\n") + 4;
          SendAll(conn, response.substr(0, body_start));
          for (size_t i = body_start; i < response.size() && !stop_; ++i) {
            std::this_thread::sleep_for(body_byte_interval_);
            SendAll(conn, response.substr(i, 1));
          }
        }
        if (wait_for_client_close_) {
          std::array<char, 1024> buffer{};
          while (recv(conn, buffer.data(), buffer.size(), 0) > 0) {
          }
        }
      } while (keep_alive_ && !stop_);
      close(conn);
    }
  }

  /// \brief Consume the request headers and any Content-Length body so the
  /// client does not see a connection reset before it reads the response.
  static std::string ReadRequest(int conn) {
    timeval timeout{.tv_sec = 10, .tv_usec = 0};
    setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(conn, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    std::string request;
    std::array<char, 4096> buf{};
    size_t header_end = std::string::npos;
    size_t content_length = 0;
    while (true) {
      ssize_t n = recv(conn, buf.data(), buf.size(), 0);
      if (n <= 0) {
        return request;
      }
      request.append(buf.data(), static_cast<size_t>(n));
      if (header_end == std::string::npos) {
        header_end = request.find("\r\n\r\n");
        if (header_end == std::string::npos) {
          continue;
        }
        content_length = ParseContentLength(request.substr(0, header_end));
      }
      if (request.size() >= header_end + 4 + content_length) {
        return request;
      }
    }
  }

  static size_t ParseContentLength(std::string headers) {
    for (auto& c : headers) {
      c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    constexpr std::string_view kHeader = "content-length:";
    size_t pos = headers.find(kHeader);
    if (pos == std::string::npos) {
      return 0;
    }
    return std::stoul(headers.substr(pos + kHeader.size()));
  }

  static void SendAll(int conn, const std::string& data) {
#  ifdef MSG_NOSIGNAL
    constexpr int kFlags = MSG_NOSIGNAL;
#  else
    constexpr int kFlags = 0;
    int no_sigpipe = 1;
    setsockopt(conn, SOL_SOCKET, SO_NOSIGPIPE, &no_sigpipe, sizeof(no_sigpipe));
#  endif
    size_t sent = 0;
    while (sent < data.size()) {
      ssize_t n = send(conn, data.data() + sent, data.size() - sent, kFlags);
      if (n <= 0) {
        return;
      }
      sent += static_cast<size_t>(n);
    }
  }

  std::string response_;
  std::chrono::milliseconds body_byte_interval_{};
  std::chrono::milliseconds header_byte_interval_{};
  std::vector<std::string> responses_;
  bool wait_for_client_close_ = false;
  bool keep_alive_ = false;
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> stop_{false};
  std::atomic<int> request_count_{0};
  std::atomic<int> connection_count_{0};
  mutable std::mutex requests_mutex_;
  std::vector<std::string> requests_;
  std::thread thread_;
};

std::string ResponseBytes(int code, std::string_view body = "{}",
                          std::string_view extra_headers = "") {
  return std::format(
      "HTTP/1.1 {} Response\r\nContent-Length: {}\r\n{}"
      "Connection: close\r\n\r\n{}",
      code, body.size(), extra_headers, body);
}

class CountingAuthSession final : public auth::AuthSession {
 public:
  explicit CountingAuthSession(bool add_key = false) : add_key_(add_key) {}

  Result<HttpRequest> Authenticate(HttpRequest request) override {
    request.headers["Authorization"] = std::format("Bearer attempt-{}", ++calls);
    if (add_key_) request.headers["idempotency-key"] = "test-key";
    return request;
  }

  int calls = 0;

 private:
  bool add_key_;
};

// A loopback socket listening on an ephemeral port.
struct Listener {
  int fd = -1;
  uint16_t port = 0;

  ~Listener() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
};

void Listen(Listener& listener) {
  listener.fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener.fd, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::bind(listener.fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)),
            0);
  ASSERT_EQ(::listen(listener.fd, 1), 0);
  socklen_t length = sizeof(address);
  ASSERT_EQ(::getsockname(listener.fd, reinterpret_cast<sockaddr*>(&address), &length),
            0);
  listener.port = ntohs(address.sin_port);
}

// Accepts one connection, reads the request head and sends `response`. Then
// ends the connection if `end`, or else waits for the client to close it.
void ServeOnce(int listener, std::string_view response, bool end,
               int timeout_seconds = 10) {
  // Bound every wait, so the test cannot hang.
  timeval wait{};
  wait.tv_sec = timeout_seconds;
  ::setsockopt(listener, SOL_SOCKET, SO_RCVTIMEO, &wait, sizeof(wait));
  const int connection = ::accept(listener, nullptr, nullptr);
  if (connection < 0) {
    return;
  }
  ::setsockopt(connection, SOL_SOCKET, SO_RCVTIMEO, &wait, sizeof(wait));
  std::string request;
  std::array<char, 1024> buffer;
  while (request.find("\r\n\r\n") == std::string::npos) {
    const auto received = ::recv(connection, buffer.data(), buffer.size(), 0);
    if (received <= 0) {
      break;
    }
    request.append(buffer.data(), static_cast<size_t>(received));
  }
  ::send(connection, response.data(), response.size(), 0);
  if (end) {
    ::shutdown(connection, SHUT_WR);
  }
  // Drain until the client closes, so ours closes cleanly.
  while (::recv(connection, buffer.data(), buffer.size(), 0) > 0) {
  }
  ::close(connection);
}

}  // namespace

TEST(HttpClientTest, GetFailsWhenItsTransferFails) {
  // A 200 arriving first does not make a truncated response a success.
  constexpr std::string_view kTruncated =
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
      "Content-Length: 100\r\n\r\n{";
  struct Case {
    std::string_view name;
    std::string_view response;
    bool end;
    std::string_view error;
  };
  for (const auto& test_case :
       {Case{.name = "no response", .response = "", .end = false, .error = "timed out"},
        Case{.name = "body stalls",
             .response = kTruncated,
             .end = false,
             .error = "timed out"}}) {
    SCOPED_TRACE(test_case.name);
    Listener listener;
    ASSERT_NO_FATAL_FAILURE(Listen(listener));
    const auto url = std::format("http://127.0.0.1:{}/v1/config", listener.port);

    HttpClient client({}, {}, std::chrono::milliseconds(500));
    auto session = auth::AuthSession::MakeDefault({});
    std::thread server(
        [&] { ServeOnce(listener.fd, test_case.response, test_case.end); });
    auto response = client.Get(url, /*params=*/{}, /*headers=*/{},
                               *DefaultErrorHandler::Instance(), *session);
    server.join();
    EXPECT_THAT(response, IsError(ErrorKind::kIOError));
    EXPECT_THAT(response, HasErrorMessage(test_case.error));
  }
}

TEST(HttpClientTest, SocketTimeoutAbortsAStalledTransfer) {
  for (std::string_view bytes :
       {std::string_view(""), std::string_view("HTTP/1.1 200 OK\r\n"
                                               "Content-Length: 100\r\n\r\n{")}) {
    SCOPED_TRACE(bytes);
    Listener listener;
    ASSERT_NO_FATAL_FAILURE(Listen(listener));
    const auto url = std::format("http://127.0.0.1:{}/v1/config", listener.port);
    auto client = HttpClient::Make(
        {}, HttpClientOptions{.socket_timeout_ms = 250, .max_retries = 0});
    auto session = auth::AuthSession::MakeDefault({});
    // Bound the server independently so a broken idle timer cannot hang the test.
    std::thread server(
        [&] { ServeOnce(listener.fd, bytes, /*end=*/false, /*timeout_seconds=*/3); });
    auto response = client->Get(url, {}, {}, *DefaultErrorHandler::Instance(), *session);
    server.join();
    EXPECT_THAT(response, IsError(ErrorKind::kIOError));
    EXPECT_THAT(response, HasErrorMessage("timed out"));
  }
}

TEST(HttpClientTest, SocketTimeoutUsesTheMillisecondReadDeadline) {
  LoopbackHttpServer server(
      "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n"
      "Connection: close\r\n\r\nnull",
      std::chrono::milliseconds(400));
  ASSERT_TRUE(server.Start());
  auto client =
      HttpClient::Make({}, HttpClientOptions{.socket_timeout_ms = 250, .max_retries = 0});
  CountingAuthSession session;
  auto response = client->Get(server.url("/v1/config"), {}, {},
                              *DefaultErrorHandler::Instance(), session);
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("timed out"));
  EXPECT_EQ(server.request_count(), 1);
}

TEST(HttpClientTest, SocketTimeoutAppliesAfterEarlyPostRejection) {
  Listener listener;
  ASSERT_NO_FATAL_FAILURE(Listen(listener));
  const auto url = std::format("http://127.0.0.1:{}/v1/commit", listener.port);
  auto client =
      HttpClient::Make({}, HttpClientOptions{.socket_timeout_ms = 250, .max_retries = 1});
  CountingAuthSession session;
  // The server rejects Expect: 100-continue before the large upload begins,
  // then leaves the final response body incomplete.
  std::thread server([&] {
    ServeOnce(listener.fd, "HTTP/1.1 403 Forbidden\r\nContent-Length: 100\r\n\r\n{",
              /*end=*/false, /*timeout_seconds=*/2);
  });
  auto response = client->Post(url, std::string(2 * 1024 * 1024, 'x'), {},
                               *DefaultErrorHandler::Instance(), session);
  server.join();
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("timed out"));
}

TEST(HttpClientTest, SocketTimeoutAllowsAHeaderArrivingInFragments) {
  LoopbackHttpServer server(ResponseBytes(200, "{}", "X-Slow: fragment\r\n"),
                            /*body_byte_interval=*/{},
                            /*header_byte_interval=*/std::chrono::milliseconds(40));
  ASSERT_TRUE(server.Start());
  auto client =
      HttpClient::Make({}, HttpClientOptions{.socket_timeout_ms = 500, .max_retries = 1});
  CountingAuthSession session;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Get(server.url("/v1/config"), {}, {},
                                     *DefaultErrorHandler::Instance(), session));
  EXPECT_EQ(response.body(), "{}");
  EXPECT_EQ(response.headers().at("X-Slow"), "fragment");
  EXPECT_EQ(server.request_count(), 1);
}

TEST(HttpClientTest, SocketTimeoutAllowsSlowContinuousProgress) {
  // The response takes longer than the idle timeout, but each gap is shorter.
  LoopbackHttpServer server(
      "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n"
      "Connection: close\r\n\r\nnull",
      std::chrono::milliseconds(300));
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make(
      {}, HttpClientOptions{.socket_timeout_ms = 1000, .max_retries = 0});
  auto session = auth::AuthSession::MakeDefault({});
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Get(server.url("/v1/config"), {}, {},
                                     *DefaultErrorHandler::Instance(), *session));
  EXPECT_EQ(response.status_code(), 200);
  EXPECT_EQ(response.body(), "null");
  EXPECT_EQ(server.request_count(), 1);
}

TEST(HttpClientTest, DoesNotRetryTruncatedGetResponse) {
  LoopbackHttpServer server(
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
      "Content-Length: 100\r\nConnection: close\r\n\r\n{");
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make({}, HttpClientOptions{.max_retries = 1});
  auto session = auth::AuthSession::MakeDefault({});
  auto response = client->Get(server.url("/v1/config"), {}, {},
                              *DefaultErrorHandler::Instance(), *session);
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("failed after status 200"));
  EXPECT_EQ(server.request_count(), 1);
}

TEST(HttpClientTest, ReusesConnectionsAcrossRequestsWithFreshMultiHandles) {
  for (const int32_t socket_timeout : {0, 2000}) {
    SCOPED_TRACE(socket_timeout);
    const std::string response = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}";
    LoopbackHttpServer server(std::vector<std::string>{response, response},
                              /*wait_for_client_close=*/false, /*keep_alive=*/true);
    ASSERT_TRUE(server.Start());
    auto client = HttpClient::Make({}, {.connection_timeout_ms = 5000,
                                        .socket_timeout_ms = socket_timeout,
                                        .max_retries = 0});
    auto session = auth::AuthSession::MakeDefault({});
    for (int request = 0; request < 2; ++request) {
      ICEBERG_UNWRAP_OR_FAIL(auto result,
                             client->Get(server.url("/v1/config"), {}, {},
                                         *DefaultErrorHandler::Instance(), *session));
      EXPECT_EQ(result.body(), "{}");
    }
    EXPECT_EQ(server.request_count(), 2);
    EXPECT_EQ(server.connection_count(), 1);
  }
}

TEST(HttpClientTest, Retries503AtHeadersBeforeReadingTheBody) {
  LoopbackHttpServer server(
      std::vector<std::string>{"HTTP/1.1 503 Service Unavailable\r\nRetry-After: 1\r\n"
                               "Content-Length: 100\r\n\r\n{",
                               ResponseBytes(200)},
      /*wait_for_client_close=*/true);
  ASSERT_TRUE(server.Start());
  auto client =
      HttpClient::Make({}, HttpClientOptions{.socket_timeout_ms = 500, .max_retries = 1});
  CountingAuthSession session;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Post(server.url("/v1/commit"), "{}", {},
                                      *DefaultErrorHandler::Instance(), session));
  EXPECT_EQ(response.body(), "{}");
  EXPECT_EQ(server.request_count(), 2);
  EXPECT_EQ(session.calls, 1);
}

TEST(HttpClientTest, RetriesKeyedPostWithTheSameAuthentication) {
  LoopbackHttpServer server(std::vector<std::string>{
      ResponseBytes(500, "{}", "Retry-After: 1\r\n"), ResponseBytes(200)});
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make(
      {}, HttpClientOptions{.max_retries = 1,
                            .idempotency_key_lifetime = std::chrono::seconds(5)});
  CountingAuthSession session(/*add_key=*/true);
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Post(server.url("/v1/commit"), "{}", {},
                                      *DefaultErrorHandler::Instance(), session));
  EXPECT_EQ(response.status_code(), 200);
  EXPECT_EQ(server.request_count(), 2);
  EXPECT_EQ(session.calls, 1);
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 2);
  EXPECT_EQ(requests[0], requests[1]);
  EXPECT_THAT(requests[0], testing::HasSubstr("Bearer attempt-1"));
}

TEST(HttpClientTest, KeyLifetimeStopsBeforeWaitingForTheNextAttempt) {
  LoopbackHttpServer server(ResponseBytes(500, "{}", "Retry-After: 3600\r\n"));
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make(
      {}, HttpClientOptions{.max_retries = 5,
                            .idempotency_key_lifetime = std::chrono::seconds(1)});
  CountingAuthSession session(/*add_key=*/true);
  auto response = client->Post(server.url("/v1/commit"), "{}", {},
                               *DefaultErrorHandler::Instance(), session);
  EXPECT_THAT(response, IsError(ErrorKind::kInternalServerError));
  EXPECT_EQ(server.request_count(), 1);
  EXPECT_EQ(session.calls, 1);
}

TEST(HttpClientTest, UnkeyedPostIsNotRetriedOn500) {
  LoopbackHttpServer server(ResponseBytes(500));
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make({}, HttpClientOptions{.max_retries = 1});
  CountingAuthSession session;
  EXPECT_THAT(client->Post(server.url("/v1/commit"), "{}", {},
                           *DefaultErrorHandler::Instance(), session),
              IsError(ErrorKind::kInternalServerError));
  EXPECT_EQ(server.request_count(), 1);
  EXPECT_EQ(session.calls, 1);
}

TEST(HttpClientTest, RetriesAnEmptyReplyWithoutReauthenticating) {
  LoopbackHttpServer server(std::vector<std::string>{"", ResponseBytes(200)});
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make({}, HttpClientOptions{.max_retries = 1});
  CountingAuthSession session;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Get(server.url("/v1/config"), {}, {},
                                     *DefaultErrorHandler::Instance(), session));
  EXPECT_EQ(response.status_code(), 200);
  EXPECT_EQ(server.request_count(), 2);
  EXPECT_EQ(session.calls, 1);
}

TEST(HttpClientTest, FailsWhenAnErrorHandlerDoesNotReturnAnError) {
  class NonThrowingHandler final : public DefaultErrorHandler {
   public:
    Status Accept(const ErrorResponse&) const override { return {}; }
  } handler;
  LoopbackHttpServer server(ResponseBytes(400));
  ASSERT_TRUE(server.Start());
  HttpClient client;
  CountingAuthSession session;
  auto response = client.Get(server.url("/v1/config"), {}, {}, handler, session);
  EXPECT_THAT(response, IsError(ErrorKind::kRestError));
  EXPECT_THAT(response, HasErrorMessage("Unhandled error"));
}

TEST(HttpClientTest, ExplicitUserAgentHeaderWinsOverTheProperty) {
  LoopbackHttpServer server(ResponseBytes(200));
  ASSERT_TRUE(server.Start());
  auto client = HttpClient::Make({{"user-agent", "explicit/2.0"}},
                                 HttpClientOptions{.user_agent = "property/1.0"});
  CountingAuthSession session;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         client->Get(server.url("/v1/config"), {}, {},
                                     *DefaultErrorHandler::Instance(), session));
  const auto requests = server.requests();
  ASSERT_EQ(requests.size(), 1);
  EXPECT_THAT(requests[0], testing::HasSubstr("explicit/2.0"));
  EXPECT_THAT(requests[0], testing::Not(testing::HasSubstr("property/1.0")));
}

TEST(HttpClientTest, PostReportsTransferFailureAfterSuccessfulStatus) {
  // A write that got its status may have taken effect, so the caller must treat
  // the result as unknown rather than retrying or cleaning up automatically.
  constexpr std::string_view kBody = R"({"metadata-location":"s3://bucket/m.json"})";
  const auto response_bytes = std::format(
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
      "Transfer-Encoding: chunked\r\n\r\n{:x}\r\n{}\r\n",
      kBody.size(), kBody);
  Listener listener;
  ASSERT_NO_FATAL_FAILURE(Listen(listener));
  const auto url = std::format("http://127.0.0.1:{}/v1/commit", listener.port);

  // Outlives the response, which keeps a curl handle in its connection pool.
  HttpClient client;
  auto session = auth::AuthSession::MakeDefault({});
  std::thread server([&] { ServeOnce(listener.fd, response_bytes, /*end=*/true); });
  auto response =
      client.Post(url, "{}", /*headers=*/{}, *DefaultErrorHandler::Instance(), *session);
  server.join();
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("failed after status 200"));
}

#endif

class CapturingAuthSession final : public auth::AuthSession {
 public:
  Result<HttpRequest> Authenticate(HttpRequest request) override {
    request_ = std::move(request);
    return InvalidArgument("captured");
  }

  const HttpRequest& request() const { return request_; }

 private:
  HttpRequest request_;
};

TEST(HttpClientTest, BuildsRequestUriLikeJavaHttpRequest) {
  const std::string base_uri = "https://catalog.example/api/";
  for (const auto& client :
       {std::make_shared<HttpClient>(std::unordered_map<std::string, std::string>{},
                                     std::chrono::milliseconds{},
                                     std::chrono::milliseconds{}, base_uri),
        HttpClient::Make({}, {}, base_uri)}) {
    CapturingAuthSession session;
    const auto& handler = *DefaultErrorHandler::Instance();
    const auto expect_url = [&](const Result<HttpResponse>& response,
                                std::string_view expected_url) {
      EXPECT_THAT(response, IsError(ErrorKind::kInvalidArgument));
      EXPECT_THAT(response, HasErrorMessage("captured"));
      EXPECT_EQ(session.request().url, expected_url);
    };

    const std::string path = "v1/oauth/tokens///";
    const std::string url = "https://catalog.example/api/v1/oauth/tokens";
    expect_url(client->Get(path, {{"page", "2"}}, {}, handler, session), url + "?page=2");
    expect_url(client->Post(path, "{}", {}, handler, session), url);
    expect_url(client->PostForm(path, {}, {}, handler, session), url);
    expect_url(client->Head(path, {}, handler, session), url);
    expect_url(client->Delete(path, {{"page", "2"}}, {}, handler, session),
               url + "?page=2");

    expect_url(client->PostForm("https://auth.example/token/", {}, {}, handler, session),
               "https://auth.example/token/");
  }
}

// Redirect responses must be reported without replaying the request at the target.
TEST(HttpClientTest, DoesNotFollowRedirects) {
#ifdef _WIN32
  GTEST_SKIP() << "Loopback test server is only implemented for POSIX";
#else
  for (const int status_code : {301, 302, 303, 307, 308}) {
    SCOPED_TRACE(status_code);
    LoopbackHttpServer target(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
        "Content-Length: 2\r\nConnection: close\r\n\r\n{}");
    ASSERT_TRUE(target.Start());

    LoopbackHttpServer redirector(
        std::format("HTTP/1.1 {} Redirect\r\nLocation: {}\r\nContent-Length: 0\r\n"
                    "Connection: close\r\n\r\n",
                    status_code, target.url("/v1/config")));
    ASSERT_TRUE(redirector.Start());

    HttpClient client({}, std::chrono::seconds(10), std::chrono::seconds(10));
    auto session = auth::AuthSession::MakeDefault(
        {{"Authorization", "Bearer token"},
         {"Original-Authorization", "Bearer delegate-token"},
         {"X-Amz-Security-Token", "session-token"}});
    const auto& handler = *DefaultErrorHandler::Instance();
    const std::string url = redirector.url("/v1/config");

    auto expect_not_followed = [&](const Result<HttpResponse>& result,
                                   std::string_view method) {
      SCOPED_TRACE(method);
      EXPECT_THAT(result, IsError(ErrorKind::kRestError));
      EXPECT_THAT(result, HasErrorMessage(std::format("code: {}", status_code)));
      EXPECT_EQ(target.request_count(), 0);
    };

    expect_not_followed(client.Get(url, {}, {}, handler, *session), "GET");
    expect_not_followed(client.Post(url, "{}", {}, handler, *session), "POST");
    expect_not_followed(client.PostForm(url, {{"grant_type", "client_credentials"}}, {},
                                        handler, *session),
                        "POST form");
    expect_not_followed(client.Head(url, {}, handler, *session), "HEAD");
    expect_not_followed(client.Delete(url, {}, {}, handler, *session), "DELETE");

    EXPECT_EQ(redirector.request_count(), 5);
  }
#endif
}

TEST(HttpClientRetryTest, TransportRetriesMatchTheJavaExceptionBoundary) {
  struct Case {
    HttpMethod method;
    int status;
    cpr::ErrorCode error;
    bool retry;
  };
  const Case cases[] = {
      {HttpMethod::kGet, 0, cpr::ErrorCode::RECV_ERROR, true},
      {HttpMethod::kHead, 0, cpr::ErrorCode::GOT_NOTHING, true},
      {HttpMethod::kDelete, 0, cpr::ErrorCode::SEND_ERROR, true},
      {HttpMethod::kGet, 0, cpr::ErrorCode::OPERATION_TIMEDOUT, false},
      {HttpMethod::kGet, 0, cpr::ErrorCode::SSL_CONNECT_ERROR, false},
      {HttpMethod::kGet, 0, cpr::ErrorCode::COULDNT_CONNECT, false},
      {HttpMethod::kGet, 0, cpr::ErrorCode::PARTIAL_FILE, false},
      {HttpMethod::kGet, 0, cpr::ErrorCode::WEIRD_SERVER_REPLY, false},
      {HttpMethod::kGet, 200, cpr::ErrorCode::PARTIAL_FILE, false},
      {HttpMethod::kGet, 200, cpr::ErrorCode::RECV_ERROR, false},
      {HttpMethod::kPost, 0, cpr::ErrorCode::RECV_ERROR, false},
      {HttpMethod::kPost, 200, cpr::ErrorCode::PARTIAL_FILE, false},
  };
  for (const auto& test_case : cases) {
    SCOPED_TRACE(std::format("method={}, status={}, error={}",
                             static_cast<int>(test_case.method), test_case.status,
                             static_cast<int>(test_case.error)));
    cpr::Response response;
    response.status_code = test_case.status;
    response.error.code = test_case.error;
    const HttpRequest request{.method = test_case.method};
    EXPECT_EQ(detail::ShouldRetry(response, request, 1, {}), test_case.retry);
  }
}

TEST(HttpClientRetryTest, StatusRetriesRespectMethodAndIdempotencyKey) {
  for (const int code : {200, 400, 408, 429, 500, 502, 503, 504}) {
    SCOPED_TRACE(code);
    cpr::Response response;
    response.status_code = code;
    HttpRequest request{.method = HttpMethod::kGet};
    EXPECT_EQ(detail::ShouldRetry(response, request, 1, {}), code != 200 && code != 400);
    request.method = HttpMethod::kPost;
    EXPECT_EQ(detail::ShouldRetry(response, request, 1, {}), code == 429);
    request.headers["iDeMpOtEnCy-KeY"] = "key";
    EXPECT_EQ(detail::ShouldRetry(response, request, 1, {}), code != 200 && code != 400);
    request.headers.clear();
    response.header["retry-after"] = "1";
    EXPECT_EQ(detail::ShouldRetry(response, request, 1, {}), code == 429 || code == 503);
    EXPECT_FALSE(detail::ShouldRetry(response, request, 6, {}));
  }
}

TEST(HttpClientRetryTest, KeyedNetworkFailuresRetryWithoutAnExceptionDelay) {
  cpr::Response response;
  response.error.code = cpr::ErrorCode::RECV_ERROR;
  const HttpRequest request{.method = HttpMethod::kPost,
                            .headers = {{"Idempotency-Key", "key"}}};
  EXPECT_TRUE(detail::ShouldRetry(response, request, 1, {}));
  EXPECT_EQ(detail::RetryInterval(response, 1), std::chrono::milliseconds::zero());
}

TEST(HttpClientRetryTest, KeyLifetimeMatchesJavaBudgetChecks) {
  struct Case {
    std::chrono::milliseconds lifetime;
    std::chrono::milliseconds elapsed;
    bool has_key;
    std::string retry_after;
    int attempt;
    bool transport_error;
    bool retry;
  };
  const Case cases[] = {
      {std::chrono::seconds(1), {}, true, "3600", 1, false, false},
      {std::chrono::hours(1), {}, true, "1", 1, false, true},
      {std::chrono::milliseconds(1), {}, true, "", 1, true, false},
      {std::chrono::milliseconds(1), {}, false, "3600", 1, false, true},
      {std::chrono::minutes(30), std::chrono::minutes(31), true, "1", 1, false, false},
      {std::chrono::seconds(2), std::chrono::milliseconds(1100), true, "1", 2, false,
       false},
      {std::chrono::milliseconds(1), {}, true, "", 1, false, false},
      {{}, {}, true, "1", 1, false, false},
      {{}, {}, false, "1", 1, false, true},
      {std::chrono::milliseconds(1050), {}, true, "", 1, false, false},
      {std::chrono::milliseconds(1101), {}, true, "", 1, false, true},
  };
  for (const auto& test_case : cases) {
    cpr::Response response;
    response.status_code = test_case.transport_error ? 0 : 500;
    if (test_case.transport_error) response.error.code = cpr::ErrorCode::RECV_ERROR;
    if (!test_case.retry_after.empty())
      response.header["Retry-After"] = test_case.retry_after;
    HttpRequest request{.method =
                            test_case.has_key ? HttpMethod::kPost : HttpMethod::kGet};
    if (test_case.has_key) request.headers["Idempotency-Key"] = "key";
    HttpClientOptions options{.idempotency_key_lifetime = test_case.lifetime};
    EXPECT_EQ(detail::ShouldRetry(response, request, test_case.attempt, options,
                                  test_case.elapsed),
              test_case.retry);
  }
  // Like Java, a missing first-attempt timestamp does not impose a bound.
  cpr::Response response;
  response.status_code = 500;
  response.header["Retry-After"] = "3600";
  const HttpRequest request{.method = HttpMethod::kPost,
                            .headers = {{"Idempotency-Key", "key"}}};
  EXPECT_TRUE(detail::ShouldRetry(
      response, request, 1, {.idempotency_key_lifetime = std::chrono::milliseconds(1)}));
}

TEST(HttpClientRetryTest, RetryAfterSupportsSecondsAndMillisecondDateDifferences) {
  cpr::Response response;
  response.header["Retry-After"] = "+2";
  EXPECT_EQ(detail::RetryInterval(response, 1), std::chrono::seconds(2));
  response.header["Retry-After"] = "Thu, 01 Jan 1970 00:00:02 GMT";
  EXPECT_EQ(detail::RetryInterval(response, 1,
                                  std::chrono::system_clock::time_point{} +
                                      std::chrono::milliseconds(1750)),
            std::chrono::milliseconds(250));
}

TEST(HttpClientRetryTest, LargeRetryAfterValuesSaturateWithoutOverflow) {
  for (const auto& value :
       {"9223372036854776", "18446744073709552", "9223372036854775807"}) {
    cpr::Response response;
    response.header["Retry-After"] = value;
    EXPECT_EQ(detail::RetryInterval(response, 1), std::chrono::milliseconds::max());
  }
}

TEST(HttpClientRetryTest, InvalidRetryAfterFallsBackWithoutOverflow) {
  for (const auto& value : {"9223372036854775808", "-9223372036854775808", "0", "-1",
                            "invalid", "Thu, 01 Jan 1970 00:00:00 GMT"}) {
    SCOPED_TRACE(value);
    cpr::Response response;
    response.header["Retry-After"] = value;
    const auto delay = detail::RetryInterval(
        response, 1,
        std::chrono::system_clock::time_point{} + std::chrono::milliseconds(1));
    EXPECT_GE(delay, std::chrono::seconds(1));
    EXPECT_LT(delay, std::chrono::milliseconds(1100));
  }
}

TEST(HttpClientRetryTest, ExponentialBackoffIsCapped) {
  cpr::Response response;
  EXPECT_EQ(detail::RetryInterval(response, 0), std::chrono::milliseconds::zero());
  for (int64_t attempt : {int64_t{7}, int64_t{8}, std::numeric_limits<int64_t>::max()}) {
    const auto delay = detail::RetryInterval(response, attempt);
    EXPECT_GE(delay, std::chrono::seconds(64));
    EXPECT_LT(delay, std::chrono::milliseconds(70400));
  }
}

class HttpClientRetryExecutionTest : public ::testing::Test {
 protected:
  HttpClientRetryExecutionTest() {
    timing_.now = [&] { return std::chrono::system_clock::time_point{} + elapsed_; };
    timing_.sleep = [&](std::chrono::milliseconds delay) {
      sleeps_.push_back(delay);
      elapsed_ += delay;
    };
  }

  static detail::Attempt Reply(int status, const std::string& retry_after = {}) {
    cpr::Response response;
    response.status_code = status;
    if (!retry_after.empty()) response.header["Retry-After"] = retry_after;
    return {.response = std::move(response)};
  }

  std::chrono::milliseconds elapsed_{};
  std::vector<std::chrono::milliseconds> sleeps_;
  detail::RetryTiming timing_;
};

TEST_F(HttpClientRetryExecutionTest, UnkeyedRequestsHonorRetryAfterWithoutCap) {
  const std::pair<const char*, std::chrono::milliseconds> cases[] = {
      {"3600", std::chrono::hours(1)},
      {"9999999999", std::chrono::milliseconds(9999999999000LL)},
      {"9223372036854775807", std::chrono::milliseconds::max()},
      {"Thu, 01 Jan 1970 01:00:00 GMT", std::chrono::hours(1)}};
  // Record even the largest durations without advancing a nanosecond clock
  // beyond its representable range or actually sleeping.
  timing_.sleep = [&](std::chrono::milliseconds delay) { sleeps_.push_back(delay); };
  for (const auto& [value, delay] : cases) {
    SCOPED_TRACE(value);
    sleeps_.clear();
    int attempts = 0;
    ICEBERG_UNWRAP_OR_FAIL(auto response,
                           detail::SendWithRetry(
                               {}, {},
                               [&](int64_t, auto) -> Result<detail::Attempt> {
                                 return Reply(++attempts == 1 ? 429 : 200, value);
                               },
                               timing_));
    EXPECT_EQ(response.status_code, 200);
    EXPECT_EQ(attempts, 2);
    EXPECT_THAT(sleeps_, ::testing::ElementsAre(delay));
  }
}

TEST_F(HttpClientRetryExecutionTest, KeyLifetimeRejectsLongRetryAfterBeforeWaiting) {
  const HttpRequest request{.method = HttpMethod::kPost,
                            .headers = {{"Idempotency-Key", "key"}}};
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto response, detail::SendWithRetry(
                         {.idempotency_key_lifetime = std::chrono::seconds(1)}, request,
                         [&](int64_t, auto) -> Result<detail::Attempt> {
                           ++attempts;
                           auto attempt = Reply(429, "3600");
                           attempt.response.text = "server error";
                           return attempt;
                         },
                         timing_));
  EXPECT_EQ(response.status_code, 429);
  EXPECT_EQ(response.text, "server error");
  EXPECT_EQ(attempts, 1);
  EXPECT_TRUE(sleeps_.empty());
}

TEST_F(HttpClientRetryExecutionTest, KeyLifetimeIncludesAttemptsAndAllPriorWaits) {
  const HttpRequest request{.headers = {{"Idempotency-Key", "key"}}};
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto response,
      detail::SendWithRetry(
          {.max_retries = 10,
           .idempotency_key_lifetime = std::chrono::milliseconds(2500)},
          request,
          [&](int64_t count, auto first_attempt) -> Result<detail::Attempt> {
            EXPECT_EQ(count, ++attempts);
            EXPECT_EQ(first_attempt, std::chrono::system_clock::time_point{});
            elapsed_ += std::chrono::milliseconds(100);
            return Reply(503, "1");
          },
          timing_));
  EXPECT_EQ(response.status_code, 503);
  EXPECT_EQ(attempts, 3);
  EXPECT_THAT(sleeps_,
              ::testing::ElementsAre(std::chrono::seconds(1), std::chrono::seconds(1)));
}

TEST_F(HttpClientRetryExecutionTest, RespectsRetryCountWithBackoffAndJitter) {
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         detail::SendWithRetry(
                             {.max_retries = 2}, {},
                             [&](int64_t, auto) -> Result<detail::Attempt> {
                               ++attempts;
                               return Reply(500);
                             },
                             timing_));
  EXPECT_EQ(response.status_code, 500);
  EXPECT_EQ(attempts, 3);
  ASSERT_EQ(sleeps_.size(), 2);
  EXPECT_GE(sleeps_[0], std::chrono::milliseconds(1000));
  EXPECT_LT(sleeps_[0], std::chrono::milliseconds(1100));
  EXPECT_GE(sleeps_[1], std::chrono::milliseconds(2000));
  EXPECT_LT(sleeps_[1], std::chrono::milliseconds(2200));
}

TEST_F(HttpClientRetryExecutionTest, IoRetriesHaveNoWait) {
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(auto response,
                         detail::SendWithRetry(
                             {.max_retries = 2}, {},
                             [&](int64_t, auto) -> Result<detail::Attempt> {
                               if (++attempts == 3) return Reply(200);
                               auto attempt = Reply(0);
                               attempt.response.error.code = cpr::ErrorCode::RECV_ERROR;
                               return attempt;
                             },
                             timing_));
  EXPECT_EQ(response.status_code, 200);
  EXPECT_EQ(attempts, 3);
  EXPECT_TRUE(sleeps_.empty());
}

TEST_F(HttpClientRetryExecutionTest, ApprovedRetryStillRunsAfterSleepOvershootsLifetime) {
  const HttpRequest request{.headers = {{"Idempotency-Key", "key"}}};
  timing_.sleep = [&](std::chrono::milliseconds delay) {
    sleeps_.push_back(delay);
    elapsed_ += std::chrono::seconds(2);
  };
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto response, detail::SendWithRetry(
                         {.idempotency_key_lifetime = std::chrono::seconds(2)}, request,
                         [&](int64_t, auto) -> Result<detail::Attempt> {
                           return Reply(++attempts == 1 ? 429 : 200, "1");
                         },
                         timing_));
  // Java checks the lifetime when approving a retry, not again after sleeping.
  EXPECT_EQ(response.status_code, 200);
  EXPECT_EQ(attempts, 2);
  EXPECT_THAT(sleeps_, ::testing::ElementsAre(std::chrono::seconds(1)));
}

TEST_F(HttpClientRetryExecutionTest, PreservesRetryApprovedAtResponseHeaders) {
  const HttpRequest request{.headers = {{"Idempotency-Key", "key"}}};
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto response, detail::SendWithRetry(
                         {.idempotency_key_lifetime = std::chrono::seconds(2)}, request,
                         [&](int64_t, auto) -> Result<detail::Attempt> {
                           if (++attempts == 2) return Reply(200);
                           auto attempt = Reply(429, "1");
                           // The retry was approved before closing the current response
                           // took time.
                           attempt.retry_approved = true;
                           elapsed_ += std::chrono::milliseconds(1500);
                           return attempt;
                         },
                         timing_));
  EXPECT_EQ(response.status_code, 200);
  EXPECT_EQ(attempts, 2);
  EXPECT_THAT(sleeps_, ::testing::ElementsAre(std::chrono::seconds(1)));
}

TEST_F(HttpClientRetryExecutionTest, KeyLifetimeAccountsForLargestPossibleJitter) {
  const HttpRequest request{.headers = {{"Idempotency-Key", "key"}}};
  for (const int64_t lifetime : {1050, 1101}) {
    SCOPED_TRACE(lifetime);
    elapsed_ = std::chrono::milliseconds::zero();
    sleeps_.clear();
    int attempts = 0;
    ICEBERG_UNWRAP_OR_FAIL(
        auto response,
        detail::SendWithRetry(
            {.idempotency_key_lifetime = std::chrono::milliseconds(lifetime)}, request,
            [&](int64_t, auto) -> Result<detail::Attempt> {
              return Reply(++attempts == 1 ? 500 : 200);
            },
            timing_));
    EXPECT_EQ(response.status_code, lifetime == 1050 ? 500 : 200);
    EXPECT_EQ(attempts, lifetime == 1050 ? 1 : 2);
    EXPECT_EQ(sleeps_.size(), lifetime == 1050 ? 0 : 1);
  }
}

TEST_F(HttpClientRetryExecutionTest, IoFailureCannotRetryAfterKeyLifetimeExpires) {
  const HttpRequest request{.headers = {{"Idempotency-Key", "key"}}};
  int attempts = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto response, detail::SendWithRetry(
                         {.idempotency_key_lifetime = std::chrono::seconds(1)}, request,
                         [&](int64_t, auto) -> Result<detail::Attempt> {
                           ++attempts;
                           elapsed_ += std::chrono::seconds(1);
                           auto attempt = Reply(0);
                           attempt.response.error.code = cpr::ErrorCode::RECV_ERROR;
                           return attempt;
                         },
                         timing_));
  EXPECT_EQ(response.error.code, cpr::ErrorCode::RECV_ERROR);
  EXPECT_EQ(attempts, 1);
  EXPECT_TRUE(sleeps_.empty());
}

TEST(HttpClientOptionsTest, SocketTimeoutAcceptsMaximumMilliseconds) {
  ICEBERG_UNWRAP_OR_FAIL(auto options,
                         HttpClientOptions::FromProperties(
                             {{"rest.client.socket-timeout-ms", "2147483647"}}));
  ASSERT_TRUE(options.socket_timeout_ms.has_value());
  const auto timeout = std::chrono::milliseconds(*options.socket_timeout_ms);
  detail::SocketIdleTimeout idle(timeout);
  const auto start = detail::SocketIdleTimeout::Clock::time_point{};
  EXPECT_TRUE(idle.Check(0, 0, 0, start));
  EXPECT_TRUE(idle.Check(0, 0, 0, start + timeout - std::chrono::milliseconds(1)));
  EXPECT_FALSE(idle.Check(0, 0, 0, start + timeout));
}

TEST(HttpClientOptionsTest, SocketTimeoutMeasuresGapsRatherThanAverageSpeed) {
  const auto timeout = std::chrono::seconds(10);
  detail::SocketIdleTimeout idle(timeout);
  const auto start = detail::SocketIdleTimeout::Clock::time_point{};
  for (int bytes = 0; bytes < 10; ++bytes) {
    EXPECT_TRUE(idle.Check(bytes, 0, 0, start + std::chrono::seconds(2 * bytes)));
  }
  // Download, upload and response-header progress each restart the idle timer.
  EXPECT_TRUE(idle.Check(9, 1, 0, start + std::chrono::seconds(27)));
  EXPECT_TRUE(idle.Check(9, 1, 1, start + std::chrono::seconds(36)));
  EXPECT_EQ(idle.Remaining(start + std::chrono::seconds(40)), std::chrono::seconds(6));
  EXPECT_FALSE(idle.Check(9, 1, 1, start + std::chrono::seconds(46)));
  EXPECT_EQ(idle.Remaining(start + std::chrono::seconds(46)),
            std::chrono::milliseconds::zero());
  idle.Reset();
  EXPECT_FALSE(idle.Remaining().has_value());
  EXPECT_TRUE(idle.Check(0, 0, 0, start + std::chrono::seconds(60)));
}

TEST(HttpClientOptionsTest, PreservesLegacyBraceInitializedConstructors) {
  HttpClient default_client;
  HttpClient headers_client({});
  HttpClient connection_timeout_client({}, {});
  HttpClient total_timeout_client({}, {}, {});
}

TEST(HttpClientOptionsTest, DefaultsMatchJavaClient) {
  ICEBERG_UNWRAP_OR_FAIL(auto options, HttpClientOptions::FromProperties({}));
  EXPECT_EQ(options.connection_timeout_ms, 180000);
  EXPECT_FALSE(options.socket_timeout_ms.has_value());
  EXPECT_EQ(options.max_retries, 5);
  EXPECT_FALSE(options.user_agent.has_value());
  EXPECT_FALSE(options.idempotency_key_lifetime.has_value());
  const RestCatalogProperties properties;
  EXPECT_EQ(options.connection_timeout_ms,
            properties.Get(RestCatalogProperties::kClientConnectionTimeoutMs));
}

TEST(HttpClientOptionsTest, AppliesTransportProperties) {
  ICEBERG_UNWRAP_OR_FAIL(auto options,
                         HttpClientOptions::FromProperties(
                             {{"rest.client.connection-timeout-ms", "17"},
                              {"rest.client.socket-timeout-ms", "29"},
                              {"rest.client.max-retries", "2"},
                              {"rest.client.user-agent", "custom/1.0"},
                              {"rest.client.idempotency-key-lifetime", "PT30M"}}));
  EXPECT_EQ(options.connection_timeout_ms, 17);
  EXPECT_EQ(options.socket_timeout_ms, 29);
  EXPECT_EQ(options.max_retries, 2);
  EXPECT_EQ(options.user_agent, "custom/1.0");
  EXPECT_EQ(options.idempotency_key_lifetime, std::chrono::minutes(30));
}

TEST(HttpClientOptionsTest, TimeoutsAcceptZeroAndExplicitPlusSigns) {
  ICEBERG_UNWRAP_OR_FAIL(auto zero, HttpClientOptions::FromProperties(
                                        {{"rest.client.connection-timeout-ms", "0"},
                                         {"rest.client.socket-timeout-ms", "0"}}));
  EXPECT_EQ(zero.connection_timeout_ms, 0);
  EXPECT_EQ(zero.socket_timeout_ms, 0);
  ICEBERG_UNWRAP_OR_FAIL(auto positive, HttpClientOptions::FromProperties(
                                            {{"rest.client.connection-timeout-ms", "+17"},
                                             {"rest.client.socket-timeout-ms", "+29"},
                                             {"rest.client.max-retries", "+2"}}));
  EXPECT_EQ(positive.connection_timeout_ms, 17);
  EXPECT_EQ(positive.socket_timeout_ms, 29);
  EXPECT_EQ(positive.max_retries, 2);
}

TEST(HttpClientOptionsTest, RejectsInvalidSettingsWithoutEchoingValues) {
  for (auto key : {"rest.client.connection-timeout-ms", "rest.client.socket-timeout-ms",
                   "rest.client.max-retries"}) {
    for (auto value :
         {"-1", "9999999999999999999999", "secret-response", "1.5", "", "+", "+-0"}) {
      auto result = HttpClientOptions::FromProperties({{key, value}});
      ASSERT_FALSE(result) << key << "=" << value;
      EXPECT_NE(result.error().message.find(key), std::string::npos);
      EXPECT_EQ(result.error().message.find("secret-response"), std::string::npos);
    }
  }
  EXPECT_FALSE(HttpClientOptions::FromProperties({{"rest.client.max-retries", "0"}}));
}

TEST(HttpClientOptionsTest, ParsesJavaDurationSyntaxForKeyLifetime) {
  const std::pair<const char*, int64_t> cases[] = {
      {"PT30M", 1800000},
      {"PT0S", 0},
      {"PT-5M", 0},
      {"-PT5M", 0},
      {"P2D", 172800000},
      {"P2DT3H4M", 183840000},
      {"PT20.345S", 20345},
      {"pt20,345s", 20345},
      {"PT0.000000001S", 0},
      {"PT-0.5S", 0},
      {"-PT-6H+3M", 21420000},
      {"+PT+1S", 1000},
      {"PT1.S", 1000},
      {"PT9223372036854775.807S", std::numeric_limits<int64_t>::max()},
  };
  for (const auto& [value, millis] : cases) {
    SCOPED_TRACE(value);
    ICEBERG_UNWRAP_OR_FAIL(auto options,
                           HttpClientOptions::FromProperties(
                               {{"rest.client.idempotency-key-lifetime", value}}));
    EXPECT_EQ(options.idempotency_key_lifetime, std::chrono::milliseconds(millis));
  }
}

TEST(HttpClientOptionsTest, IgnoresMalformedOrOutOfRangeKeyLifetimeLikeJava) {
  for (const auto* value :
       {"PT9999999999999H", "PT99999999999999999999H", "garbage", "30m", "", "P", "PT",
        "P1DT", "PT.5S", "PT1.1234567890S", "PT9223372036854775.807000001S"}) {
    SCOPED_TRACE(value);
    ICEBERG_UNWRAP_OR_FAIL(auto options,
                           HttpClientOptions::FromProperties(
                               {{"rest.client.idempotency-key-lifetime", value}}));
    EXPECT_FALSE(options.idempotency_key_lifetime.has_value());
  }
}

}  // namespace iceberg::rest
