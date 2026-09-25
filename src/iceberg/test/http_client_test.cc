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
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

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
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

#ifndef _WIN32

namespace {

/// \brief Minimal HTTP/1.1 server on 127.0.0.1 that answers every request with
/// a fixed response and closes the connection.
class LoopbackHttpServer {
 public:
  explicit LoopbackHttpServer(std::string response) : response_(std::move(response)) {}

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
      ReadRequest(conn);
      ++request_count_;
      SendAll(conn, response_);
      close(conn);
    }
  }

  /// \brief Consume the request headers and any Content-Length body so the
  /// client does not see a connection reset before it reads the response.
  static void ReadRequest(int conn) {
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
        return;
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
        return;
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
  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> stop_{false};
  std::atomic<int> request_count_{0};
  std::thread thread_;
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
void ServeOnce(int listener, std::string_view response, bool end) {
  // Bound every wait, so the test cannot hang.
  timeval wait{};
  wait.tv_sec = 10;
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
             .error = "timed out"},
        Case{.name = "body ends early",
             .response = kTruncated,
             .end = true,
             .error = "failed after status 200"}}) {
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

  HttpClient client;
  auto session = auth::AuthSession::MakeDefault({});
  std::thread server([&] { ServeOnce(listener.fd, response_bytes, /*end=*/true); });
  auto response =
      client.Post(url, "{}", /*headers=*/{}, *DefaultErrorHandler::Instance(), *session);
  server.join();
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("failed after status 200"));
}

// A response owns its data, so it stays readable after a later request reuses
// its connection pool and after the client is destroyed.
TEST(HttpClientTest, ResponseOutlivesLaterRequestsAndClient) {
  constexpr std::string_view kBody = R"({"defaults":{}})";
  const auto response_bytes = std::format(
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Test: kept\r\n"
      "Content-Length: {}\r\nConnection: close\r\n\r\n{}",
      kBody.size(), kBody);
  Listener listener;
  ASSERT_NO_FATAL_FAILURE(Listen(listener));
  const auto url = std::format("http://127.0.0.1:{}/v1/config", listener.port);

  auto client = std::make_unique<HttpClient>();
  auto session = auth::AuthSession::MakeDefault({});
  std::thread server([&] {
    ServeOnce(listener.fd, response_bytes, /*end=*/true);
    ServeOnce(listener.fd, response_bytes, /*end=*/true);
  });
  auto first = client->Get(url, /*params=*/{}, /*headers=*/{},
                           *DefaultErrorHandler::Instance(), *session);
  auto second = client->Get(url, /*params=*/{}, /*headers=*/{},
                            *DefaultErrorHandler::Instance(), *session);
  server.join();
  client.reset();

  auto expect_intact = [&](const Result<HttpResponse>& response) {
    ASSERT_THAT(response, IsOk());
    EXPECT_EQ(response->status_code(), 200);
    EXPECT_EQ(response->body(), kBody);
    EXPECT_THAT(response->headers(),
                ::testing::Contains(::testing::Pair("X-Test", "kept")));
  };
  expect_intact(first);
  expect_intact(second);
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
  HttpClient client({}, {}, {}, std::string("https://catalog.example/api/"));
  CapturingAuthSession session;

  auto relative = client.PostForm("v1/oauth/tokens///", {}, {},
                                  *DefaultErrorHandler::Instance(), session);
  EXPECT_THAT(relative, IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(session.request().url, "https://catalog.example/api/v1/oauth/tokens");

  auto absolute = client.PostForm("https://auth.example/token/", {}, {},
                                  *DefaultErrorHandler::Instance(), session);
  EXPECT_THAT(absolute, IsError(ErrorKind::kInvalidArgument));
  EXPECT_EQ(session.request().url, "https://auth.example/token/");
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

}  // namespace iceberg::rest
