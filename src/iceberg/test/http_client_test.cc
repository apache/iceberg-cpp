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
#include <cstddef>
#include <format>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/test/matchers.h"

#ifndef _WIN32
#  include <poll.h>
#  include <unistd.h>

#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <sys/time.h>
#endif

namespace iceberg::rest {

namespace {

#ifndef _WIN32

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
    timeval timeout{.tv_sec = 2, .tv_usec = 0};
    setsockopt(conn, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

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

#endif  // _WIN32

}  // namespace

// A 3xx response must be surfaced to the caller as an error and must not be
// followed: following it would replay the request, including all headers, to
// a location the caller did not choose.
TEST(HttpClientTest, DoesNotFollowRedirects) {
#ifdef _WIN32
  GTEST_SKIP() << "Loopback test server is only implemented for POSIX";
#else
  LoopbackHttpServer target(
      "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
      "Content-Length: 2\r\nConnection: close\r\n\r\n{}");
  ASSERT_TRUE(target.Start());

  LoopbackHttpServer redirector(
      std::format("HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\n"
                  "Connection: close\r\n\r\n",
                  target.url("/v1/config")));
  ASSERT_TRUE(redirector.Start());

  HttpClient client;
  auto session = auth::AuthSession::MakeDefault({{"Authorization", "Bearer token"}});
  const auto& handler = *DefaultErrorHandler::Instance();
  const std::string url = redirector.url("/v1/config");

  auto expect_not_followed = [&](const Result<HttpResponse>& result,
                                 std::string_view method) {
    SCOPED_TRACE(method);
    EXPECT_THAT(result, IsError(ErrorKind::kRestError));
    EXPECT_THAT(result, HasErrorMessage("code: 302"));
    EXPECT_EQ(target.request_count(), 0);
  };

  expect_not_followed(client.Get(url, {}, {}, handler, *session), "GET");
  expect_not_followed(client.Post(url, "{}", {}, handler, *session), "POST");
  expect_not_followed(
      client.PostForm(url, {{"grant_type", "client_credentials"}}, {}, handler, *session),
      "POST form");
  expect_not_followed(client.Head(url, {}, handler, *session), "HEAD");
  expect_not_followed(client.Delete(url, {}, {}, handler, *session), "DELETE");

  EXPECT_EQ(redirector.request_count(), 5);
#endif
}

}  // namespace iceberg::rest
