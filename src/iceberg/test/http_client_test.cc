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
#include <chrono>
#include <cstdint>
#include <format>
#include <string>
#include <string_view>
#include <thread>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#ifndef _WIN32
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

}  // namespace iceberg::rest
