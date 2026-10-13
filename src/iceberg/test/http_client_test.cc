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

#include <chrono>
#include <format>
#include <string>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#ifndef _WIN32
#  include <unistd.h>

#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#endif

#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

#ifndef _WIN32

TEST(HttpClientTest, GetFailsOnceItsTimeoutPasses) {
  // A listening socket that never accepts: the kernel completes the connection,
  // but no response ever comes.
  const int server = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(server, 0);
  struct Closer {
    int fd;
    ~Closer() { ::close(fd); }
  } closer{server};
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ASSERT_EQ(::bind(server, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(server, 1), 0);
  socklen_t length = sizeof(address);
  ASSERT_EQ(::getsockname(server, reinterpret_cast<sockaddr*>(&address), &length), 0);
  const auto url = std::format("http://127.0.0.1:{}/v1/config", ntohs(address.sin_port));

  HttpClient client;
  auto session = auth::AuthSession::MakeDefault({});
  const auto start = std::chrono::steady_clock::now();
  auto response =
      client.Get(url, /*params=*/{}, /*headers=*/{}, *DefaultErrorHandler::Instance(),
                 *session, std::chrono::milliseconds(200));
  EXPECT_THAT(response, IsError(ErrorKind::kIOError));
  EXPECT_THAT(response, HasErrorMessage("timed out"));
  EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(10));
}

#endif

}  // namespace iceberg::rest
