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

#include <gtest/gtest.h>

#ifndef _WIN32

#  include <unistd.h>

#  include <array>
#  include <atomic>
#  include <memory>
#  include <mutex>
#  include <string>
#  include <thread>
#  include <unordered_map>
#  include <vector>

#  include <netinet/in.h>
#  include <sys/socket.h>

#  include "iceberg/catalog/rest/auth/auth_manager.h"
#  include "iceberg/catalog/rest/auth/auth_managers.h"
#  include "iceberg/catalog/rest/auth/auth_properties.h"
#  include "iceberg/catalog/rest/auth/auth_session.h"
#  include "iceberg/catalog/rest/catalog_properties.h"
#  include "iceberg/catalog/rest/rest_catalog.h"
#  include "iceberg/file_io.h"
#  include "iceberg/file_io_registry.h"
#  include "iceberg/table_identifier.h"
#  include "iceberg/table_requirement.h"
#  include "iceberg/table_update.h"
#  include "iceberg/test/matchers.h"

namespace iceberg::rest {

namespace {

constexpr std::string_view kMetadataJson =
    R"({"format-version":2,"table-uuid":"test-uuid-1234","location":"s3://bucket/test",)"
    R"("last-sequence-number":0,"last-updated-ms":0,"last-column-id":1,)"
    R"("schemas":[{"type":"struct","schema-id":1,"fields":[{"id":1,"name":"id","type":"int","required":true}]}],)"
    R"("current-schema-id":1,"partition-specs":[{"spec-id":0,"fields":[]}],"default-spec-id":0,)"
    R"("last-partition-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"default-sort-order-id":0})";

struct RecordedRequest {
  std::string method;
  std::string path;
  std::string auth_marker;
};

class MiniRestServer {
 public:
  bool Start() {
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) return false;
    int reuse = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
      return false;
    }
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    if (::listen(listen_fd_, 8) < 0) return false;
    server_thread_ = std::thread([this, fd = listen_fd_] { Loop(fd); });
    return true;
  }

  void Stop() {
    stopping_ = true;
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (server_thread_.joinable()) server_thread_.join();
  }

  int port() const { return port_; }

  std::vector<RecordedRequest> requests() {
    std::lock_guard<std::mutex> lock(mutex_);
    return requests_;
  }

 private:
  void Loop(int listen_fd) {
    while (!stopping_) {
      int fd = ::accept(listen_fd, nullptr, nullptr);
      if (fd < 0) break;
      HandleConnection(fd);
      ::close(fd);
    }
  }

  void HandleConnection(int fd) {
    std::string raw;
    std::array<char, 4096> buf{};
    size_t header_end = std::string::npos;
    while (header_end == std::string::npos) {
      ssize_t n = ::read(fd, buf.data(), buf.size());
      if (n <= 0) return;
      raw.append(buf.data(), static_cast<size_t>(n));
      header_end = raw.find("\r\n\r\n");
    }
    size_t content_length = 0;
    {
      std::string lower;
      lower.reserve(header_end);
      for (size_t i = 0; i < header_end; ++i) {
        lower.push_back(
            static_cast<char>(std::tolower(static_cast<unsigned char>(raw[i]))));
      }
      auto pos = lower.find("content-length:");
      if (pos != std::string::npos) {
        content_length = std::stoul(lower.substr(pos + 15));
      }
    }
    while (raw.size() < header_end + 4 + content_length) {
      ssize_t n = ::read(fd, buf.data(), buf.size());
      if (n <= 0) break;
      raw.append(buf.data(), static_cast<size_t>(n));
    }

    auto line_end = raw.find("\r\n");
    auto request_line = raw.substr(0, line_end);
    auto sp1 = request_line.find(' ');
    auto sp2 = request_line.find(' ', sp1 + 1);
    RecordedRequest req;
    req.method = request_line.substr(0, sp1);
    req.path = request_line.substr(sp1 + 1, sp2 - sp1 - 1);
    req.auth_marker = HeaderValue(raw.substr(0, header_end), "x-test-auth");
    {
      std::lock_guard<std::mutex> lock(mutex_);
      requests_.push_back(req);
    }

    Respond(fd, BodyFor(req));
  }

  static std::string HeaderValue(const std::string& headers, std::string_view name) {
    std::string lower;
    lower.reserve(headers.size());
    for (char c : headers) {
      lower.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(c))));
    }
    auto pos = lower.find(std::string(name) + ":");
    if (pos == std::string::npos) return "";
    auto value_start = pos + name.size() + 1;
    auto value_end = headers.find("\r\n", value_start);
    auto value = headers.substr(value_start, value_end - value_start);
    auto first = value.find_first_not_of(' ');
    return first == std::string::npos ? "" : value.substr(first);
  }

  std::string BodyFor(const RecordedRequest& req) {
    if (req.path.find("/v1/config") != std::string::npos) {
      return R"({"defaults":{},"overrides":{}})";
    }
    if (req.method == "GET" && req.path.find("/tables/") != std::string::npos) {
      return std::string(R"({"metadata-location":"s3://bucket/meta/v1.json",)") +
             R"("metadata":)" + std::string(kMetadataJson) +
             R"(,"config":{"token":"tbl-token-1"}})";
    }
    if (req.method == "POST" && req.path.find("/tables/") != std::string::npos) {
      return std::string(R"({"metadata-location":"s3://bucket/meta/v2.json",)") +
             R"("metadata":)" + std::string(kMetadataJson) + "}";
    }
    return "{}";
  }

  static void Respond(int fd, const std::string& body) {
    std::string response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n";
    response += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    response += "Connection: close\r\n\r\n";
    response += body;
    size_t sent = 0;
    while (sent < response.size()) {
      ssize_t n = ::write(fd, response.data() + sent, response.size() - sent);
      if (n <= 0) break;
      sent += static_cast<size_t>(n);
    }
  }

  int listen_fd_ = -1;
  int port_ = 0;
  std::atomic<bool> stopping_{false};
  std::thread server_thread_;
  std::mutex mutex_;
  std::vector<RecordedRequest> requests_;
};

class RecordingAuthManager : public auth::AuthManager {
 public:
  Result<std::shared_ptr<auth::AuthSession>> InitSession(
      HttpClient& /*init_client*/,
      const std::unordered_map<std::string, std::string>& /*properties*/) override {
    return auth::AuthSession::MakeDefault({{"x-test-auth", "init"}});
  }

  Result<std::shared_ptr<auth::AuthSession>> CatalogSession(
      HttpClient& /*shared_client*/,
      const std::unordered_map<std::string, std::string>& /*properties*/) override {
    return auth::AuthSession::MakeDefault({{"x-test-auth", "catalog"}});
  }

  Result<std::shared_ptr<auth::AuthSession>> TableSession(
      const TableIdentifier& /*table*/,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<auth::AuthSession> parent) override {
    auto token = properties.find("token");
    if (token == properties.end()) {
      return parent;
    }
    return auth::AuthSession::MakeDefault({{"x-test-auth", "table:" + token->second}});
  }
};

class MockFileIO : public FileIO {};

}  // namespace

TEST(RestCatalogSessionTest, RefreshAndCommitUseTableSessionFromResponseConfig) {
  MiniRestServer server;
  ASSERT_TRUE(server.Start());

  auth::AuthManagers::Register(
      "test-session-recorder",
      [](std::string_view /*name*/,
         const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<auth::AuthManager>> {
        return std::make_unique<RecordingAuthManager>();
      });
  FileIORegistry::Register(
      "test.SessionMockFileIO",
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto config = RestCatalogProperties::FromMap(
      {{"uri", "http://127.0.0.1:" + std::to_string(server.port())},
       {auth::AuthProperties::kAuthType, "test-session-recorder"},
       {"io-impl", "test.SessionMockFileIO"}});

  {
    auto catalog_result = RestCatalog::Make(config);
    ASSERT_THAT(catalog_result, IsOk());
    auto catalog = catalog_result.value();

    TableIdentifier identifier{.ns = Namespace{{"ns1"}}, .name = "tbl1"};
    ASSERT_THAT(catalog->LoadTable(identifier), IsOk());
    ASSERT_THAT(catalog->LoadTable(identifier), IsOk());
    ASSERT_THAT(catalog->UpdateTable(identifier, {}, {}), IsOk());
  }

  server.Stop();

  auto requests = server.requests();
  ASSERT_EQ(requests.size(), 4);
  EXPECT_TRUE(requests[0].path.find("/v1/config") != std::string::npos);
  EXPECT_EQ(requests[0].auth_marker, "init");
  EXPECT_EQ(requests[1].method, "GET");
  EXPECT_TRUE(requests[1].path.find("/tables/tbl1") != std::string::npos);
  EXPECT_EQ(requests[1].auth_marker, "catalog");
  EXPECT_EQ(requests[2].method, "GET");
  EXPECT_TRUE(requests[2].path.find("/tables/tbl1") != std::string::npos);
  EXPECT_EQ(requests[2].auth_marker, "table:tbl-token-1");
  EXPECT_EQ(requests[3].method, "POST");
  EXPECT_TRUE(requests[3].path.find("/tables/tbl1") != std::string::npos);
  EXPECT_EQ(requests[3].auth_marker, "table:tbl-token-1");
}

}  // namespace iceberg::rest

#else

TEST(RestCatalogSessionTest, RefreshAndCommitUseTableSessionFromResponseConfig) {
  GTEST_SKIP() << "POSIX-socket test server is not available on Windows";
}

#endif  // _WIN32
