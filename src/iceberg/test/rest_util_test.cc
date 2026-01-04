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

#include "iceberg/catalog/rest/rest_util.h"

#include <unistd.h>

#include <array>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/rest_catalog.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"

namespace iceberg::rest {

namespace {

class StubHttpServer {
 public:
  explicit StubHttpServer(std::string config_json)
      : config_json_(std::move(config_json)) {}

  ~StubHttpServer() { Join(); }

  void Start() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(listen_fd_, 0);

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);  // ephemeral port
    ASSERT_EQ(bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
    ASSERT_EQ(listen(listen_fd_, 16), 0);

    socklen_t len = sizeof(addr);
    ASSERT_EQ(getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len), 0);
    port_ = ntohs(addr.sin_port);

    server_thread_ = std::thread([this]() { ServeTwoRequests(); });
  }

  void Join() {
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
    if (listen_fd_ >= 0) {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
    }
  }

  uint16_t port() const { return port_; }

  const std::string& first_path() const { return first_path_; }
  const std::string& second_path() const { return second_path_; }

 private:
  static std::string buildJsonResponse(const std::string& body) {
    std::string resp = "HTTP/1.1 200 OK\r\n";
    resp += "Content-Type: application/json\r\n";
    resp += "Connection: close\r\n";
    resp += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    resp += "\r\n";
    resp += body;
    return resp;
  }

  static std::string extractPathFromRequest(const std::string& req) {
    auto line_end = req.find("\r\n");
    std::string first_line =
        (line_end == std::string::npos) ? req : req.substr(0, line_end);
    auto sp1 = first_line.find(' ');
    auto sp2 =
        (sp1 == std::string::npos) ? std::string::npos : first_line.find(' ', sp1 + 1);
    if (sp1 != std::string::npos && sp2 != std::string::npos && sp2 > sp1 + 1) {
      return first_line.substr(sp1 + 1, sp2 - (sp1 + 1));
    }
    return {};
  }

  void ServeOneRequest(std::string& out_path) {
    sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);
    int fd = accept(listen_fd_, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
    ASSERT_GE(fd, 0);

    std::string req;
    std::array<char, 4096> buf{};
    ssize_t n = read(fd, buf.data(), buf.size());
    if (n > 0) {
      req.assign(buf.data(), static_cast<size_t>(n));
    }

    out_path = extractPathFromRequest(req);

    std::string resp;
    if (out_path == "/v1/config") {
      resp = buildJsonResponse(config_json_);
    } else {
      resp = buildJsonResponse(R"({"namespaces":[]})");
    }

    (void)write(fd, resp.data(), resp.size());
    shutdown(fd, SHUT_RDWR);
    close(fd);
  }

  void ServeTwoRequests() {
    ServeOneRequest(first_path_);
    ServeOneRequest(second_path_);
  }

  std::string config_json_;
  int listen_fd_ = -1;
  uint16_t port_ = 0;
  std::thread server_thread_;

  std::string first_path_;
  std::string second_path_;
};

}  // namespace

TEST(RestUtilTest, TrimTrailingSlash) {
  EXPECT_EQ(TrimTrailingSlash("https://foo"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo/"), "https://foo");
  EXPECT_EQ(TrimTrailingSlash("https://foo////"), "https://foo");
}

TEST(RestUtilTest, RoundTripUrlEncodeDecodeNamespace) {
  // {"dogs"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs"}}),
              HasValue(::testing::Eq("dogs")));
  EXPECT_THAT(DecodeNamespace("dogs"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs"}})));

  // {"dogs.named.hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs.named.hank"}}),
              HasValue(::testing::Eq("dogs.named.hank")));
  EXPECT_THAT(DecodeNamespace("dogs.named.hank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs.named.hank"}})));

  // {"dogs/named/hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs/named/hank"}}),
              HasValue(::testing::Eq("dogs%2Fnamed%2Fhank")));
  EXPECT_THAT(DecodeNamespace("dogs%2Fnamed%2Fhank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs/named/hank"}})));

  // {"dogs", "named", "hank"}
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {"dogs", "named", "hank"}}),
              HasValue(::testing::Eq("dogs%1Fnamed%1Fhank")));
  EXPECT_THAT(DecodeNamespace("dogs%1Fnamed%1Fhank"),
              HasValue(::testing::Eq(Namespace{.levels = {"dogs", "named", "hank"}})));

  // {"dogs.and.cats", "named", "hank.or.james-westfall"}
  EXPECT_THAT(EncodeNamespace(Namespace{
                  .levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}}),
              HasValue(::testing::Eq("dogs.and.cats%1Fnamed%1Fhank.or.james-westfall")));
  EXPECT_THAT(DecodeNamespace("dogs.and.cats%1Fnamed%1Fhank.or.james-westfall"),
              HasValue(::testing::Eq(Namespace{
                  .levels = {"dogs.and.cats", "named", "hank.or.james-westfall"}})));

  // empty namespace
  EXPECT_THAT(EncodeNamespace(Namespace{.levels = {}}), HasValue(::testing::Eq("")));
  EXPECT_THAT(DecodeNamespace(""), HasValue(::testing::Eq(Namespace{.levels = {}})));
}

TEST(RestUtilTest, EncodeString) {
  // RFC 3986 unreserved characters should not be encoded
  EXPECT_THAT(EncodeString("abc123XYZ"), HasValue(::testing::Eq("abc123XYZ")));
  EXPECT_THAT(EncodeString("test-file_name.txt~backup"),
              HasValue(::testing::Eq("test-file_name.txt~backup")));

  // Spaces and special characters should be encoded
  EXPECT_THAT(EncodeString("hello world"), HasValue(::testing::Eq("hello%20world")));
  EXPECT_THAT(EncodeString("test@example.com"),
              HasValue(::testing::Eq("test%40example.com")));
  EXPECT_THAT(EncodeString("path/to/file"), HasValue(::testing::Eq("path%2Fto%2Ffile")));
  EXPECT_THAT(EncodeString("key=value&foo=bar"),
              HasValue(::testing::Eq("key%3Dvalue%26foo%3Dbar")));
  EXPECT_THAT(EncodeString("100%"), HasValue(::testing::Eq("100%25")));
  EXPECT_THAT(EncodeString("hello\x1Fworld"), HasValue(::testing::Eq("hello%1Fworld")));
  EXPECT_THAT(EncodeString(""), HasValue(::testing::Eq("")));
}

TEST(RestUtilTest, DecodeString) {
  // Decode percent-encoded strings
  EXPECT_THAT(DecodeString("hello%20world"), HasValue(::testing::Eq("hello world")));
  EXPECT_THAT(DecodeString("test%40example.com"),
              HasValue(::testing::Eq("test@example.com")));
  EXPECT_THAT(DecodeString("path%2Fto%2Ffile"), HasValue(::testing::Eq("path/to/file")));
  EXPECT_THAT(DecodeString("key%3Dvalue%26foo%3Dbar"),
              HasValue(::testing::Eq("key=value&foo=bar")));
  EXPECT_THAT(DecodeString("100%25"), HasValue(::testing::Eq("100%")));

  // ASCII Unit Separator (0x1F)
  EXPECT_THAT(DecodeString("hello%1Fworld"), HasValue(::testing::Eq("hello\x1Fworld")));

  // Unreserved characters remain unchanged
  EXPECT_THAT(DecodeString("test-file_name.txt~backup"),
              HasValue(::testing::Eq("test-file_name.txt~backup")));
  EXPECT_THAT(DecodeString(""), HasValue(::testing::Eq("")));
}

TEST(RestUtilTest, EncodeDecodeStringRoundTrip) {
  std::vector<std::string> test_cases = {"hello world",
                                         "test@example.com",
                                         "path/to/file",
                                         "key=value&foo=bar",
                                         "100%",
                                         "hello\x1Fworld",
                                         "special!@#$%^&*()chars",
                                         "mixed-123_test.file~ok",
                                         ""};

  for (const auto& test : test_cases) {
    ICEBERG_UNWRAP_OR_FAIL(std::string encoded, EncodeString(test));
    ICEBERG_UNWRAP_OR_FAIL(std::string decoded, DecodeString(encoded));
    EXPECT_EQ(decoded, test) << "Round-trip failed for: " << test;
  }
}

TEST(RestUtilTest, MergeConfigs) {
  std::unordered_map<std::string, std::string> server_defaults = {
      {"default1", "value1"}, {"default2", "value2"}, {"common", "default_value"}};

  std::unordered_map<std::string, std::string> client_configs = {
      {"client1", "value1"}, {"common", "client_value"}, {"extra", "client_value"}};

  std::unordered_map<std::string, std::string> server_overrides = {
      {"override1", "value1"}, {"common", "override_value"}};

  auto merged = MergeConfigs(server_defaults, client_configs, server_overrides);

  EXPECT_EQ(merged.size(), 6);

  // Check precedence: server_overrides > client_configs > server_defaults
  EXPECT_EQ(merged["default1"], "value1");
  EXPECT_EQ(merged["default2"], "value2");
  EXPECT_EQ(merged["client1"], "value1");
  EXPECT_EQ(merged["override1"], "value1");
  EXPECT_EQ(merged["common"], "override_value");
  EXPECT_EQ(merged["extra"], "client_value");

  // Test with empty maps
  auto merged_empty = MergeConfigs({}, {{"key", "value"}}, {});
  EXPECT_EQ(merged_empty.size(), 1);
  EXPECT_EQ(merged_empty["key"], "value");
}

TEST(RestUtilTest, PrefixIsTakenFromFinalConfig) {
  StubHttpServer server(R"({"defaults":{},"overrides":{"prefix":"serverp"}})");
  server.Start();

  auto config = RestCatalogProperties::default_properties();
  config
      ->Set(RestCatalogProperties::kUri,
            std::string("http://127.0.0.1:") + std::to_string(server.port()))
      .Set(RestCatalogProperties::kName, std::string("test_catalog"))
      .Set(RestCatalogProperties::kWarehouse, std::string("wh"))
      .Set(RestCatalogProperties::kPrefix, std::string("clientp"));

  auto catalog_result = RestCatalog::Make(*config, std::make_shared<test::StdFileIO>());
  ASSERT_THAT(catalog_result, IsOk());
  auto& catalog = catalog_result.value();

  Namespace root{.levels = {}};
  auto list_result = catalog->ListNamespaces(root);
  ASSERT_THAT(list_result, IsOk());

  server.Join();
  EXPECT_EQ(server.first_path(), "/v1/config");
  EXPECT_EQ(server.second_path(), "/v1/serverp/namespaces");
}

}  // namespace iceberg::rest
