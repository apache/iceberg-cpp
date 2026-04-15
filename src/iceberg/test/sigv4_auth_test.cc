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

#include <string>
#include <unordered_map>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <gtest/gtest.h>

#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/sigv4_auth_manager.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest::auth {

class SigV4AuthTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    static bool initialized = false;
    if (!initialized) {
      Aws::SDKOptions options;
      Aws::InitAPI(options);
      initialized = true;
    }
  }

  HttpClient client_{{}};

  std::unordered_map<std::string, std::string> MakeSigV4Properties() {
    return {
        {AuthProperties::kAuthType, "sigv4"},
        {AuthProperties::kSigV4SigningRegion, "us-east-1"},
        {AuthProperties::kSigV4SigningName, "execute-api"},
        {AuthProperties::kSigV4AccessKeyId, "AKIAIOSFODNN7EXAMPLE"},
        {AuthProperties::kSigV4SecretAccessKey,
         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
    };
  }
};

TEST_F(SigV4AuthTest, LoadSigV4AuthManager) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());
}

TEST_F(SigV4AuthTest, CatalogSessionProducesSession) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());
}

TEST_F(SigV4AuthTest, AuthenticateAddsAuthorizationHeader) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_NE(headers.find("authorization"), headers.end());
  EXPECT_TRUE(headers.at("authorization").starts_with("AWS4-HMAC-SHA256"));
  EXPECT_NE(headers.find("x-amz-date"), headers.end());
}

TEST_F(SigV4AuthTest, AuthenticateWithPostBody) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kPost,
                      .url = "https://example.com/v1/namespaces",
                      .headers = {{"Content-Type", "application/json"}},
                      .body = R"({"namespace":["ns1"]})"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_NE(headers.find("authorization"), headers.end());
  EXPECT_TRUE(headers.at("authorization").starts_with("AWS4-HMAC-SHA256"));
}

TEST_F(SigV4AuthTest, DelegateAuthorizationHeaderRelocated) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kToken.key()] = "my-oauth-token";
  properties[AuthProperties::kSigV4DelegateAuthType] = "oauth2";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_NE(headers.find("authorization"), headers.end());
  EXPECT_TRUE(headers.at("authorization").starts_with("AWS4-HMAC-SHA256"));
  EXPECT_NE(headers.find("original-authorization"), headers.end());
  EXPECT_EQ(headers.at("original-authorization"), "Bearer my-oauth-token");
}

TEST_F(SigV4AuthTest, AuthenticateWithSessionToken) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SessionToken] = "FwoGZXIvYXdzEBYaDHqa0";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_NE(headers.find("authorization"), headers.end());
  EXPECT_NE(headers.find("x-amz-security-token"), headers.end());
  EXPECT_EQ(headers.at("x-amz-security-token"), "FwoGZXIvYXdzEBYaDHqa0");
}

TEST_F(SigV4AuthTest, CustomSigningNameAndRegion) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "eu-west-1";
  properties[AuthProperties::kSigV4SigningName] = "custom-service";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());
  EXPECT_TRUE(auth_it->second.find("eu-west-1") != std::string::npos);
  EXPECT_TRUE(auth_it->second.find("custom-service") != std::string::npos);
}

TEST_F(SigV4AuthTest, AuthTypeCaseInsensitive) {
  for (const auto& auth_type : {"SIGV4", "SigV4", "sigV4"}) {
    auto properties = MakeSigV4Properties();
    properties[AuthProperties::kAuthType] = auth_type;
    EXPECT_THAT(AuthManagers::Load("test-catalog", properties), IsOk())
        << "Failed for auth type: " << auth_type;
  }
}

TEST_F(SigV4AuthTest, DelegateDefaultsToOAuth2NoAuth) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_EQ(headers.find("original-authorization"), headers.end());
}

TEST_F(SigV4AuthTest, TableSessionInheritsProperties) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto catalog_session = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(catalog_session, IsOk());

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"ns1"}}, .name = "table1"};
  std::unordered_map<std::string, std::string> table_props;
  auto table_session = manager_result.value()->TableSession(table_id, table_props,
                                                            catalog_session.value());
  ASSERT_THAT(table_session, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet,
                      .url = "https://example.com/v1/ns1/tables/table1"};
  auto auth_result = table_session.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_NE(auth_result.value().headers.find("authorization"),
            auth_result.value().headers.end());
}

// ---------- Tests ported from Java TestRESTSigV4AuthSession ----------

// Java: authenticateWithoutBody
TEST_F(SigV4AuthTest, AuthenticateWithoutBodyDetailedHeaders) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet,
                      .url = "http://localhost:8080/path",
                      .headers = {{"Content-Type", "application/json"}}};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  // Original header preserved
  EXPECT_EQ(headers.at("content-type"), "application/json");

  // Host header generated by the signer
  EXPECT_NE(headers.find("host"), headers.end());

  // SigV4 headers
  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());
  EXPECT_TRUE(auth_it->second.starts_with("AWS4-HMAC-SHA256 Credential="));

  EXPECT_TRUE(auth_it->second.find("content-type") != std::string::npos);
  EXPECT_TRUE(auth_it->second.find("host") != std::string::npos);
  EXPECT_TRUE(auth_it->second.find("x-amz-content-sha256") != std::string::npos);
  EXPECT_TRUE(auth_it->second.find("x-amz-date") != std::string::npos);

  // Empty body SHA256 hash
  EXPECT_EQ(headers.at("x-amz-content-sha256"), SigV4AuthSession::kEmptyBodySha256);

  // X-Amz-Date present
  EXPECT_NE(headers.find("x-amz-date"), headers.end());
}

// Java: authenticateWithBody
TEST_F(SigV4AuthTest, AuthenticateWithBodyDetailedHeaders) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kPost,
                      .url = "http://localhost:8080/path",
                      .headers = {{"Content-Type", "application/json"}},
                      .body = R"({"namespace":["ns1"]})"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  // SigV4 Authorization header
  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());
  EXPECT_TRUE(auth_it->second.starts_with("AWS4-HMAC-SHA256 Credential="));

  // x-amz-content-sha256 should be Base64-encoded body SHA256 (matching Java)
  auto sha_it = headers.find("x-amz-content-sha256");
  ASSERT_NE(sha_it, headers.end());
  EXPECT_NE(sha_it->second, SigV4AuthSession::kEmptyBodySha256);

  EXPECT_EQ(sha_it->second.size(), 44)
      << "Expected Base64 SHA256, got: " << sha_it->second;
}

// Java: authenticateConflictingAuthorizationHeader
TEST_F(SigV4AuthTest, ConflictingAuthorizationHeaderIncludedInSignedHeaders) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kToken.key()] = "my-oauth-token";
  properties[AuthProperties::kSigV4DelegateAuthType] = "oauth2";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet,
                      .url = "http://localhost:8080/path",
                      .headers = {{"Content-Type", "application/json"}}};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  // SigV4 Authorization header
  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());
  EXPECT_TRUE(auth_it->second.starts_with("AWS4-HMAC-SHA256 Credential="));

  // Relocated delegate header should be in SignedHeaders
  EXPECT_TRUE(auth_it->second.find("original-authorization") != std::string::npos)
      << "SignedHeaders should include 'original-authorization', got: "
      << auth_it->second;

  // Relocated Authorization present
  auto orig_it = headers.find("original-authorization");
  ASSERT_NE(orig_it, headers.end());
  EXPECT_EQ(orig_it->second, "Bearer my-oauth-token");
}

// Java: authenticateConflictingSigv4Headers
TEST_F(SigV4AuthTest, ConflictingSigV4HeadersRelocated) {
  auto delegate = AuthSession::MakeDefault({
      {"x-amz-content-sha256", "fake-sha256"},
      {"X-Amz-Date", "fake-date"},
      {"Content-Type", "application/json"},
  });
  auto credentials =
      std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(Aws::Auth::AWSCredentials(
          "AKIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"));
  auto session = std::make_shared<SigV4AuthSession>(
      delegate, "us-east-1", "execute-api", credentials,
      std::unordered_map<std::string, std::string>{});

  HTTPRequest request{.method = HttpMethod::kGet, .url = "http://localhost:8080/path"};
  auto auth_result = session->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  // The real x-amz-content-sha256 should be the empty body hash (signer overwrites fake)
  EXPECT_EQ(headers.at("x-amz-content-sha256"), SigV4AuthSession::kEmptyBodySha256);

  // The fake values should be relocated since the signer produced different values
  auto orig_sha_it = headers.find("Original-x-amz-content-sha256");
  ASSERT_NE(orig_sha_it, headers.end());
  EXPECT_EQ(orig_sha_it->second, "fake-sha256");

  auto orig_date_it = headers.find("Original-X-Amz-Date");
  ASSERT_NE(orig_date_it, headers.end());
  EXPECT_EQ(orig_date_it->second, "fake-date");

  // SigV4 Authorization present
  EXPECT_NE(headers.find("authorization"), headers.end());
}

// Java: close (TestRESTSigV4AuthSession)
TEST_F(SigV4AuthTest, SessionCloseDelegatesToInner) {
  auto delegate = AuthSession::MakeDefault({});
  auto credentials = std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(
      Aws::Auth::AWSCredentials("id", "secret"));
  auto session = std::make_shared<SigV4AuthSession>(
      delegate, "us-east-1", "execute-api", credentials,
      std::unordered_map<std::string, std::string>{});

  // Close should succeed without error
  EXPECT_THAT(session->Close(), IsOk());
}

// ---------- Tests ported from Java TestRESTSigV4AuthManager ----------

// Java: createCustomDelegate
TEST_F(SigV4AuthTest, CreateCustomDelegateNone) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "sigv4"},
      {AuthProperties::kSigV4DelegateAuthType, "none"},
      {AuthProperties::kSigV4SigningRegion, "us-west-2"},
      {AuthProperties::kSigV4AccessKeyId, "id"},
      {AuthProperties::kSigV4SecretAccessKey, "secret"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  // Authenticate should work with noop delegate
  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = session_result.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  EXPECT_NE(headers.find("authorization"), headers.end());
  EXPECT_EQ(headers.find("original-authorization"), headers.end());
}

// Java: createInvalidCustomDelegate
TEST_F(SigV4AuthTest, CreateInvalidCustomDelegateSigV4Circular) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "sigv4"},
      {AuthProperties::kSigV4DelegateAuthType, "sigv4"},
      {AuthProperties::kSigV4SigningRegion, "us-east-1"},
      {AuthProperties::kSigV4AccessKeyId, "id"},
      {AuthProperties::kSigV4SecretAccessKey, "secret"},
  };

  auto result = AuthManagers::Load("test-catalog", properties);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot delegate a SigV4 auth manager to another SigV4"));
}

// Java: contextualSession
TEST_F(SigV4AuthTest, ContextualSessionOverridesProperties) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto catalog_session = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(catalog_session, IsOk());

  // Context overrides region and credentials
  std::unordered_map<std::string, std::string> context = {
      {AuthProperties::kSigV4AccessKeyId, "id2"},
      {AuthProperties::kSigV4SecretAccessKey, "secret2"},
      {AuthProperties::kSigV4SigningRegion, "eu-west-1"},
  };

  auto ctx_session =
      manager_result.value()->ContextualSession(context, catalog_session.value());
  ASSERT_THAT(ctx_session, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet, .url = "https://example.com/v1/config"};
  auto auth_result = ctx_session.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());

  EXPECT_TRUE(auth_it->second.find("eu-west-1") != std::string::npos)
      << "Expected eu-west-1 in Authorization, got: " << auth_it->second;
}

// Java: tableSession (with property override)
TEST_F(SigV4AuthTest, TableSessionOverridesProperties) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto catalog_session = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(catalog_session, IsOk());

  // Table properties override region and credentials
  std::unordered_map<std::string, std::string> table_props = {
      {AuthProperties::kSigV4AccessKeyId, "table-key-id"},
      {AuthProperties::kSigV4SecretAccessKey, "table-secret"},
      {AuthProperties::kSigV4SigningRegion, "ap-southeast-1"},
  };

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"db1"}}, .name = "table1"};
  auto table_session = manager_result.value()->TableSession(table_id, table_props,
                                                            catalog_session.value());
  ASSERT_THAT(table_session, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet,
                      .url = "https://example.com/v1/db1/tables/table1"};
  auto auth_result = table_session.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());
  const auto& headers = auth_result.value().headers;

  auto auth_it = headers.find("authorization");
  ASSERT_NE(auth_it, headers.end());

  EXPECT_TRUE(auth_it->second.find("ap-southeast-1") != std::string::npos)
      << "Expected ap-southeast-1 in Authorization, got: " << auth_it->second;
}

TEST_F(SigV4AuthTest, TableSessionInheritsContextualOverrides) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto catalog_session = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(catalog_session, IsOk());

  auto ctx_session = manager_result.value()->ContextualSession(
      {{AuthProperties::kSigV4SigningRegion, "eu-west-1"}}, catalog_session.value());
  ASSERT_THAT(ctx_session, IsOk());

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"db1"}}, .name = "table1"};
  auto table_session = manager_result.value()->TableSession(table_id, /*properties=*/{},
                                                            ctx_session.value());
  ASSERT_THAT(table_session, IsOk());

  HTTPRequest request{.method = HttpMethod::kGet,
                      .url = "https://example.com/v1/db1/tables/table1"};
  auto auth_result = table_session.value()->Authenticate(request);
  ASSERT_THAT(auth_result, IsOk());

  auto auth_it = auth_result.value().headers.find("authorization");
  ASSERT_NE(auth_it, auth_result.value().headers.end());
  EXPECT_TRUE(auth_it->second.find("eu-west-1") != std::string::npos)
      << "Table session should inherit eu-west-1 from contextual parent, got: "
      << auth_it->second;
}

// Java: close (TestRESTSigV4AuthManager)
TEST_F(SigV4AuthTest, ManagerCloseDelegatesToInner) {
  auto properties = MakeSigV4Properties();
  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  // Close should succeed without error
  EXPECT_THAT(manager_result.value()->Close(), IsOk());
}

}  // namespace iceberg::rest::auth
