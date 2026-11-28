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

#include "iceberg/catalog/rest/auth/auth_manager.h"

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/retry.h"
#include "iceberg/util/base64.h"

namespace iceberg::rest::auth {

namespace {

/// Helper to parse OAuthTokenResponse from a JSON string.
Result<OAuthTokenResponse> ParseTokenResponse(const std::string& str) {
  ICEBERG_ASSIGN_OR_RAISE(auto json, iceberg::FromJsonString(str));
  return iceberg::rest::FromJson<OAuthTokenResponse>(json);
}

std::string MakeJwt(const std::string& payload_json) {
  std::string header = R"({"alg":"HS256","typ":"JWT"})";
  std::string signature = "test-signature";
  return Base64::UrlEncode(header) + "." + Base64::UrlEncode(payload_json) + "." +
         Base64::UrlEncode(signature);
}

}  // namespace

class AuthManagerTest : public ::testing::Test {
 protected:
  std::shared_ptr<HttpClient> client_ = std::make_shared<HttpClient>();
};

// Verifies loading NoopAuthManager with explicit "none" auth type
TEST_F(AuthManagerTest, LoadNoopAuthManagerExplicit) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "none"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_TRUE(auth_result.value().headers.empty());
}

// Verifies that NoopAuthManager is inferred when no auth properties are set
TEST_F(AuthManagerTest, LoadNoopAuthManagerInferred) {
  auto manager_result = AuthManagers::Load("test-catalog", {});
  ASSERT_THAT(manager_result, IsOk());
}

TEST_F(AuthManagerTest, NoopContextualSessionReturnsParentSession) {
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", {}));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, {}));

  SessionContext context{
      .session_id = "tenant-a",
      .identity = "user-a",
      .credentials = {{"credential-key", "credential-value"}},
      .properties = {{"property-key", "property-value"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto contextual, manager->ContextualSession(context, parent));

  EXPECT_EQ(contextual, parent);
}

TEST_F(AuthManagerTest, HttpHeadersAreCaseInsensitiveSingleValueMap) {
  HttpHeaders headers;
  headers.emplace("Authorization", "Bearer first");
  headers.emplace("authorization", "Bearer second");

  EXPECT_EQ(headers.size(), 1);
  EXPECT_EQ(headers.at("AUTHORIZATION"), "Bearer first");
}

TEST_F(AuthManagerTest, DefaultSessionPreservesRequestAuthorizationHeader) {
  auto session = AuthSession::MakeDefault(OAuth2Util::AuthHeaders("parent-token"));

  ICEBERG_UNWRAP_OR_FAIL(
      auto authenticated,
      session->Authenticate({.headers = {{"Authorization", "Basic credentials"}}}));

  EXPECT_EQ(authenticated.headers.at("Authorization"), "Basic credentials");
  EXPECT_FALSE(session->OAuth2Info().has_value());
}

TEST_F(AuthManagerTest, OAuth2SessionPreservesRequestAuthorizationHeader) {
  OAuthTokenResponse token_response{
      .access_token = "parent-token",
      .token_type = "bearer",
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto session,
      AuthSession::MakeOAuth2(token_response, "https://auth.example.com/token", "", "",
                              "catalog", /*keep_refreshed=*/false, {}, client_));

  ICEBERG_UNWRAP_OR_FAIL(
      auto authenticated,
      session->Authenticate({.headers = {{"Authorization", "Basic credentials"}}}));

  EXPECT_EQ(authenticated.headers.at("Authorization"), "Basic credentials");

  auto info = session->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->issued_token_type, AuthProperties::kAccessTokenType);
}

TEST_F(AuthManagerTest, OAuth2SessionExposesMetadata) {
  OAuthTokenResponse token_response{
      .access_token = "parent-token",
      .token_type = "bearer",
      .issued_token_type = AuthProperties::kJwtTokenType,
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto session,
      AuthSession::MakeOAuth2(
          token_response, "https://auth.example.com/token", "client-id", "client-secret",
          "catalog", /*keep_refreshed=*/false,
          {{AuthProperties::kAudience.key(), "catalog-audience"}}, client_));

  auto info = session->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->token, "parent-token");
  EXPECT_EQ(info->issued_token_type, AuthProperties::kJwtTokenType);
  EXPECT_EQ(info->credential, "client-id:client-secret");
  EXPECT_EQ(info->scope, "catalog");
  EXPECT_EQ(info->oauth2_server_uri, "https://auth.example.com/token");
  EXPECT_EQ(info->optional_oauth_params.at(AuthProperties::kAudience.key()),
            "catalog-audience");
}

TEST_F(AuthManagerTest, HttpClientRejectsParamsWhenUrlAlreadyHasQuery) {
  auto session = AuthSession::MakeDefault({});
  auto result =
      client_->Get("http://127.0.0.1/v1/config?existing=true", {{"warehouse", "prod"}},
                   /*headers=*/{}, *rest::DefaultErrorHandler::Instance(), *session);

  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("must not contain a query string"));
}

// Verifies that auth type is case-insensitive
TEST_F(AuthManagerTest, AuthTypeCaseInsensitive) {
  for (const auto& auth_type : {"NONE", "None", "NoNe"}) {
    std::unordered_map<std::string, std::string> properties = {
        {AuthProperties::kAuthType, auth_type}};
    EXPECT_THAT(AuthManagers::Load("test-catalog", properties), IsOk())
        << "Failed for auth type: " << auth_type;
  }
}

// Verifies that unknown auth type returns InvalidArgument
TEST_F(AuthManagerTest, UnknownAuthTypeReturnsInvalidArgument) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "unknown-auth-type"}};

  auto result = AuthManagers::Load("test-catalog", properties);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Unknown authentication type"));
}

// Verifies loading BasicAuthManager with valid credentials
TEST_F(AuthManagerTest, LoadBasicAuthManager) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "basic"},
      {AuthProperties::kBasicUsername, "admin"},
      {AuthProperties::kBasicPassword, "secret"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  // base64("admin:secret") == "YWRtaW46c2VjcmV0"
  EXPECT_EQ(auth_result.value().headers["Authorization"], "Basic YWRtaW46c2VjcmV0");
}

// Verifies BasicAuthManager is case-insensitive for auth type
TEST_F(AuthManagerTest, BasicAuthTypeCaseInsensitive) {
  for (const auto& auth_type : {"BASIC", "Basic", "bAsIc"}) {
    std::unordered_map<std::string, std::string> properties = {
        {AuthProperties::kAuthType, auth_type},
        {AuthProperties::kBasicUsername, "user"},
        {AuthProperties::kBasicPassword, "pass"}};
    auto manager_result = AuthManagers::Load("test-catalog", properties);
    ASSERT_THAT(manager_result, IsOk()) << "Failed for auth type: " << auth_type;

    auto session_result = manager_result.value()->CatalogSession(client_, properties);
    ASSERT_THAT(session_result, IsOk()) << "Failed for auth type: " << auth_type;

    auto auth_result = session_result.value()->Authenticate({});
    ASSERT_THAT(auth_result, IsOk()) << "Failed for auth type: " << auth_type;
    // base64("user:pass") == "dXNlcjpwYXNz"
    EXPECT_EQ(auth_result.value().headers["Authorization"], "Basic dXNlcjpwYXNz");
  }
}

// Verifies BasicAuthManager fails when username is missing
TEST_F(AuthManagerTest, BasicAuthMissingUsername) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "basic"}, {AuthProperties::kBasicPassword, "secret"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  EXPECT_THAT(session_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(session_result, HasErrorMessage("Missing required property"));
}

// Verifies BasicAuthManager fails when password is missing
TEST_F(AuthManagerTest, BasicAuthMissingPassword) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "basic"}, {AuthProperties::kBasicUsername, "admin"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  EXPECT_THAT(session_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(session_result, HasErrorMessage("Missing required property"));
}

// Verifies BasicAuthManager handles special characters in credentials
TEST_F(AuthManagerTest, BasicAuthSpecialCharacters) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "basic"},
      {AuthProperties::kBasicUsername, "user@domain.com"},
      {AuthProperties::kBasicPassword, "p@ss:w0rd!"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  // base64("user@domain.com:p@ss:w0rd!") == "dXNlckBkb21haW4uY29tOnBAc3M6dzByZCE="
  EXPECT_EQ(auth_result.value().headers["Authorization"],
            "Basic dXNlckBkb21haW4uY29tOnBAc3M6dzByZCE=");
}

// Verifies custom auth manager registration
TEST_F(AuthManagerTest, RegisterCustomAuthManager) {
  AuthManagers::Register(
      "custom",
      []([[maybe_unused]] std::string_view name,
         [[maybe_unused]] const std::unordered_map<std::string, std::string>& props)
          -> Result<std::unique_ptr<AuthManager>> {
        class CustomAuthManager : public AuthManager {
         public:
          Result<std::shared_ptr<AuthSession>> CatalogSession(
              std::shared_ptr<HttpClient>,
              const std::unordered_map<std::string, std::string>&) override {
            return AuthSession::MakeDefault({{"X-Custom-Auth", "custom-value"}});
          }
        };
        return std::make_unique<CustomAuthManager>();
      });

  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "custom"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_EQ(auth_result.value().headers["X-Custom-Auth"], "custom-value");
}

// Verifies OAuth2 with static token
TEST_F(AuthManagerTest, OAuth2StaticToken) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kToken.key(), "my-static-token"},
      {AuthProperties::kCredential.key(), "client-id:client-secret"},
      {AuthProperties::kScope.key(), "catalog"},
      {AuthProperties::kOAuth2ServerUri.key(), "https://auth.example.com/token"},
      {AuthProperties::kAudience.key(), "catalog-audience"},
      {AuthProperties::kResource.key(), "catalog-resource"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_EQ(auth_result.value().headers["Authorization"], "Bearer my-static-token");

  auto info = session_result.value()->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->token, "my-static-token");
  EXPECT_EQ(info->issued_token_type, AuthProperties::kAccessTokenType);
  EXPECT_EQ(info->credential, "client-id:client-secret");
  EXPECT_EQ(info->scope, "catalog");
  EXPECT_EQ(info->oauth2_server_uri, "https://auth.example.com/token");
  EXPECT_EQ(info->optional_oauth_params.at(AuthProperties::kAudience.key()),
            "catalog-audience");
  EXPECT_EQ(info->optional_oauth_params.at(AuthProperties::kResource.key()),
            "catalog-resource");
}

TEST_F(AuthManagerTest, OAuth2ZeroSessionTimeoutCreatesCatalogSession) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kToken.key(), "my-static-token"},
      {AuthProperties::kSessionTimeoutMs.key(), "0"},
  };

  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto session, manager->CatalogSession(client_, properties));

  EXPECT_NE(session, nullptr);
  EXPECT_THAT(manager->Close(), IsOk());
}

TEST_F(AuthManagerTest, OAuth2InvalidSessionTimeoutReturnsError) {
  for (const std::string value : {"1h", "abc", "-1"}) {
    std::unordered_map<std::string, std::string> properties = {
        {AuthProperties::kAuthType, "oauth2"},
        {AuthProperties::kToken.key(), "my-static-token"},
        {AuthProperties::kSessionTimeoutMs.key(), value},
    };

    ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
    EXPECT_THAT(manager->CatalogSession(client_, properties),
                IsError(ErrorKind::kInvalidArgument))
        << value;
  }
}

// Verifies OAuth2 type is inferred from token property
TEST_F(AuthManagerTest, OAuth2InferredFromToken) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kToken.key(), "inferred-token"},
      {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_EQ(auth_result.value().headers["Authorization"], "Bearer inferred-token");
}

// Verifies OAuth2 returns unauthenticated session when neither token nor credential is
// provided
TEST_F(AuthManagerTest, OAuth2MissingCredentials) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  // Session should have no auth headers
  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_TRUE(auth_result.has_value());
  EXPECT_EQ(auth_result.value().headers.find("Authorization"),
            auth_result.value().headers.end());

  auto info = session_result.value()->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_TRUE(info->token.empty());
  EXPECT_EQ(info->issued_token_type, AuthProperties::kAccessTokenType);
}

TEST_F(AuthManagerTest, OAuth2ContextTokenCreatesChildAndHasPriority) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kScope.key(), "catalog"},
      {AuthProperties::kOAuth2ServerUri.key(), "https://auth.example.com/token"},
      {AuthProperties::kAudience.key(), "catalog-audience"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, properties));

  SessionContext context{
      .session_id = "tenant-a",
      .credentials =
          {
              {AuthProperties::kToken.key(), "context-token"},
              {AuthProperties::kCredential.key(), "unused-credential"},
              {AuthProperties::kIdTokenType, "unused-id-token"},
          },
  };
  ICEBERG_UNWRAP_OR_FAIL(auto child, manager->ContextualSession(context, parent));

  EXPECT_NE(child, parent);
  ICEBERG_UNWRAP_OR_FAIL(auto authenticated, child->Authenticate({}));
  EXPECT_EQ(authenticated.headers.at("Authorization"), "Bearer context-token");

  auto info = child->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->token, "context-token");
  EXPECT_EQ(info->issued_token_type, AuthProperties::kAccessTokenType);
  EXPECT_TRUE(info->credential.empty());
  EXPECT_EQ(info->scope, "catalog");
  EXPECT_EQ(info->oauth2_server_uri, "https://auth.example.com/token");
  EXPECT_EQ(info->optional_oauth_params.at(AuthProperties::kAudience.key()),
            "catalog-audience");
}

TEST_F(AuthManagerTest, OAuth2ContextTypedTokenOnlyUsesCredentials) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, properties));

  SessionContext context{
      .session_id = "tenant-a",
      .credentials = {{"unrelated", "value"}},
      .properties = {{AuthProperties::kIdTokenType, "property-id-token"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto child, manager->ContextualSession(context, parent));

  EXPECT_EQ(child, parent);
}

TEST_F(AuthManagerTest, OAuth2TableTokenCreatesChild) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kScope.key(), "catalog"},
      {AuthProperties::kOAuth2ServerUri.key(), "https://auth.example.com/token"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, properties));
  TableIdentifier table{.ns = Namespace{{"db"}}, .name = "table"};

  ICEBERG_UNWRAP_OR_FAIL(
      auto child,
      manager->TableSession(table,
                            {{AuthProperties::kToken.key(), "table-token"},
                             {AuthProperties::kCredential.key(), "ignored-credential"}},
                            parent));

  EXPECT_NE(child, parent);
  ICEBERG_UNWRAP_OR_FAIL(auto authenticated, child->Authenticate({}));
  EXPECT_EQ(authenticated.headers.at("Authorization"), "Bearer table-token");
  auto info = child->OAuth2Info();
  ASSERT_TRUE(info.has_value());
  EXPECT_EQ(info->issued_token_type, AuthProperties::kAccessTokenType);
  EXPECT_TRUE(info->credential.empty());
}

TEST_F(AuthManagerTest, OAuth2TableTokenSessionIsCachedByToken) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, properties));
  TableIdentifier table{.ns = Namespace{{"db"}}, .name = "table"};

  ICEBERG_UNWRAP_OR_FAIL(
      auto first,
      manager->TableSession(table, {{AuthProperties::kToken.key(), "token-a"}}, parent));
  ICEBERG_UNWRAP_OR_FAIL(
      auto second,
      manager->TableSession(table, {{AuthProperties::kToken.key(), "token-a"}}, parent));
  ICEBERG_UNWRAP_OR_FAIL(
      auto other,
      manager->TableSession(table, {{AuthProperties::kToken.key(), "token-b"}}, parent));

  EXPECT_EQ(first, second);
  EXPECT_NE(first, other);
  EXPECT_THAT(manager->Close(), IsOk());
}

TEST_F(AuthManagerTest, OAuth2TableIgnoresCredential) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client_, properties));
  TableIdentifier table{.ns = Namespace{{"db"}}, .name = "table"};

  ICEBERG_UNWRAP_OR_FAIL(
      auto child,
      manager->TableSession(
          table, {{AuthProperties::kCredential.key(), "ignored-credential"}}, parent));

  EXPECT_EQ(child, parent);
}

// Verifies that when both token and credential are provided, token takes priority
// in CatalogSession (without a prior InitSession call)
TEST_F(AuthManagerTest, OAuth2TokenTakesPriorityOverCredential) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kCredential.key(), "secret-only"},
      {AuthProperties::kToken.key(), "my-static-token"},
      {"uri", "http://localhost:8181"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  auto auth_result = session_result.value()->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_EQ(auth_result.value().headers["Authorization"], "Bearer my-static-token");
}

// Verifies OAuthTokenResponse JSON parsing
TEST_F(AuthManagerTest, OAuthTokenResponseParsing) {
  std::string json = R"({
    "access_token": "test-access-token",
    "token_type": "bearer",
    "expires_in": 3600,
    "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
    "refresh_token": "test-refresh-token",
    "scope": "catalog"
  })";

  auto result = ParseTokenResponse(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->access_token, "test-access-token");
  EXPECT_EQ(result->token_type, "bearer");
  ASSERT_TRUE(result->expires_in_secs.has_value());
  EXPECT_EQ(result->expires_in_secs.value(), 3600);
  EXPECT_EQ(result->issued_token_type, "urn:ietf:params:oauth:token-type:access_token");
  EXPECT_EQ(result->refresh_token, "test-refresh-token");
  EXPECT_EQ(result->scope, "catalog");
}

// Verifies OAuthTokenResponse parsing with minimal fields
TEST_F(AuthManagerTest, OAuthTokenResponseMinimal) {
  std::string json = R"({
    "access_token": "token123",
    "token_type": "Bearer"
  })";

  auto result = ParseTokenResponse(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->access_token, "token123");
  EXPECT_EQ(result->token_type, "Bearer");
  EXPECT_FALSE(result->expires_in_secs.has_value());
  EXPECT_TRUE(result->issued_token_type.empty());
  EXPECT_TRUE(result->refresh_token.empty());
  EXPECT_TRUE(result->scope.empty());
}

// Verifies OAuthTokenResponse validation fails when access_token is missing
TEST_F(AuthManagerTest, OAuthTokenResponseMissingAccessToken) {
  std::string json = R"({"token_type": "bearer"})";
  auto result = ParseTokenResponse(json);
  EXPECT_THAT(result, ::testing::Not(IsOk()));
}

// Verifies OAuthTokenResponse validation fails when token_type is missing
TEST_F(AuthManagerTest, OAuthTokenResponseMissingTokenType) {
  std::string json = R"({"access_token": "token123"})";
  auto result = ParseTokenResponse(json);
  EXPECT_THAT(result, ::testing::Not(IsOk()));
}

// Verifies OAuthTokenResponse validation fails for unsupported token_type
TEST_F(AuthManagerTest, OAuthTokenResponseUnsupportedTokenType) {
  std::string json = R"({
    "access_token": "token123",
    "token_type": "mac"
  })";
  auto result = ParseTokenResponse(json);
  EXPECT_THAT(result, ::testing::Not(IsOk()));
}

// Verifies OAuthTokenResponse accepts N_A token type
TEST_F(AuthManagerTest, OAuthTokenResponseNATokenType) {
  std::string json = R"({
    "access_token": "token123",
    "token_type": "N_A"
  })";
  auto result = ParseTokenResponse(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result->token_type, "N_A");
}

// ---- OAuth2Util expiry tests ----

TEST_F(AuthManagerTest, ExpiresAtMillisValidJwt) {
  std::string token = MakeJwt(R"({"sub":"user","exp":1700000000})");

  auto result = OAuth2Util::ExpiresAtMillis(token);

  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1700000000LL * 1000);
}

TEST_F(AuthManagerTest, ExpiresAtMillisInvalidTokensReturnNullopt) {
  std::vector<std::string> tokens = {
      "",
      "just-a-plain-token",
      "part1.part2",
      "a.b.c.d",
      MakeJwt(R"({"sub":"user","iat":1700000000})"),
      MakeJwt(R"({"exp":"not-a-number"})"),
      "eyJhbGciOiJIUzI1NiJ9.!!!invalid!!!.signature",
      Base64::UrlEncode(R"({"alg":"HS256"})") + "." +
          Base64::UrlEncode("this is not json") + ".sig",
  };

  for (const auto& token : tokens) {
    EXPECT_FALSE(OAuth2Util::ExpiresAtMillis(token).has_value()) << token;
  }
}

// ---- OAuth2Session tests ----

namespace {

class RefreshHttpClient : public HttpClient {
 public:
  struct Request {
    std::string path;
    std::unordered_map<std::string, std::string> form;
    HttpHeaders headers;
  };

  Result<HttpResponse> PostForm(
      const std::string& path, const std::unordered_map<std::string, std::string>& form,
      const std::unordered_map<std::string, std::string>& headers, const ErrorHandler&,
      AuthSession& session) override {
    ICEBERG_ASSIGN_OR_RAISE(
        auto authenticated,
        session.Authenticate({.method = HttpMethod::kPost,
                              .url = path,
                              .headers = {headers.begin(), headers.end()}}));
    recorded_requests.push_back(
        {.path = path, .form = form, .headers = std::move(authenticated.headers)});
    ++requests;
    return response();
  }
  std::function<Result<HttpResponse>()> response = [] {
    return IOError("refresh failed");
  };
  std::atomic<int> requests{0};
  std::vector<Request> recorded_requests;
};

using test::MockExecutor;

// Already expired, so the session schedules its first refresh immediately.
OAuthTokenResponse ExpiredInitialToken() {
  return {.access_token = "initial", .token_type = "bearer", .expires_in_secs = 0};
}

Result<std::shared_ptr<AuthSession>> RefreshingSession(
    const std::shared_ptr<HttpClient>& client, Executor& executor) {
  return AuthSession::MakeOAuth2(ExpiredInitialToken(), "https://auth.example/token",
                                 "client", "secret", "catalog", true, {}, client,
                                 &executor);
}

}  // namespace

TEST(OAuth2SessionTest, RefreshesAndReschedulesThroughPublicFactory) {
  MockExecutor executor;
  std::array<std::promise<ExecutorTask>, 2> queued;
  int submitted = 0;
  EXPECT_CALL(executor, Submit).Times(2).WillRepeatedly([&](ExecutorTask task) -> Status {
    queued.at(submitted++).set_value(std::move(task));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  client->response = [&]() -> Result<HttpResponse> {
    return HttpResponse(
        200, client->requests == 1
                 ? R"({"access_token":"first","token_type":"bearer","expires_in":1})"
                 : R"({"access_token":"second","token_type":"bearer"})");
  };
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  for (auto& slot : queued) {
    auto task = slot.get_future();
    ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    std::move(task.get())();
  }
  ASSERT_THAT(session->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer second");
  EXPECT_EQ(client->requests, 2);
  for (const auto& request : client->recorded_requests) {
    EXPECT_EQ(request.form.at("grant_type"), "client_credentials");
    EXPECT_FALSE(request.headers.contains("Authorization"));
  }
}

class OAuth2ManagerRefreshTest : public ::testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    if (!GetParam().empty()) {
      properties_[AuthProperties::kExchangeEnabled.key()] = GetParam();
    }
    ICEBERG_UNWRAP_OR_FAIL(manager_, AuthManagers::Load("test-catalog", properties_));
    manager_->SetAuthRefreshExecutor(&executor_);
    client_->response = [this]() -> Result<HttpResponse> {
      return HttpResponse(
          200, client_->requests == 1
                   ? R"({"access_token":"initial","token_type":"bearer","expires_in":1})"
                   : R"({"access_token":"fresh","token_type":"bearer"})");
    };
  }

  void TearDown() override {
    if (session_) {
      EXPECT_THAT(session_->Close(), IsOk());
    }
    if (manager_) {
      EXPECT_THAT(manager_->Close(), IsOk());
    }
  }

  void ExpectRefresh() {
    EXPECT_CALL(executor_, Submit).WillOnce([this](ExecutorTask task) -> Status {
      queued_.set_value(std::move(task));
      return {};
    });
  }

  void RunRefresh() {
    auto task = queued_.get_future();
    ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    std::move(task.get())();
  }

  void CheckChildRequests(const std::string& client_id, const std::string& client_secret,
                          const std::string& parent_token) {
    ASSERT_EQ(client_->recorded_requests.size(), 2);
    std::unordered_map<std::string, std::string> credential_form{
        {"grant_type", "client_credentials"}, {"client_id", client_id},
        {"client_secret", client_secret},     {"scope", "catalog"},
        {"audience", "catalog-service"},      {"resource", "https://catalog.example"},
    };
    const auto& initial = client_->recorded_requests[0];
    EXPECT_THAT(initial.form, ::testing::ContainerEq(credential_form));
    EXPECT_EQ(initial.headers.at("Authorization"), "Bearer " + parent_token);

    const auto& refresh = client_->recorded_requests[1];
    if (GetParam() == "false") {
      EXPECT_THAT(refresh.form, ::testing::ContainerEq(credential_form));
      EXPECT_TRUE(refresh.headers.empty());
    } else {
      const std::unordered_map<std::string, std::string> exchange_form{
          {"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"},
          {"subject_token", "initial"},
          {"subject_token_type", AuthProperties::kAccessTokenType},
          {"scope", "catalog"},
          {"audience", "catalog-service"},
          {"resource", "https://catalog.example"},
      };
      EXPECT_THAT(refresh.form, ::testing::ContainerEq(exchange_form));
      // A delayed worker may correctly recover with the session's own credential.
      EXPECT_THAT(refresh.headers,
                  ::testing::ElementsAre(::testing::Pair(
                      "Authorization",
                      ::testing::AnyOf(
                          "Bearer initial",
                          "Basic " + Base64::Encode(client_id + ":" + client_secret)))));
    }
    for (const auto& request : client_->recorded_requests) {
      EXPECT_EQ(request.path, properties_.at(AuthProperties::kOAuth2ServerUri.key()));
    }
    ICEBERG_UNWRAP_OR_FAIL(auto request, session_->Authenticate({}));
    EXPECT_EQ(request.headers.at("Authorization"), "Bearer fresh");
    EXPECT_EQ(session_->OAuth2Info()->credential, client_id + ":" + client_secret);
  }

  MockExecutor executor_;
  std::promise<ExecutorTask> queued_;
  std::shared_ptr<RefreshHttpClient> client_ = std::make_shared<RefreshHttpClient>();
  std::unordered_map<std::string, std::string> properties_{
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kCredential.key(), "catalog:catalog-secret"},
      {AuthProperties::kOAuth2ServerUri.key(), "https://auth.example/token"},
      {AuthProperties::kScope.key(), "catalog"},
      {AuthProperties::kAudience.key(), "catalog-service"},
      {AuthProperties::kResource.key(), "https://catalog.example"},
  };
  std::unique_ptr<AuthManager> manager_;
  std::shared_ptr<AuthSession> session_;
};

TEST_P(OAuth2ManagerRefreshTest, CredentialContextInheritsCatalogRefreshGrant) {
  ExpectRefresh();
  properties_[AuthProperties::kToken.key()] = "parent-token";
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager_->CatalogSession(client_, properties_));
  SessionContext context{
      .session_id = "tenant-a",
      .credentials = {{AuthProperties::kCredential.key(), "child:child-secret"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(session_, manager_->ContextualSession(context, parent));
  ASSERT_NO_FATAL_FAILURE(RunRefresh());
  CheckChildRequests("child", "child-secret", "parent-token");
  ICEBERG_UNWRAP_OR_FAIL(auto cached, manager_->ContextualSession(context, parent));
  EXPECT_EQ(cached, session_);
}

TEST_P(OAuth2ManagerRefreshTest, ExpiredCredentialContextRecoversWithOwnRawCredential) {
  ExpectRefresh();
  properties_[AuthProperties::kToken.key()] = "parent-token";
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager_->CatalogSession(client_, properties_));
  client_->response = [this]() -> Result<HttpResponse> {
    return HttpResponse(
        200,
        client_->requests == 1
            ? R"({"access_token":"child-expired","token_type":"bearer","issued_token_type":"urn:ietf:params:oauth:token-type:jwt","expires_in":0})"
            : R"({"access_token":"child-fresh","token_type":"bearer"})");
  };
  SessionContext context{
      .session_id = "tenant-recovery",
      .credentials = {{AuthProperties::kCredential.key(), ":child-secret"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(session_, manager_->ContextualSession(context, parent));
  EXPECT_EQ(session_->OAuth2Info()->credential, ":child-secret");
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 2);
  const auto& initial = client_->recorded_requests[0];
  const std::unordered_map<std::string, std::string> credential_form{
      {"grant_type", "client_credentials"},
      {"client_secret", "child-secret"},
      {"scope", "catalog"},
      {"audience", "catalog-service"},
      {"resource", "https://catalog.example"},
  };
  EXPECT_THAT(initial.form, ::testing::ContainerEq(credential_form));
  EXPECT_EQ(initial.headers.at("Authorization"), "Bearer parent-token");
  const auto& recovery = client_->recorded_requests[1];
  if (GetParam() == "false") {
    EXPECT_THAT(recovery.form, ::testing::ContainerEq(credential_form));
    EXPECT_TRUE(recovery.headers.empty());
  } else {
    const std::unordered_map<std::string, std::string> exchange_form{
        {"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"},
        {"subject_token", "child-expired"},
        {"subject_token_type", AuthProperties::kJwtTokenType},
        {"scope", "catalog"},
        {"audience", "catalog-service"},
        {"resource", "https://catalog.example"},
    };
    EXPECT_THAT(recovery.form, ::testing::ContainerEq(exchange_form));
    EXPECT_THAT(recovery.headers, ::testing::ElementsAre(::testing::Pair(
                                      "Authorization", "Basic OmNoaWxkLXNlY3JldA==")));
  }
  EXPECT_EQ(recovery.path, properties_.at(AuthProperties::kOAuth2ServerUri.key()));
  EXPECT_EQ(session_->OAuth2Info()->credential, ":child-secret");
  ICEBERG_UNWRAP_OR_FAIL(auto request, session_->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer child-fresh");
  ICEBERG_UNWRAP_OR_FAIL(auto parent_request, parent->Authenticate({}));
  EXPECT_EQ(parent_request.headers.at("Authorization"), "Bearer parent-token");
  ICEBERG_UNWRAP_OR_FAIL(auto cached, manager_->ContextualSession(context, parent));
  EXPECT_EQ(cached, session_);
  EXPECT_EQ(client_->requests, 2);
}

TEST_P(OAuth2ManagerRefreshTest, RefreshDisabledDoesNotScheduleCatalogOrContext) {
  EXPECT_CALL(executor_, Submit).Times(0);
  properties_[AuthProperties::kKeepRefreshed.key()] = "false";
  client_->response = []() -> Result<HttpResponse> {
    return HttpResponse(
        200, R"({"access_token":"initial","token_type":"bearer","expires_in":0})");
  };
  ICEBERG_UNWRAP_OR_FAIL(session_, manager_->CatalogSession(client_, properties_));
  ICEBERG_UNWRAP_OR_FAIL(
      auto child,
      manager_->ContextualSession(
          {.session_id = "tenant-a",
           .credentials = {{AuthProperties::kCredential.key(), "child:child-secret"}}},
          session_));
  ASSERT_TRUE(test::WaitForSharedTimer());
  EXPECT_EQ(client_->requests, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto request, child->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer initial");
}

INSTANTIATE_TEST_SUITE_P(ExchangeModes, OAuth2ManagerRefreshTest,
                         ::testing::Values(std::string(""), std::string("true"),
                                           std::string("false")),
                         [](const ::testing::TestParamInfo<std::string>& info) {
                           return info.param.empty()     ? "Default"
                                  : info.param == "true" ? "Enabled"
                                                         : "Disabled";
                         });

class OAuth2ExchangeRefreshTest : public ::testing::Test {
 protected:
  void SetUp() override {
    config_.now = [this] { return now_; };
  }
  void ExpectRefreshes(int count) {
    if (count == 0) {
      EXPECT_CALL(executor_, Submit).Times(0);
      return;
    }
    EXPECT_CALL(executor_, Submit)
        .Times(count)
        .WillRepeatedly([this](ExecutorTask task) -> Status {
          queued_.at(submitted_++).set_value(std::move(task));
          return {};
        });
  }

  Status Start() {
    ICEBERG_ASSIGN_OR_RAISE(
        session_, internal::OAuth2Session::Make(token_, config_, client_, std::nullopt));
    return {};
  }

  void RunRefresh(size_t index = 0) {
    auto task = queued_.at(index).get_future();
    ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    std::move(task.get())();
  }

  MockExecutor executor_;
  std::array<std::promise<ExecutorTask>, 2> queued_;
  int submitted_ = 0;
  std::shared_ptr<RefreshHttpClient> client_ = std::make_shared<RefreshHttpClient>();
  std::chrono::steady_clock::time_point now_ = std::chrono::steady_clock::now();
  internal::OAuth2Session::Config config_{
      .token_endpoint = "https://auth.example/token",
      .scope = "catalog",
      .optional_oauth_params = {{"audience", "catalog-service"},
                                {"resource", "https://catalog.example"}},
      .exchange_enabled = true,
      .refresh_executor = &executor_,
  };
  OAuthTokenResponse token_{
      .access_token = "initial",
      .token_type = "bearer",
      .expires_in_secs = 1,
      .issued_token_type = AuthProperties::kAccessTokenType,
  };
  // Destroy the session before its borrowed executor and task promises.
  std::shared_ptr<internal::OAuth2Session> session_;
};

TEST_F(OAuth2ExchangeRefreshTest, ExchangesCurrentTokenAndTypeOnEachRefresh) {
  ExpectRefreshes(2);
  // Even when a credential is available, exchange must keep the current subject.
  config_.credential = "client:secret";
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 1) {
      return HttpResponse(
          200, nlohmann::json({{"access_token", "first"},
                               {"token_type", "bearer"},
                               {"issued_token_type", AuthProperties::kJwtTokenType},
                               {"expires_in", 1}})
                   .dump());
    }
    return HttpResponse(200, R"({"access_token":"second","token_type":"bearer"})");
  };
  ASSERT_THAT(Start(), IsOk());

  ASSERT_NO_FATAL_FAILURE(RunRefresh());
  ICEBERG_UNWRAP_OR_FAIL(auto first_request, session_->Authenticate({}));
  EXPECT_EQ(first_request.headers.at("Authorization"), "Bearer first");
  ASSERT_TRUE(session_->ExpiresAt().has_value());
  EXPECT_TRUE(session_->IsRefreshable());

  ASSERT_NO_FATAL_FAILURE(RunRefresh(1));
  ASSERT_EQ(client_->recorded_requests.size(), 2);
  for (size_t i = 0; i < client_->recorded_requests.size(); ++i) {
    const auto& request = client_->recorded_requests[i];
    const std::string subject = i == 0 ? "initial" : "first";
    const std::unordered_map<std::string, std::string> expected_form{
        {"grant_type", "urn:ietf:params:oauth:grant-type:token-exchange"},
        {"subject_token", subject},
        {"subject_token_type",
         i == 0 ? AuthProperties::kAccessTokenType : AuthProperties::kJwtTokenType},
        {"scope", "catalog"},
        {"audience", "catalog-service"},
        {"resource", "https://catalog.example"},
    };
    EXPECT_EQ(request.path, config_.token_endpoint);
    EXPECT_THAT(request.form, ::testing::ContainerEq(expected_form));
    EXPECT_EQ(request.headers.at("Authorization"), "Bearer " + subject);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto second_request, session_->Authenticate({}));
  EXPECT_EQ(second_request.headers.at("Authorization"), "Bearer second");
  EXPECT_EQ(session_->OAuth2Info()->issued_token_type, AuthProperties::kAccessTokenType);
  EXPECT_FALSE(session_->ExpiresAt().has_value());
  EXPECT_FALSE(session_->IsRefreshable());
  EXPECT_THAT(session_->Close(), IsOk());
}

TEST_F(OAuth2ExchangeRefreshTest, RetriesExchangeWithoutChangingIdentity) {
  ExpectRefreshes(1);
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 1) {
      throw std::runtime_error("HTTP library failure");
    }
    if (client_->requests == 2) {
      return IOError("temporarily unavailable");
    }
    return HttpResponse(200, R"({"access_token":"fresh","token_type":"bearer"})");
  };
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 3);
  EXPECT_THAT(fake_retry.observed_base_delays_ms(), ::testing::ElementsAre(100, 200));
  for (const auto& request : client_->recorded_requests) {
    EXPECT_EQ(request.form.at("subject_token"), "initial");
    EXPECT_EQ(request.headers.at("Authorization"), "Bearer initial");
    EXPECT_FALSE(request.form.contains("client_secret"));
  }
  EXPECT_EQ(session_->OAuth2Info()->token, "fresh");
  EXPECT_THAT(session_->Close(), IsOk());
}

class OAuth2ExpiredExchangeRefreshTest : public OAuth2ExchangeRefreshTest,
                                         public ::testing::WithParamInterface<bool> {
 protected:
  HttpResponse ExpiredResponse() const {
    return HttpResponse(
        200, nlohmann::json(
                 {{"access_token", GetParam() ? MakeJwt(R"({"exp":0})") : "expired"},
                  {"token_type", "bearer"},
                  {"issued_token_type", GetParam() ? AuthProperties::kJwtTokenType
                                                   : AuthProperties::kAccessTokenType},
                  {"expires_in", GetParam() ? 3600 : 0}})
                 .dump());
  }

  void ExpectInitialState(
      std::optional<std::chrono::steady_clock::time_point> expiration) {
    EXPECT_EQ(session_->OAuth2Info()->token, token_.access_token);
    EXPECT_EQ(session_->OAuth2Info()->issued_token_type, token_.issued_token_type);
    EXPECT_EQ(session_->ExpiresAt(), expiration);
    ICEBERG_UNWRAP_OR_FAIL(auto request, session_->Authenticate({}));
    EXPECT_EQ(request.headers.at("Authorization"), "Bearer " + token_.access_token);
  }

  void ExpectInitialExchangeRequests(bool final_recovery = false) {
    for (size_t i = 0; i < client_->recorded_requests.size(); ++i) {
      const auto& request = client_->recorded_requests[i];
      EXPECT_EQ(request.form.at("subject_token"), token_.access_token);
      EXPECT_EQ(request.form.at("subject_token_type"), token_.issued_token_type);
      EXPECT_EQ(request.headers.at("Authorization"),
                final_recovery && i == 6 ? "Basic Y2xpZW50OnNlY3JldA=="
                                         : "Bearer " + token_.access_token);
    }
  }
};

TEST_P(OAuth2ExpiredExchangeRefreshTest, RetriesWithPreviousTokenUntilSuccess) {
  ExpectRefreshes(1);
  ASSERT_THAT(Start(), IsOk());
  auto initial_expiration = session_->ExpiresAt();
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 1) {
      return ExpiredResponse();
    }
    ExpectInitialState(initial_expiration);
    const auto& request = client_->recorded_requests.back();
    if (request.form.at("subject_token") != token_.access_token ||
        request.headers.at("Authorization") != "Bearer " + token_.access_token) {
      return IOError("authorization server rejects the expired token");
    }
    return HttpResponse(200, R"({"access_token":"fresh","token_type":"bearer"})");
  };
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 2);
  EXPECT_THAT(fake_retry.observed_base_delays_ms(), ::testing::ElementsAre(100));
  ExpectInitialExchangeRequests();
  ICEBERG_UNWRAP_OR_FAIL(auto request, session_->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer fresh");
  EXPECT_EQ(session_->OAuth2Info()->issued_token_type, AuthProperties::kAccessTokenType);
  EXPECT_FALSE(session_->ExpiresAt().has_value());
  EXPECT_FALSE(session_->IsRefreshable());
  EXPECT_THAT(session_->Close(), IsOk());
}

TEST_P(OAuth2ExpiredExchangeRefreshTest, KeepsPreviousTokenAfterRetriesExhausted) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  ASSERT_THAT(Start(), IsOk());
  auto initial_expiration = session_->ExpiresAt();
  client_->response = [&]() -> Result<HttpResponse> { return ExpiredResponse(); };
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 7);
  ExpectInitialExchangeRequests(true);
  ExpectInitialState(initial_expiration);
  EXPECT_FALSE(session_->IsRefreshable());
  EXPECT_THAT(session_->Close(), IsOk());
}

INSTANTIATE_TEST_SUITE_P(ExpirationSources, OAuth2ExpiredExchangeRefreshTest,
                         ::testing::Bool(),
                         [](const ::testing::TestParamInfo<bool>& info) {
                           return info.param ? "Jwt" : "ExpiresIn";
                         });

TEST_F(OAuth2ExchangeRefreshTest, StopsAfterExchangeRetryBudget) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 7);
  for (size_t i = 0; i < client_->recorded_requests.size(); ++i) {
    const auto& request = client_->recorded_requests[i];
    EXPECT_EQ(request.headers.at("Authorization"),
              i == 6 ? "Basic Y2xpZW50OnNlY3JldA==" : "Bearer initial");
    EXPECT_EQ(request.form.at("grant_type"),
              "urn:ietf:params:oauth:grant-type:token-exchange");
    EXPECT_EQ(request.form.at("subject_token"), "initial");
    EXPECT_FALSE(request.form.contains("client_secret"));
  }
  EXPECT_EQ(session_->OAuth2Info()->token, "initial");
  EXPECT_FALSE(session_->IsRefreshable());
  EXPECT_THAT(session_->Close(), IsOk());
}

class OAuth2ExpiredCredentialTest
    : public OAuth2ExchangeRefreshTest,
      public ::testing::WithParamInterface<std::pair<std::string, std::string>> {};

TEST_P(OAuth2ExpiredCredentialTest, EncodesClientCredentialsForBasicAuth) {
  ExpectRefreshes(1);
  token_.expires_in_secs = 0;
  const auto& [credential, expected_user_password] = GetParam();
  config_.credential = credential;
  client_->response = []() -> Result<HttpResponse> {
    return HttpResponse(200, R"({"access_token":"fresh","token_type":"bearer"})");
  };
  ASSERT_THAT(Start(), IsOk());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 1);
  const auto& request = client_->recorded_requests.front();
  EXPECT_EQ(request.headers.at("Authorization"),
            "Basic " + Base64::Encode(expected_user_password));
  EXPECT_EQ(request.form.at("subject_token"), "initial");
  EXPECT_EQ(request.form.at("subject_token_type"), AuthProperties::kAccessTokenType);
  EXPECT_FALSE(request.form.contains("actor_token"));
  EXPECT_FALSE(request.form.contains("client_secret"));
  EXPECT_EQ(session_->OAuth2Info()->credential, credential);
  EXPECT_EQ(session_->OAuth2Info()->token, "fresh");
  EXPECT_FALSE(session_->IsRefreshable());
}

INSTANTIATE_TEST_SUITE_P(
    CredentialForms, OAuth2ExpiredCredentialTest,
    ::testing::Values(
        std::pair<std::string, std::string>{"id:secret", "id:secret"},
        std::pair<std::string, std::string>{"secret", ":secret"},
        std::pair<std::string, std::string>{":secret", ":secret"},
        std::pair<std::string, std::string>{"id:", "id:"},
        std::pair<std::string, std::string>{"id:secret:tail", "id:secret%3Atail"},
        std::pair<std::string, std::string>{"a+b:s+e cret", "a%2Bb:s%2Be+cret"},
        std::pair<std::string, std::string>{"*~:%caf\xC3\xA9", "*%7E:%25caf%C3%A9"}));

TEST_F(OAuth2ExchangeRefreshTest, SwitchesToBasicWhenTokenExpiresDuringRetries) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 1) {
      now_ += std::chrono::seconds(2);
      return IOError("Bearer refresh failed");
    }
    return HttpResponse(200, R"({"access_token":"fresh","token_type":"bearer"})");
  };
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 2);
  EXPECT_EQ(client_->recorded_requests[0].headers.at("Authorization"), "Bearer initial");
  EXPECT_EQ(client_->recorded_requests[1].headers.at("Authorization"),
            "Basic Y2xpZW50OnNlY3JldA==");
  for (const auto& request : client_->recorded_requests) {
    EXPECT_EQ(request.form.at("subject_token"), "initial");
    EXPECT_EQ(request.form.at("subject_token_type"), AuthProperties::kAccessTokenType);
  }
  EXPECT_EQ(session_->OAuth2Info()->token, "fresh");
}

TEST_F(OAuth2ExchangeRefreshTest, FinalRecoveryUsesOwnRequestTimeAndReschedules) {
  ExpectRefreshes(2);
  config_.credential = "client:secret";
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests <= 6) {
      if (client_->requests == 6) {
        now_ += std::chrono::milliseconds(250);
      }
      return IOError("Bearer refresh failed");
    }
    if (client_->requests == 7) {
      return HttpResponse(
          200, R"({"access_token":"recovered","token_type":"bearer","expires_in":1})");
    }
    return HttpResponse(200, R"({"access_token":"next","token_type":"bearer"})");
  };
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 7);
  for (size_t i = 0; i < 7; ++i) {
    const auto& request = client_->recorded_requests[i];
    EXPECT_EQ(request.headers.at("Authorization"),
              i == 6 ? "Basic Y2xpZW50OnNlY3JldA==" : "Bearer initial");
    EXPECT_EQ(request.form.at("subject_token"), "initial");
    EXPECT_FALSE(request.form.contains("actor_token"));
  }
  EXPECT_THAT(fake_retry.observed_base_delays_ms(),
              ::testing::ElementsAre(100, 200, 400, 800, 1600));
  EXPECT_EQ(session_->ExpiresAt(), now_ + std::chrono::seconds(1));
  EXPECT_EQ(session_->OAuth2Info()->token, "recovered");
  EXPECT_TRUE(session_->IsRefreshable());
  ASSERT_NO_FATAL_FAILURE(RunRefresh(1));
  EXPECT_EQ(client_->recorded_requests.back().headers.at("Authorization"),
            "Bearer recovered");
  EXPECT_EQ(client_->recorded_requests.back().form.at("subject_token"), "recovered");
  EXPECT_EQ(session_->OAuth2Info()->token, "next");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, ExpiredJwtSharesSevenAttemptBudget) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  token_.access_token = MakeJwt(R"({"exp":0})");
  token_.issued_token_type = AuthProperties::kJwtTokenType;
  token_.expires_in_secs = 3600;
  ASSERT_THAT(Start(), IsOk());
  const auto expiration = session_->ExpiresAt();
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 7);
  for (const auto& request : client_->recorded_requests) {
    EXPECT_EQ(request.headers.at("Authorization"), "Basic Y2xpZW50OnNlY3JldA==");
    EXPECT_EQ(request.form.at("subject_token"), token_.access_token);
    EXPECT_EQ(request.form.at("subject_token_type"), AuthProperties::kJwtTokenType);
  }
  EXPECT_EQ(session_->OAuth2Info()->token, token_.access_token);
  EXPECT_EQ(session_->ExpiresAt(), expiration);
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, MissingCredentialSkipsExpiredTokenRecovery) {
  ExpectRefreshes(1);
  token_.expires_in_secs = 0;
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 0);
  EXPECT_EQ(session_->OAuth2Info()->token, "initial");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, MissingCredentialSkipsFinalRecovery) {
  ExpectRefreshes(1);
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  ASSERT_EQ(client_->recorded_requests.size(), 6);
  for (const auto& request : client_->recorded_requests) {
    EXPECT_EQ(request.headers.at("Authorization"), "Bearer initial");
  }
  EXPECT_EQ(session_->OAuth2Info()->token, "initial");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, MissingCredentialStopsWhenTokenExpiresDuringRetries) {
  ExpectRefreshes(1);
  client_->response = [&]() -> Result<HttpResponse> {
    now_ += std::chrono::seconds(2);
    return IOError("Bearer refresh failed");
  };
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 1);
  EXPECT_EQ(client_->recorded_requests.front().headers.at("Authorization"),
            "Bearer initial");
  EXPECT_EQ(session_->OAuth2Info()->token, "initial");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, StopBeforeFinalRecoverySkipsRequest) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 6) {
      session_->StopRefreshing();
    }
    return IOError("refresh failed");
  };
  ASSERT_THAT(Start(), IsOk());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());

  EXPECT_EQ(client_->requests, 6);
  EXPECT_EQ(session_->OAuth2Info()->token, "initial");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, CloseWaitsForFinalRecovery) {
  ExpectRefreshes(1);
  config_.credential = "client:secret";
  std::promise<void> entered;
  std::promise<void> release;
  auto entered_future = entered.get_future();
  auto release_future = release.get_future();
  client_->response = [&]() -> Result<HttpResponse> {
    if (client_->requests == 7) {
      entered.set_value();
      release_future.wait();
      return HttpResponse(
          200, R"({"access_token":"recovered","token_type":"bearer","expires_in":1})");
    }
    return IOError("refresh failed");
  };
  ASSERT_THAT(Start(), IsOk());
  auto task = queued_[0].get_future();
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto refresh = std::async(std::launch::async, [&] {
    test::FakeRetryEnvironment fake_retry;
    ScopedRetryTestHooks hooks(fake_retry.hooks());
    std::move(task.get())();
  });
  auto entered_status = entered_future.wait_for(std::chrono::seconds(5));
  if (entered_status != std::future_status::ready) {
    release.set_value();
  }
  ASSERT_EQ(entered_status, std::future_status::ready);
  auto closing = std::async(std::launch::async, [&] { return session_->Close(); });
  EXPECT_EQ(closing.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
  release.set_value();
  refresh.get();
  EXPECT_THAT(closing.get(), IsOk());

  EXPECT_EQ(client_->requests, 7);
  EXPECT_EQ(session_->OAuth2Info()->token, "recovered");
  EXPECT_FALSE(session_->IsRefreshable());
}

TEST_F(OAuth2ExchangeRefreshTest, StopDuringExchangePreventsRescheduling) {
  ExpectRefreshes(1);
  client_->response = [&]() -> Result<HttpResponse> {
    session_->StopRefreshing();
    return HttpResponse(
        200, R"({"access_token":"fresh","token_type":"bearer","expires_in":1})");
  };
  ASSERT_THAT(Start(), IsOk());
  ASSERT_NO_FATAL_FAILURE(RunRefresh());
  EXPECT_EQ(session_->OAuth2Info()->token, "fresh");
  EXPECT_FALSE(session_->IsRefreshable());
  ASSERT_TRUE(test::WaitForSharedTimer(std::chrono::seconds(1)));
  EXPECT_THAT(session_->Close(), IsOk());
}

TEST_F(OAuth2ExchangeRefreshTest, DoesNotScheduleWithoutExpirationOrExecutor) {
  ExpectRefreshes(0);
  token_.expires_in_secs = std::nullopt;
  ASSERT_THAT(Start(), IsOk());
  EXPECT_FALSE(session_->IsRefreshable());
  EXPECT_THAT(session_->Close(), IsOk());

  token_.expires_in_secs = 0;
  config_.refresh_executor = nullptr;
  ASSERT_THAT(Start(), IsOk());
  EXPECT_FALSE(session_->IsRefreshable());
  ASSERT_TRUE(test::WaitForSharedTimer());
  EXPECT_EQ(client_->requests, 0);
}

TEST(OAuth2SessionTest, AlreadyExpiredRefreshedTokenBacksOff) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  client->response = [&]() -> Result<HttpResponse> {
    return HttpResponse(
        200, client->requests == 1
                 ? R"({"access_token":"stale","token_type":"bearer","expires_in":0})"
                 : R"({"access_token":"fresh","token_type":"bearer"})");
  };
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  std::move(task.get())();
  EXPECT_THAT(fake_retry.sleep_durations(),
              ::testing::ElementsAre(std::chrono::milliseconds(100)));
  EXPECT_EQ(client->requests, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer fresh");
  ASSERT_THAT(session->Close(), IsOk());
}

TEST(OAuth2SessionTest, ThrowingRefreshIsRetried) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  client->response = [&]() -> Result<HttpResponse> {
    if (client->requests == 1) {
      throw std::runtime_error("HTTP library failure");
    }
    return HttpResponse(200, R"({"access_token":"recovered","token_type":"bearer"})");
  };
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  std::move(task.get())();
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer recovered");
  EXPECT_EQ(client->requests, 2);
  ASSERT_THAT(session->Close(), IsOk());
}

TEST(OAuth2SessionTest, CloseSkipsAlreadyQueuedRefresh) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  ASSERT_THAT(session->Close(), IsOk());
  std::move(task.get())();
  EXPECT_EQ(client->requests, 0);
}

TEST(OAuth2SessionTest, ConcurrentCloseCallsWaitForCurrentHttpRequest) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  std::promise<void> entered;
  std::promise<void> release;
  auto entered_future = entered.get_future();
  auto release_future = release.get_future().share();
  client->response = [&]() -> Result<HttpResponse> {
    entered.set_value();
    release_future.wait();
    // Closing the session must stop retries of this failed request.
    return IOError("refresh failed during Close");
  };
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  auto refresh = std::async(std::launch::async, [&] { std::move(task.get())(); });
  auto entered_status = entered_future.wait_for(std::chrono::seconds(5));
  if (entered_status != std::future_status::ready) {
    release.set_value();
  }
  ASSERT_EQ(entered_status, std::future_status::ready);
  auto first = std::async(std::launch::async, [&] { return session->Close(); });
  auto second = std::async(std::launch::async, [&] { return session->Close(); });
  EXPECT_EQ(first.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
  EXPECT_EQ(second.wait_for(std::chrono::milliseconds(30)), std::future_status::timeout);
  release.set_value();
  refresh.get();
  ASSERT_THAT(first.get(), IsOk());
  ASSERT_THAT(second.get(), IsOk());
  EXPECT_EQ(client->requests, 1);
}

TEST(OAuth2SessionTest, StopsAfterHttpRetryBudget) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  ICEBERG_UNWRAP_OR_FAIL(auto session, RefreshingSession(client, executor));
  auto oauth2 = std::dynamic_pointer_cast<internal::OAuth2Session>(session);
  ASSERT_NE(oauth2, nullptr);
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  EXPECT_TRUE(oauth2->IsRefreshable());
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  std::move(task.get())();
  EXPECT_EQ(client->requests, 7);
  for (const auto& request : client->recorded_requests) {
    EXPECT_EQ(request.form.at("grant_type"), "client_credentials");
    EXPECT_EQ(request.form.at("client_id"), "client");
    EXPECT_EQ(request.form.at("client_secret"), "secret");
    EXPECT_FALSE(request.headers.contains("Authorization"));
  }
  EXPECT_THAT(fake_retry.observed_base_delays_ms(),
              ::testing::ElementsAre(100, 200, 400, 800, 1600));
  // No further refresh is scheduled, so the session no longer counts as refreshable.
  EXPECT_FALSE(oauth2->IsRefreshable());
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer initial");
  ASSERT_THAT(session->Close(), IsOk());
}

TEST(OAuth2SessionTest, FinalClientCredentialsRecoveryUpdatesSession) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  auto client = std::make_shared<RefreshHttpClient>();
  client->response = [&]() -> Result<HttpResponse> {
    if (client->requests == 7) {
      return HttpResponse(200, R"({"access_token":"recovered","token_type":"bearer"})");
    }
    return IOError("ordinary refresh failed");
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto session,
      AuthSession::MakeOAuth2(
          {.access_token = "initial", .token_type = "bearer", .expires_in_secs = 1},
          "https://auth.example/token", "client", "secret", "catalog",
          /*keep_refreshed=*/true, {}, client, &executor));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  std::move(task.get())();

  ASSERT_EQ(client->recorded_requests.size(), 7);
  const auto& recovery = client->recorded_requests.back();
  EXPECT_EQ(recovery.form.at("grant_type"), "client_credentials");
  EXPECT_EQ(recovery.form.at("client_id"), "client");
  EXPECT_EQ(recovery.form.at("client_secret"), "secret");
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer recovered");
}

TEST(OAuth2SessionTest, StopRefreshingKeepsSessionUsable) {
  MockExecutor executor;
  EXPECT_CALL(executor, Submit).Times(0);
  OAuthTokenResponse token_response{
      .access_token = "token",
      .token_type = "bearer",
      .expires_in_secs = 3600,
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto session,
      internal::OAuth2Session::Make(token_response,
                                    {.token_endpoint = "http://localhost/oauth/tokens",
                                     .credential = "client_id:client_secret",
                                     .scope = "catalog",
                                     .refresh_executor = &executor},
                                    std::make_shared<HttpClient>(), std::nullopt));
  EXPECT_TRUE(session->IsRefreshable());

  session->StopRefreshing();
  session->StopRefreshing();
  EXPECT_FALSE(session->IsRefreshable());

  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer token");
  EXPECT_THAT(session->Close(), IsOk());
}

TEST(OAuth2SessionTest, ExpiresAtReflectsTokenLifetime) {
  auto make_session = [](std::optional<int64_t> expires_in_secs) {
    OAuthTokenResponse token_response{
        .access_token = "opaque-token",
        .token_type = "bearer",
        .expires_in_secs = expires_in_secs,
    };
    return internal::OAuth2Session::Make(
        token_response, {.token_endpoint = "http://localhost/oauth/tokens"},
        std::make_shared<HttpClient>(), std::nullopt);
  };

  auto before = std::chrono::steady_clock::now();
  ICEBERG_UNWRAP_OR_FAIL(auto expiring, make_session(60));
  auto after = std::chrono::steady_clock::now();
  auto expires_at = expiring->ExpiresAt();
  ASSERT_TRUE(expires_at.has_value());
  EXPECT_GE(*expires_at, before + std::chrono::seconds(60));
  EXPECT_LE(*expires_at, after + std::chrono::seconds(60));

  ICEBERG_UNWRAP_OR_FAIL(auto unknown, make_session(std::nullopt));
  EXPECT_FALSE(unknown->ExpiresAt().has_value());
}

TEST(OAuth2SessionTest, InitTokenExpirationUsesRequestStartTime) {
  OAuthTokenResponse token_response{
      .access_token = "opaque-token",
      .token_type = "bearer",
      .expires_in_secs = 60,
  };
  auto request_started_at = std::chrono::steady_clock::time_point{};

  auto expiration = internal::TokenExpirationTime(token_response, request_started_at);

  ASSERT_TRUE(expiration.has_value());
  EXPECT_EQ(*expiration, request_started_at + std::chrono::seconds(60));
}

TEST(OAuth2SessionTest, JwtExpirationTakesPriorityOverExpiresIn) {
  OAuthTokenResponse token_response{
      .access_token = MakeJwt(R"({"exp":120})"),
      .token_type = "bearer",
      .expires_in_secs = 60,
  };
  auto request_started_at = std::chrono::steady_clock::time_point{};
  auto now_system = std::chrono::system_clock::time_point(std::chrono::seconds(100));
  auto now_steady = std::chrono::steady_clock::time_point(std::chrono::seconds(50));

  auto expiration = internal::TokenExpirationTime(token_response, request_started_at,
                                                  now_system, now_steady);

  ASSERT_TRUE(expiration.has_value());
  EXPECT_EQ(*expiration, std::chrono::steady_clock::time_point(std::chrono::seconds(70)));
}

namespace {

/// Creates a credential child session from a static-token catalog whose token
/// endpoint always succeeds.
Result<std::shared_ptr<internal::OAuth2Session>> CredentialChildSession(
    AuthManager& manager) {
  auto client = std::make_shared<RefreshHttpClient>();
  client->response = []() -> Result<HttpResponse> {
    return HttpResponse(
        200, R"({"access_token":"child-token","token_type":"bearer","expires_in":3600})");
  };
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kToken.key(), "catalog-token"},
  };
  ICEBERG_ASSIGN_OR_RAISE(auto parent, manager.CatalogSession(client, properties));
  SessionContext context{
      .session_id = "tenant-a",
      .credentials = {{AuthProperties::kCredential.key(), "client:secret"}},
  };
  ICEBERG_ASSIGN_OR_RAISE(auto child, manager.ContextualSession(context, parent));
  auto oauth2 = std::dynamic_pointer_cast<internal::OAuth2Session>(child);
  ICEBERG_PRECHECK(oauth2 != nullptr, "child session is not an OAuth2 session");
  return oauth2;
}

}  // namespace

TEST(OAuth2ChildSessionTest, CredentialChildRefreshesWithExecutor) {
  // The refresh is due long after this test ends, so nothing is submitted.
  MockExecutor executor;
  EXPECT_CALL(executor, Submit).Times(0);
  ICEBERG_UNWRAP_OR_FAIL(
      auto manager,
      AuthManagers::Load("test-catalog", {{AuthProperties::kAuthType, "oauth2"}}));
  manager->SetAuthRefreshExecutor(&executor);

  ICEBERG_UNWRAP_OR_FAIL(auto child, CredentialChildSession(*manager));

  EXPECT_TRUE(child->IsRefreshable());
  ICEBERG_UNWRAP_OR_FAIL(auto request, child->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer child-token");
  EXPECT_THAT(manager->Close(), IsOk());
}

TEST(OAuth2ChildSessionTest, CredentialChildWithoutExecutorDoesNotRefresh) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto manager,
      AuthManagers::Load("test-catalog", {{AuthProperties::kAuthType, "oauth2"}}));

  ICEBERG_UNWRAP_OR_FAIL(auto child, CredentialChildSession(*manager));

  EXPECT_FALSE(child->IsRefreshable());
  ICEBERG_UNWRAP_OR_FAIL(auto request, child->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer child-token");
  EXPECT_THAT(manager->Close(), IsOk());
}

TEST(OAuth2ChildSessionTest, ContextSessionIsReloadedAfterRefreshGivesUp) {
  MockExecutor executor;
  std::promise<ExecutorTask> queued;
  auto task = queued.get_future();
  EXPECT_CALL(executor, Submit).WillOnce([&](ExecutorTask work) -> Status {
    queued.set_value(std::move(work));
    return {};
  });
  // The token endpoint first issues an already expired token, then fails until it
  // recovers.
  auto client = std::make_shared<RefreshHttpClient>();
  std::atomic<bool> recovered = false;
  client->response = [&]() -> Result<HttpResponse> {
    if (client->requests == 1) {
      return HttpResponse(
          200, R"({"access_token":"expired","token_type":"bearer","expires_in":0})");
    }
    if (!recovered) {
      return IOError("token endpoint unavailable");
    }
    return HttpResponse(
        200, R"({"access_token":"fresh","token_type":"bearer","expires_in":3600})");
  };
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kToken.key(), "catalog-token"},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  manager->SetAuthRefreshExecutor(&executor);
  ICEBERG_UNWRAP_OR_FAIL(auto parent, manager->CatalogSession(client, properties));
  SessionContext context{
      .session_id = "tenant-a",
      .credentials = {{AuthProperties::kCredential.key(), "client:secret"}},
  };

  ICEBERG_UNWRAP_OR_FAIL(auto first, manager->ContextualSession(context, parent));
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  // While its refresh is pending, the expired session stays cached.
  ICEBERG_UNWRAP_OR_FAIL(auto pending, manager->ContextualSession(context, parent));
  EXPECT_EQ(pending, first);

  {
    test::FakeRetryEnvironment fake_retry;
    ScopedRetryTestHooks hooks(fake_retry.hooks());
    std::move(task.get())();  // every refresh attempt fails
  }
  recovered = true;

  // The refresh gave up and the token has expired, so a new session is loaded.
  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, manager->ContextualSession(context, parent));
  EXPECT_NE(reloaded, first);
  ICEBERG_UNWRAP_OR_FAIL(auto request, reloaded->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer fresh");
  EXPECT_THAT(manager->Close(), IsOk());
}

namespace {

/// Answers token exchanges with a token that records the subject and actor.
class ExchangeHttpClient : public HttpClient {
 public:
  Result<HttpResponse> PostForm(const std::string&,
                                const std::unordered_map<std::string, std::string>& form,
                                const std::unordered_map<std::string, std::string>&,
                                const ErrorHandler&, AuthSession&) override {
    ++requests;
    auto actor = form.find("actor_token");
    std::string token = "sub=" + form.at("subject_token") + ",act=" +
                        (actor == form.end() ? std::string("none") : actor->second);
    return HttpResponse(200, R"({"access_token":")" + token +
                                 R"(","token_type":"bearer","issued_token_type":")" +
                                 AuthProperties::kAccessTokenType + R"("})");
  }
  std::atomic<int> requests{0};
};

class TableSessionCacheKeyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ICEBERG_UNWRAP_OR_FAIL(
        manager_,
        AuthManagers::Load("test-catalog", {{AuthProperties::kAuthType, "oauth2"}}));
    ICEBERG_UNWRAP_OR_FAIL(
        catalog_, manager_->CatalogSession(
                      client_, {{AuthProperties::kAuthType, "oauth2"},
                                {AuthProperties::kToken.key(), "catalog-token"}}));
  }

  void TearDown() override { EXPECT_THAT(manager_->Close(), IsOk()); }

  /// A contextual session holding `token`, used as a table session parent.
  Result<std::shared_ptr<AuthSession>> Parent(const std::string& session_id,
                                              const std::string& token) {
    return manager_->ContextualSession(
        {.session_id = session_id,
         .credentials = {{AuthProperties::kToken.key(), token}}},
        catalog_);
  }

  Result<std::shared_ptr<AuthSession>> Table(
      const std::unordered_map<std::string, std::string>& config,
      std::shared_ptr<AuthSession> parent) {
    return manager_->TableSession(table_, config, std::move(parent));
  }

  static std::string Authorization(AuthSession& session) {
    auto request = session.Authenticate({});
    return request.has_value() ? request->headers.at("Authorization") : "";
  }

  std::shared_ptr<ExchangeHttpClient> client_ = std::make_shared<ExchangeHttpClient>();
  std::unique_ptr<AuthManager> manager_;
  std::shared_ptr<AuthSession> catalog_;
  TableIdentifier table_{.ns = Namespace{{"db"}}, .name = "orders"};
};

}  // namespace

TEST_F(TableSessionCacheKeyTest, ExchangedSessionsAreKeyedByActor) {
  ICEBERG_UNWRAP_OR_FAIL(auto alice, Parent("alice", "alice-token"));
  ICEBERG_UNWRAP_OR_FAIL(auto bob, Parent("bob", "bob-token"));
  std::unordered_map<std::string, std::string> config = {
      {AuthProperties::kJwtTokenType, "table-jwt"}};

  ICEBERG_UNWRAP_OR_FAIL(auto alice_first, Table(config, alice));
  ICEBERG_UNWRAP_OR_FAIL(auto alice_again, Table(config, alice));
  ICEBERG_UNWRAP_OR_FAIL(auto bob_session, Table(config, bob));

  EXPECT_EQ(alice_again, alice_first);
  EXPECT_NE(bob_session, alice_first);
  EXPECT_EQ(client_->requests, 2);
  EXPECT_EQ(Authorization(*alice_first), "Bearer sub=table-jwt,act=alice-token");
  EXPECT_EQ(Authorization(*bob_session), "Bearer sub=table-jwt,act=bob-token");
}

TEST_F(TableSessionCacheKeyTest, SessionsAreKeyedByFlowAndTokenType) {
  ICEBERG_UNWRAP_OR_FAIL(auto alice, Parent("alice", "alice-token"));

  ICEBERG_UNWRAP_OR_FAIL(auto bearer,
                         Table({{AuthProperties::kToken.key(), "same-value"}}, alice));
  ICEBERG_UNWRAP_OR_FAIL(auto jwt,
                         Table({{AuthProperties::kJwtTokenType, "same-value"}}, alice));
  ICEBERG_UNWRAP_OR_FAIL(auto saml2,
                         Table({{AuthProperties::kSaml2TokenType, "same-value"}}, alice));

  EXPECT_NE(jwt, bearer);
  EXPECT_NE(saml2, jwt);
  EXPECT_EQ(client_->requests, 2);
  EXPECT_EQ(Authorization(*bearer), "Bearer same-value");
  EXPECT_EQ(Authorization(*jwt), "Bearer sub=same-value,act=alice-token");
}

}  // namespace iceberg::rest::auth
