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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/auth_session_internal.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/rest/catalog_properties.h"
#include "iceberg/catalog/rest/error_handlers.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/session_context.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
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

TEST(AuthPropertiesTest, ResolvesDefaultOAuth2ServerUri) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto config,
      AuthProperties::FromProperties({
          {RestCatalogProperties::kUri.key(), "https://catalog.example.com/api/"},
          {RestCatalogProperties::kPrefix.key(), "warehouse"},
      }));

  EXPECT_EQ(config.oauth2_server_uri(),
            "https://catalog.example.com/api/v1/oauth/tokens");
}

TEST(AuthPropertiesTest, PreservesEmptyOAuth2ServerUri) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto config, AuthProperties::FromProperties({
                       {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
                       {AuthProperties::kOAuth2ServerUri.key(), ""},
                   }));

  EXPECT_TRUE(config.oauth2_server_uri().empty());
}

TEST(AuthPropertiesTest, ResolvesExplicitRelativeOAuth2ServerUri) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto config,
      AuthProperties::FromProperties({
          {RestCatalogProperties::kUri.key(), "https://catalog.example.com/api/"},
          {AuthProperties::kOAuth2ServerUri.key(), "oauth/token/"},
      }));

  EXPECT_EQ(config.oauth2_server_uri(), "https://catalog.example.com/api/oauth/token/");
}

TEST(AuthPropertiesTest, PreservesExplicitAbsoluteOAuth2ServerUri) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto config,
      AuthProperties::FromProperties({
          {RestCatalogProperties::kUri.key(), "https://catalog.example.com"},
          {AuthProperties::kOAuth2ServerUri.key(), "https://auth.example.com/token/"},
      }));

  EXPECT_EQ(config.oauth2_server_uri(), "https://auth.example.com/token/");
}

TEST(AuthPropertiesTest, PreservesRelativeOAuth2ServerUriWithoutCatalogUri) {
  ICEBERG_UNWRAP_OR_FAIL(auto config,
                         AuthProperties::FromProperties({
                             {AuthProperties::kOAuth2ServerUri.key(), "oauth/token"},
                         }));

  EXPECT_EQ(config.oauth2_server_uri(), "oauth/token");
}

TEST(AuthPropertiesTest, PreservesAbsolutePathWithoutCatalogUri) {
  ICEBERG_UNWRAP_OR_FAIL(auto config,
                         AuthProperties::FromProperties({
                             {AuthProperties::kOAuth2ServerUri.key(), "/oauth/token"},
                         }));

  EXPECT_EQ(config.oauth2_server_uri(), "/oauth/token");
}

TEST(AuthPropertiesTest, ResolvesOAuth2ServerUriWithLeadingSlash) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto config,
      AuthProperties::FromProperties({
          {RestCatalogProperties::kUri.key(), "https://catalog.example.com/api/"},
          {AuthProperties::kOAuth2ServerUri.key(), "/v1/oauth/tokens"},
      }));

  EXPECT_EQ(config.oauth2_server_uri(),
            "https://catalog.example.com/api/v1/oauth/tokens");
}

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

// Verifies OAuth2 type is inferred from token property
TEST_F(AuthManagerTest, OAuth2InferredFromToken) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kToken.key(), "inferred-token"},
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

TEST_F(AuthManagerTest, OAuth2TableIgnoresCredential) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
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

// ---- TokenRefreshScheduler tests ----

// Verifies that a scheduled task fires after the specified delay
TEST(TokenRefreshSchedulerTest, ScheduleFiresAfterDelay) {
  TokenRefreshScheduler scheduler;
  std::mutex mutex;
  std::condition_variable cv;
  bool fired = false;

  scheduler.Schedule(std::chrono::milliseconds(50), [&] {
    {
      std::lock_guard lock(mutex);
      fired = true;
    }
    cv.notify_one();
  });

  {
    std::unique_lock lock(mutex);
    EXPECT_TRUE(cv.wait_for(lock, std::chrono::seconds(5), [&] { return fired; }));
  }

  scheduler.Shutdown();
}

// Verifies that cancelling a task prevents it from executing
TEST(TokenRefreshSchedulerTest, CancelPreventsExecution) {
  TokenRefreshScheduler scheduler;
  std::atomic<bool> fired{false};

  auto handle =
      scheduler.Schedule(std::chrono::milliseconds(100), [&] { fired.store(true); });

  // Cancel before it fires
  scheduler.Cancel(handle);

  // Wait past the scheduled time
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_FALSE(fired.load());

  scheduler.Shutdown();
}

// Verifies that shutdown with pending tasks does not crash
TEST(TokenRefreshSchedulerTest, ShutdownWithPendingTasks) {
  TokenRefreshScheduler scheduler;
  std::atomic<bool> fired{false};

  scheduler.Schedule(std::chrono::milliseconds(5000), [&] { fired.store(true); });

  // Shutdown immediately — should not crash and task should not fire
  scheduler.Shutdown();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  EXPECT_FALSE(fired.load());
}

// Verifies that Schedule after shutdown returns invalid handle (0)
TEST(TokenRefreshSchedulerTest, ScheduleAfterShutdownIsNoop) {
  TokenRefreshScheduler scheduler;
  scheduler.Shutdown();

  auto handle = scheduler.Schedule(std::chrono::milliseconds(10), [] {});
  EXPECT_EQ(0u, handle);
}

// Verifies that cancelling an invalid handle does not crash
TEST(TokenRefreshSchedulerTest, CancelInvalidHandleIsNoop) {
  TokenRefreshScheduler scheduler;
  // Should not crash
  scheduler.Cancel(0);
  scheduler.Cancel(999);
  scheduler.Shutdown();
}

// ---- OAuth2Session tests ----

TEST(OAuth2SessionTest, InitialTokenIsUsed) {
  auto client = std::make_shared<HttpClient>();
  OAuthTokenResponse token_response;
  token_response.access_token = "initial-token-123";
  token_response.token_type = "bearer";
  token_response.expires_in_secs = 3600;

  // Create session (refresh will fail since there's no real server, but
  // initial token should work)
  auto session_result =
      AuthSession::MakeOAuth2(token_response, "http://localhost/oauth/tokens",
                              "client_id", "client_secret", "catalog", true, {}, client);
  ASSERT_THAT(session_result, IsOk());
  auto session = session_result.value();

  auto auth_result = session->Authenticate({});
  ASSERT_THAT(auth_result, IsOk());
  EXPECT_EQ(auth_result.value().headers.at("Authorization"), "Bearer initial-token-123");

  session->Close();
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

}  // namespace iceberg::rest::auth
