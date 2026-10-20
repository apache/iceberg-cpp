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

// ---- OAuth2Session tests ----

namespace {

class RefreshHttpClient : public HttpClient {
 public:
  Result<HttpResponse> PostForm(const std::string&,
                                const std::unordered_map<std::string, std::string>&,
                                const std::unordered_map<std::string, std::string>&,
                                const ErrorHandler&, AuthSession&) override {
    ++requests;
    return response();
  }
  std::function<Result<HttpResponse>()> response = [] {
    return IOError("refresh failed");
  };
  std::atomic<int> requests{0};
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
  ASSERT_EQ(task.wait_for(std::chrono::seconds(5)), std::future_status::ready);
  test::FakeRetryEnvironment fake_retry;
  ScopedRetryTestHooks hooks(fake_retry.hooks());
  std::move(task.get())();
  EXPECT_EQ(client->requests, 6);
  EXPECT_THAT(fake_retry.observed_base_delays_ms(),
              ::testing::ElementsAre(100, 200, 400, 800, 1600));
  ICEBERG_UNWRAP_OR_FAIL(auto request, session->Authenticate({}));
  EXPECT_EQ(request.headers.at("Authorization"), "Bearer initial");
  ASSERT_THAT(session->Close(), IsOk());
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
