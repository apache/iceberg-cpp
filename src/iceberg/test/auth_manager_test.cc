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

#include <string>
#include <unordered_map>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/auth/auth_managers.h"
#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/auth/oauth2_util.h"
#include "iceberg/catalog/rest/http_client.h"
#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest::auth {

namespace {

/// Helper to parse OAuthTokenResponse from a JSON string.
Result<OAuthTokenResponse> ParseTokenResponse(const std::string& str) {
  ICEBERG_ASSIGN_OR_RAISE(auto json, iceberg::FromJsonString(str));
  return iceberg::rest::FromJson<OAuthTokenResponse>(json);
}

}  // namespace

class AuthManagerTest : public ::testing::Test {
 protected:
  HttpClient client_{{}};
};

// Verifies loading NoopAuthManager with explicit "none" auth type
TEST_F(AuthManagerTest, LoadNoopAuthManagerExplicit) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "none"}};

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  EXPECT_TRUE(headers.empty());
}

// Verifies that NoopAuthManager is inferred when no auth properties are set
TEST_F(AuthManagerTest, LoadNoopAuthManagerInferred) {
  auto manager_result = AuthManagers::Load("test-catalog", {});
  ASSERT_THAT(manager_result, IsOk());
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

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  // base64("admin:secret") == "YWRtaW46c2VjcmV0"
  EXPECT_EQ(headers["Authorization"], "Basic YWRtaW46c2VjcmV0");
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

    std::unordered_map<std::string, std::string> headers;
    EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
    // base64("user:pass") == "dXNlcjpwYXNz"
    EXPECT_EQ(headers["Authorization"], "Basic dXNlcjpwYXNz");
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

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  // base64("user@domain.com:p@ss:w0rd!") == "dXNlckBkb21haW4uY29tOnBAc3M6dzByZCE="
  EXPECT_EQ(headers["Authorization"], "Basic dXNlckBkb21haW4uY29tOnBAc3M6dzByZCE=");
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
              HttpClient&, const std::unordered_map<std::string, std::string>&) override {
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

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["X-Custom-Auth"], "custom-value");
}

// Verifies OAuth2 with static token
TEST_F(AuthManagerTest, OAuth2StaticToken) {
  std::unordered_map<std::string, std::string> properties = {
      {AuthProperties::kAuthType, "oauth2"},
      {AuthProperties::kToken.key(), "my-static-token"},
  };

  auto manager_result = AuthManagers::Load("test-catalog", properties);
  ASSERT_THAT(manager_result, IsOk());

  auto session_result = manager_result.value()->CatalogSession(client_, properties);
  ASSERT_THAT(session_result, IsOk());

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer my-static-token");
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

  std::unordered_map<std::string, std::string> headers;
  EXPECT_THAT(session_result.value()->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer inferred-token");
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
  std::unordered_map<std::string, std::string> headers;
  ASSERT_TRUE(session_result.value()->Authenticate(headers).has_value());
  EXPECT_EQ(headers.find("Authorization"), headers.end());
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

  std::unordered_map<std::string, std::string> headers;
  ASSERT_THAT(session_result.value()->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer my-static-token");
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

// ---- ExpiresAtMillis tests ----

// Helper: build a minimal JWT with a given payload JSON string.
// JWT = base64url(header) + "." + base64url(payload) + "." + base64url(signature)
namespace {

// Base64url encode (no padding) for test token construction
std::string Base64UrlEncode(std::string_view input) {
  static constexpr std::string_view kChars =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string output;
  uint32_t buffer = 0;
  int bits = 0;
  for (uint8_t c : input) {
    buffer = (buffer << 8) | c;
    bits += 8;
    while (bits >= 6) {
      bits -= 6;
      output.push_back(kChars[(buffer >> bits) & 0x3F]);
    }
  }
  if (bits > 0) {
    output.push_back(kChars[(buffer << (6 - bits)) & 0x3F]);
  }
  return output;
}

std::string MakeJwt(const std::string& payload_json) {
  std::string header = R"({"alg":"HS256","typ":"JWT"})";
  std::string signature = "test-signature";
  return Base64UrlEncode(header) + "." + Base64UrlEncode(payload_json) + "." +
         Base64UrlEncode(signature);
}

}  // namespace

// Verifies ExpiresAtMillis extracts exp claim from a valid JWT
TEST_F(AuthManagerTest, ExpiresAtMillisValidJwt) {
  // exp = 1700000000 (seconds since epoch)
  std::string token = MakeJwt(R"({"sub":"user","exp":1700000000})");
  auto result = ExpiresAtMillis(token);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1700000000LL * 1000);  // milliseconds
}

// Verifies ExpiresAtMillis handles large exp values correctly
TEST_F(AuthManagerTest, ExpiresAtMillisLargeExp) {
  std::string token = MakeJwt(R"({"exp":2000000000})");
  auto result = ExpiresAtMillis(token);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 2000000000LL * 1000);
}

// Verifies ExpiresAtMillis truncates floating-point exp to integer
TEST_F(AuthManagerTest, ExpiresAtMillisFloatExp) {
  std::string token = MakeJwt(R"({"exp":1700000000.5})");
  auto result = ExpiresAtMillis(token);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), 1700000000LL * 1000);  // truncated to int
}

// Verifies ExpiresAtMillis returns nullopt for non-JWT token without dots
TEST_F(AuthManagerTest, ExpiresAtMillisNonJwtNoDots) {
  auto result = ExpiresAtMillis("just-a-plain-token");
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt for token with only one dot
TEST_F(AuthManagerTest, ExpiresAtMillisOneDot) {
  auto result = ExpiresAtMillis("part1.part2");
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt for token with too many segments
TEST_F(AuthManagerTest, ExpiresAtMillisFourParts) {
  auto result = ExpiresAtMillis("a.b.c.d");
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt when JWT has no exp claim
TEST_F(AuthManagerTest, ExpiresAtMillisNoExpClaim) {
  std::string token = MakeJwt(R"({"sub":"user","iat":1700000000})");
  auto result = ExpiresAtMillis(token);
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt when exp is not a number
TEST_F(AuthManagerTest, ExpiresAtMillisExpNotInteger) {
  std::string token = MakeJwt(R"({"exp":"not-a-number"})");
  auto result = ExpiresAtMillis(token);
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt for malformed base64 payload
TEST_F(AuthManagerTest, ExpiresAtMillisMalformedBase64) {
  // Use invalid base64url characters in the payload part
  std::string token = "eyJhbGciOiJIUzI1NiJ9.!!!invalid!!!.signature";
  auto result = ExpiresAtMillis(token);
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt for empty token
TEST_F(AuthManagerTest, ExpiresAtMillisEmptyToken) {
  auto result = ExpiresAtMillis("");
  EXPECT_FALSE(result.has_value());
}

// Verifies ExpiresAtMillis returns nullopt when payload is not valid JSON
TEST_F(AuthManagerTest, ExpiresAtMillisInvalidJson) {
  std::string header = R"({"alg":"HS256"})";
  std::string invalid_json = "this is not json";
  std::string token =
      Base64UrlEncode(header) + "." + Base64UrlEncode(invalid_json) + "." + "sig";
  auto result = ExpiresAtMillis(token);
  EXPECT_FALSE(result.has_value());
}

// ---- TokenRefreshScheduler tests ----

}  // namespace iceberg::rest::auth

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "iceberg/catalog/rest/auth/token_refresh_scheduler.h"
#include "iceberg/catalog/rest/types.h"

namespace iceberg::rest::auth {

// Verifies that a scheduled task fires after the specified delay
TEST(TokenRefreshSchedulerTest, ScheduleFiresAfterDelay) {
  TokenRefreshScheduler scheduler;
  std::atomic<bool> fired{false};

  scheduler.Schedule(std::chrono::milliseconds(50), [&] { fired.store(true); });

  // Should not have fired immediately
  EXPECT_FALSE(fired.load());

  // Wait enough time for it to fire
  std::this_thread::sleep_for(std::chrono::milliseconds(150));
  EXPECT_TRUE(fired.load());

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

// Verifies that multiple tasks fire in chronological order
TEST(TokenRefreshSchedulerTest, MultipleTasksFireInOrder) {
  TokenRefreshScheduler scheduler;
  std::vector<int> order;
  std::mutex order_mutex;

  auto append = [&](int val) {
    std::lock_guard lock(order_mutex);
    order.push_back(val);
  };

  // Schedule in reverse order of fire time
  scheduler.Schedule(std::chrono::milliseconds(150), [&] { append(3); });
  scheduler.Schedule(std::chrono::milliseconds(50), [&] { append(1); });
  scheduler.Schedule(std::chrono::milliseconds(100), [&] { append(2); });

  // Wait for all to fire
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  std::lock_guard lock(order_mutex);
  ASSERT_EQ(3u, order.size());
  EXPECT_EQ(1, order[0]);
  EXPECT_EQ(2, order[1]);
  EXPECT_EQ(3, order[2]);

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

// ---- OAuth2AuthSession integration tests ----

// Verifies that MakeOAuth2 creates a session with correct initial Bearer header
TEST(OAuth2AuthSessionTest, InitialTokenIsUsed) {
  HttpClient client({});
  OAuthTokenResponse token_response;
  token_response.access_token = "initial-token-123";
  token_response.token_type = "bearer";
  token_response.expires_in_secs = 3600;

  // Create session (refresh will fail since there's no real server, but
  // initial token should work)
  auto session = AuthSession::MakeOAuth2(token_response, "http://localhost/oauth/tokens",
                                         "client_id", "client_secret", "catalog", client);

  std::unordered_map<std::string, std::string> headers;
  ASSERT_THAT(session->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer initial-token-123");

  session->Close();
}

// Verifies that session without expiration does not schedule refresh
TEST(OAuth2AuthSessionTest, NoExpirationNoRefresh) {
  HttpClient client({});
  OAuthTokenResponse token_response;
  token_response.access_token = "static-token";
  token_response.token_type = "bearer";
  // No expires_in_secs set — token is not a JWT either

  auto session = AuthSession::MakeOAuth2(token_response, "http://localhost/oauth/tokens",
                                         "id", "secret", "catalog", client);

  std::unordered_map<std::string, std::string> headers;
  ASSERT_THAT(session->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer static-token");

  // Wait a bit — no crash, no refresh attempt
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  headers.clear();
  ASSERT_THAT(session->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer static-token");

  session->Close();
}

// Verifies that Close prevents further refresh callbacks
TEST(OAuth2AuthSessionTest, CloseStopsRefresh) {
  HttpClient client({});
  OAuthTokenResponse token_response;
  token_response.access_token = "token-before-close";
  token_response.token_type = "bearer";
  token_response.expires_in_secs = 1;  // Expires in 1 second

  auto session = AuthSession::MakeOAuth2(token_response, "http://localhost:9999/tokens",
                                         "id", "secret", "catalog", client);

  // Close immediately — should cancel the scheduled refresh
  session->Close();

  // Wait past expiration + refresh window
  std::this_thread::sleep_for(std::chrono::milliseconds(2000));

  // Token should still be the original (no refresh happened)
  std::unordered_map<std::string, std::string> headers;
  ASSERT_THAT(session->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer token-before-close");
}

// Verifies that concurrent Authenticate calls are thread-safe
TEST(OAuth2AuthSessionTest, ConcurrentAuthenticate) {
  HttpClient client({});
  OAuthTokenResponse token_response;
  token_response.access_token = "concurrent-token";
  token_response.token_type = "bearer";
  token_response.expires_in_secs = 3600;

  auto session = AuthSession::MakeOAuth2(token_response, "http://localhost/oauth/tokens",
                                         "id", "secret", "catalog", client);

  // Launch multiple threads calling Authenticate concurrently
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&] {
      for (int j = 0; j < 100; ++j) {
        std::unordered_map<std::string, std::string> headers;
        auto status = session->Authenticate(headers);
        if (status.has_value()) {
          success_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(1000, success_count.load());
  session->Close();
}

// Verifies that session still returns last known token after all refresh retries fail
TEST(OAuth2AuthSessionTest, RefreshFailureKeepsLastToken) {
  HttpClient client({});
  OAuthTokenResponse token_response;
  token_response.access_token = "original-token";
  token_response.token_type = "bearer";
  token_response.expires_in_secs = 1;  // Very short — will trigger refresh soon

  auto session = AuthSession::MakeOAuth2(
      token_response, "http://localhost:9999/nonexistent",  // Will fail
      "id", "secret", "catalog", client);

  // Wait for refresh to be attempted and fail (all retries)
  // With non-blocking retries: 200ms + 400ms + 800ms + 1600ms ≈ 3s total
  std::this_thread::sleep_for(std::chrono::milliseconds(5000));

  // Session should still return the original token (no crash)
  std::unordered_map<std::string, std::string> headers;
  ASSERT_THAT(session->Authenticate(headers), IsOk());
  EXPECT_EQ(headers["Authorization"], "Bearer original-token");

  session->Close();
}

}  // namespace iceberg::rest::auth
