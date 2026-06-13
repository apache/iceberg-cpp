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

#if ICEBERG_SIGV4_ENABLED

#  include <algorithm>
#  include <cctype>
#  include <string>
#  include <string_view>
#  include <unordered_map>
#  include <utility>
#  include <vector>

#  include <aws/core/auth/AWSCredentialsProvider.h>
#  include <aws/core/utils/HashingUtils.h>
#  include <aws/core/utils/crypto/Sha256HMAC.h>
#  include <gmock/gmock.h>
#  include <gtest/gtest.h>

#  include "iceberg/catalog/rest/auth/auth_managers.h"
#  include "iceberg/catalog/rest/auth/auth_properties.h"
#  include "iceberg/catalog/rest/auth/auth_session.h"
#  include "iceberg/catalog/rest/auth/sigv4_auth_manager_internal.h"
#  include "iceberg/catalog/rest/http_client.h"
#  include "iceberg/table_identifier.h"
#  include "iceberg/test/matchers.h"

namespace iceberg::rest::auth {

namespace {

using ::testing::HasSubstr;
using ::testing::StartsWith;

constexpr std::string_view kAccessKey = "AKIAIOSFODNN7EXAMPLE";
constexpr std::string_view kSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
constexpr std::string_view kAmzContentSha256Header = "x-amz-content-sha256";

std::string HexEncode(const Aws::Utils::ByteBuffer& buffer) {
  static constexpr char kHex[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(buffer.GetLength() * 2);
  for (size_t i = 0; i < buffer.GetLength(); ++i) {
    auto byte = buffer.GetUnderlyingData()[i];
    hex.push_back(kHex[byte >> 4]);
    hex.push_back(kHex[byte & 0x0F]);
  }
  return hex;
}

Aws::Utils::ByteBuffer BufferFromString(std::string_view value) {
  return Aws::Utils::ByteBuffer(reinterpret_cast<const unsigned char*>(value.data()),
                                value.size());
}

Aws::Utils::ByteBuffer HmacSha256(const Aws::Utils::ByteBuffer& key,
                                  std::string_view value) {
  Aws::Utils::Crypto::Sha256HMAC hmac;
  auto result = hmac.Calculate(BufferFromString(value), key);
  EXPECT_TRUE(result.IsSuccess());
  return result.GetResult();
}

std::string Sha256Hex(std::string_view value) {
  auto digest =
      Aws::Utils::HashingUtils::CalculateSHA256(Aws::String(value.data(), value.size()));
  return Aws::Utils::HashingUtils::HexEncode(digest).c_str();
}

std::string ExtractAuthField(std::string_view authorization, std::string_view prefix) {
  auto pos = authorization.find(prefix);
  EXPECT_NE(pos, std::string_view::npos) << authorization;
  if (pos == std::string_view::npos) return {};
  pos += prefix.size();
  auto end = authorization.find(',', pos);
  return std::string(authorization.substr(pos, end - pos));
}

std::string HeaderValue(const HttpHeaders& headers, std::string_view name) {
  auto it = headers.find(name);
  EXPECT_NE(it, headers.end()) << "Missing header: " << name;
  if (it == headers.end()) return {};
  return it->second;
}

std::string PathFromUrl(const std::string& url) {
  auto scheme = url.find("://");
  auto path_start =
      scheme == std::string::npos ? url.find('/') : url.find('/', scheme + 3);
  if (path_start == std::string::npos) return "/";
  auto query_start = url.find('?', path_start);
  return url.substr(path_start, query_start - path_start);
}

std::string CanonicalQueryFromUrl(const std::string& url) {
  auto query_start = url.find('?');
  if (query_start == std::string::npos) return {};
  std::vector<std::string> params;
  size_t start = query_start + 1;
  while (start <= url.size()) {
    auto end = url.find('&', start);
    params.emplace_back(url.substr(start, end - start));
    if (end == std::string::npos) break;
    start = end + 1;
  }
  std::sort(params.begin(), params.end());

  std::string canonical;
  for (const auto& param : params) {
    if (!canonical.empty()) canonical += '&';
    canonical += param;
  }
  return canonical;
}

std::string ExpectedSigV4Signature(const HttpRequest& request,
                                   std::string_view signing_region,
                                   std::string_view signing_name) {
  const auto authorization = HeaderValue(request.headers, "authorization");
  const auto x_amz_date = HeaderValue(request.headers, "x-amz-date");
  const auto credential_scope =
      ExtractAuthField(authorization, std::string(kAccessKey) + "/");
  const auto signed_headers = ExtractAuthField(authorization, "SignedHeaders=");
  const auto date = x_amz_date.substr(0, 8);

  std::string canonical_headers;
  size_t start = 0;
  while (start <= signed_headers.size()) {
    auto end = signed_headers.find(';', start);
    auto header_name = signed_headers.substr(start, end - start);
    canonical_headers += header_name;
    canonical_headers += ':';
    canonical_headers += HeaderValue(request.headers, header_name);
    canonical_headers += '\n';
    if (end == std::string::npos) break;
    start = end + 1;
  }

  auto payload_hash = request.body.empty()
                          ? std::string(SigV4AuthSession::kEmptyBodySha256)
                          : Sha256Hex(request.body);
  const auto canonical_request =
      std::string(ToString(request.method)) + "\n" + PathFromUrl(request.url) + "\n" +
      CanonicalQueryFromUrl(request.url) + "\n" + canonical_headers + "\n" +
      signed_headers + "\n" + payload_hash;
  const auto string_to_sign = "AWS4-HMAC-SHA256\n" + x_amz_date + "\n" +
                              credential_scope + "\n" + Sha256Hex(canonical_request);

  auto date_key = HmacSha256(BufferFromString("AWS4" + std::string(kSecretKey)), date);
  auto region_key = HmacSha256(date_key, signing_region);
  auto service_key = HmacSha256(region_key, signing_name);
  auto signing_key = HmacSha256(service_key, "aws4_request");
  return HexEncode(HmacSha256(signing_key, string_to_sign));
}

}  // namespace

class SigV4AuthTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { ASSERT_THAT(InitializeAwsSdk(), IsOk()); }

  static void TearDownTestSuite() { EXPECT_THAT(FinalizeAwsSdk(), IsOk()); }

  std::unordered_map<std::string, std::string> MakeSigV4Properties() {
    return {
        {AuthProperties::kAuthType, "sigv4"},
        {AuthProperties::kSigV4SigningRegion, "us-east-1"},
        {AuthProperties::kSigV4SigningName, "execute-api"},
        {AuthProperties::kSigV4AccessKeyId, std::string(kAccessKey)},
        {AuthProperties::kSigV4SecretAccessKey, std::string(kSecretKey)},
    };
  }

  Result<std::shared_ptr<AuthSession>> MakeCatalogSession(
      const std::unordered_map<std::string, std::string>& properties) {
    ICEBERG_ASSIGN_OR_RAISE(auto manager, AuthManagers::Load("test-catalog", properties));
    return manager->CatalogSession(client_, properties);
  }

  Result<HttpRequest> SignRequest(
      const std::unordered_map<std::string, std::string>& properties,
      HttpRequest request) {
    ICEBERG_ASSIGN_OR_RAISE(auto session, MakeCatalogSession(properties));
    return session->Authenticate(std::move(request));
  }

  HttpClient client_{{}};
};

TEST_F(SigV4AuthTest, LifecycleInitializeIsIdempotent) {
  EXPECT_THAT(InitializeAwsSdk(), IsOk());
  EXPECT_TRUE(IsAwsSdkInitialized());
  EXPECT_FALSE(IsAwsSdkFinalized());
}

TEST_F(SigV4AuthTest, LifecycleFinalizeRefusesWhileSessionsAlive) {
  ICEBERG_UNWRAP_OR_FAIL(auto session, MakeCatalogSession(MakeSigV4Properties()));

  EXPECT_THAT(FinalizeAwsSdk(), IsError(ErrorKind::kInvalid));
  EXPECT_TRUE(IsAwsSdkInitialized());
}

TEST_F(SigV4AuthTest, AuthenticateWithoutBodyDetailedHeaders) {
  ICEBERG_UNWRAP_OR_FAIL(auto signed_request,
                         SignRequest(MakeSigV4Properties(),
                                     {.method = HttpMethod::kGet,
                                      .url = "http://localhost:8080/path",
                                      .headers = {{"Content-Type", "application/json"},
                                                  {"Content-Encoding", "gzip"}}}));

  EXPECT_EQ(HeaderValue(signed_request.headers, "content-type"), "application/json");
  EXPECT_EQ(HeaderValue(signed_request.headers, "content-encoding"), "gzip");
  EXPECT_EQ(HeaderValue(signed_request.headers, "host"), "localhost:8080");
  EXPECT_EQ(HeaderValue(signed_request.headers, kAmzContentSha256Header),
            SigV4AuthSession::kEmptyBodySha256);
  EXPECT_NE(signed_request.headers.find("x-amz-date"), signed_request.headers.end());

  auto authorization = HeaderValue(signed_request.headers, "authorization");
  EXPECT_THAT(authorization, StartsWith("AWS4-HMAC-SHA256 Credential="));
  EXPECT_THAT(authorization, HasSubstr("SignedHeaders=content-encoding;content-type;host;"
                                       "x-amz-content-sha256;x-amz-date"));
  EXPECT_EQ(ExtractAuthField(authorization, "Signature="),
            ExpectedSigV4Signature(signed_request, "us-east-1", "execute-api"));
}

TEST_F(SigV4AuthTest, AuthenticateWithBodyDetailedHeaders) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(MakeSigV4Properties(),
                  {.method = HttpMethod::kPost,
                   .url = "http://localhost:8080/path",
                   .headers = {{"Content-Type", "application/x-www-form-urlencoded"},
                               {"Content-Encoding", "gzip"}},
                   .body = R"({"namespace":["ns"]})"}));

  auto authorization = HeaderValue(signed_request.headers, "authorization");
  EXPECT_THAT(authorization, StartsWith("AWS4-HMAC-SHA256 Credential="));
  EXPECT_THAT(authorization, HasSubstr("SignedHeaders=content-encoding;content-type;host;"
                                       "x-amz-content-sha256;x-amz-date"));
  EXPECT_EQ(HeaderValue(signed_request.headers, kAmzContentSha256Header),
            "LL0/LbCIE/WzVCHsfA3ASGOx9vJNPeTL0jBro8scPfA=");
  EXPECT_EQ(ExtractAuthField(authorization, "Signature="),
            ExpectedSigV4Signature(signed_request, "us-east-1", "execute-api"));
}

TEST_F(SigV4AuthTest, QueryStringIsIncludedInSignature) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(MakeSigV4Properties(),
                  {.method = HttpMethod::kGet,
                   .url = "http://localhost:8080/path?warehouse=prod&prefix=a"}));

  auto authorization = HeaderValue(signed_request.headers, "authorization");
  EXPECT_THAT(authorization, StartsWith("AWS4-HMAC-SHA256 Credential="));
  EXPECT_EQ(ExtractAuthField(authorization, "Signature="),
            ExpectedSigV4Signature(signed_request, "us-east-1", "execute-api"));
}

TEST_F(SigV4AuthTest, DelegateAuthorizationHeaderIsRelocatedAndSigned) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kToken.key()] = "my-oauth-token";

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(properties, {.method = HttpMethod::kGet,
                               .url = "http://localhost:8080/path",
                               .headers = {{"Content-Type", "application/json"}}}));

  EXPECT_EQ(HeaderValue(signed_request.headers, "original-authorization"),
            "Bearer my-oauth-token");
  auto authorization = HeaderValue(signed_request.headers, "authorization");
  EXPECT_THAT(authorization,
              HasSubstr("SignedHeaders=content-type;host;original-authorization;"
                        "x-amz-content-sha256;x-amz-date"));
  EXPECT_EQ(ExtractAuthField(authorization, "Signature="),
            ExpectedSigV4Signature(signed_request, "us-east-1", "execute-api"));
}

TEST_F(SigV4AuthTest, ConflictingSigV4HeadersRelocated) {
  auto delegate = AuthSession::MakeDefault({
      {"x-amz-content-sha256", "fake-sha256"},
      {"X-Amz-Date", "fake-date"},
      {"Content-Type", "application/json"},
  });
  auto credentials =
      std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(Aws::Auth::AWSCredentials(
          std::string(kAccessKey).c_str(), std::string(kSecretKey).c_str()));
  ICEBERG_UNWRAP_OR_FAIL(
      auto session,
      SigV4AuthSession::Make(delegate, "us-east-1", "execute-api", credentials));

  ICEBERG_UNWRAP_OR_FAIL(auto signed_request,
                         session->Authenticate({.method = HttpMethod::kGet,
                                                .url = "http://localhost:8080/path"}));

  EXPECT_EQ(HeaderValue(signed_request.headers, kAmzContentSha256Header),
            SigV4AuthSession::kEmptyBodySha256);
  EXPECT_EQ(HeaderValue(signed_request.headers, "Original-x-amz-content-sha256"),
            "fake-sha256");
  EXPECT_EQ(HeaderValue(signed_request.headers, "Original-X-Amz-Date"), "fake-date");
  EXPECT_NE(signed_request.headers.find("authorization"), signed_request.headers.end());
}

TEST_F(SigV4AuthTest, AuthenticateWithSessionToken) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SessionToken] = "FwoGZXIvYXdzEBYaDHqa0";

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(properties,
                  {.method = HttpMethod::kGet, .url = "https://example.com/v1/config"}));

  EXPECT_EQ(HeaderValue(signed_request.headers, "x-amz-security-token"),
            "FwoGZXIvYXdzEBYaDHqa0");
  EXPECT_THAT(HeaderValue(signed_request.headers, "authorization"),
              HasSubstr("SignedHeaders=host;x-amz-content-sha256;x-amz-date;"
                        "x-amz-security-token"));
}

TEST_F(SigV4AuthTest, CustomSigningNameAndRegion) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "eu-west-1";
  properties[AuthProperties::kSigV4SigningName] = "custom-service";

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(properties,
                  {.method = HttpMethod::kGet, .url = "https://example.com/v1/config"}));

  auto authorization = HeaderValue(signed_request.headers, "authorization");
  EXPECT_THAT(authorization, HasSubstr("eu-west-1"));
  EXPECT_THAT(authorization, HasSubstr("custom-service"));
}

TEST_F(SigV4AuthTest, LegacySigV4EnabledFlagSelectsSigV4) {
  auto properties = MakeSigV4Properties();
  properties.erase(AuthProperties::kAuthType);
  properties[AuthProperties::kSigV4Enabled] = "true";

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(properties,
                  {.method = HttpMethod::kGet, .url = "https://example.com/v1/config"}));

  EXPECT_THAT(HeaderValue(signed_request.headers, "authorization"),
              StartsWith("AWS4-HMAC-SHA256"));
}

TEST_F(SigV4AuthTest, AuthTypeCaseInsensitive) {
  for (const auto& auth_type : {"SIGV4", "SigV4", "sigV4"}) {
    auto properties = MakeSigV4Properties();
    properties[AuthProperties::kAuthType] = auth_type;
    EXPECT_THAT(AuthManagers::Load("test-catalog", properties), IsOk())
        << "Failed for auth type: " << auth_type;
  }
}

TEST_F(SigV4AuthTest, MissingStaticCredentialsAreRejected) {
  for (auto missing_property :
       {AuthProperties::kSigV4AccessKeyId, AuthProperties::kSigV4SecretAccessKey}) {
    auto properties = MakeSigV4Properties();
    properties.erase(missing_property);
    ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));

    auto session_result = manager->CatalogSession(client_, properties);
    EXPECT_THAT(session_result, IsError(ErrorKind::kInvalidArgument))
        << "Missing property: " << missing_property;
    EXPECT_THAT(session_result, HasErrorMessage("must be set together"));
  }

  auto session_token_only = MakeSigV4Properties();
  session_token_only.erase(AuthProperties::kSigV4AccessKeyId);
  session_token_only.erase(AuthProperties::kSigV4SecretAccessKey);
  session_token_only[AuthProperties::kSigV4SessionToken] = "token";
  ICEBERG_UNWRAP_OR_FAIL(auto manager,
                         AuthManagers::Load("test-catalog", session_token_only));

  auto session_result = manager->CatalogSession(client_, session_token_only);
  EXPECT_THAT(session_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(session_result, HasErrorMessage("requires"));
}

TEST_F(SigV4AuthTest, DerivedCredentialOverridesMustBeComplete) {
  auto properties = MakeSigV4Properties();
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog_session,
                         manager->CatalogSession(client_, properties));

  auto context_result = manager->ContextualSession(
      {{AuthProperties::kSigV4SecretAccessKey, "context-secret"}}, catalog_session);
  EXPECT_THAT(context_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(context_result, HasErrorMessage("must be set together"));

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"db1"}}, .name = "table1"};
  auto table_result = manager->TableSession(
      table_id, {{AuthProperties::kSigV4SessionToken, "table-token"}}, catalog_session);
  EXPECT_THAT(table_result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(table_result, HasErrorMessage("requires"));
}

TEST_F(SigV4AuthTest, CreateCustomDelegateNone) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4DelegateAuthType] = "none";

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      SignRequest(properties,
                  {.method = HttpMethod::kGet, .url = "https://example.com/v1/config"}));

  EXPECT_NE(signed_request.headers.find("authorization"), signed_request.headers.end());
  EXPECT_EQ(signed_request.headers.find("original-authorization"),
            signed_request.headers.end());
}

TEST_F(SigV4AuthTest, CreateInvalidCustomDelegateSigV4Circular) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4DelegateAuthType] = "sigv4";

  auto result = AuthManagers::Load("test-catalog", properties);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result,
              HasErrorMessage("Cannot delegate a SigV4 auth manager to another SigV4"));
}

TEST_F(SigV4AuthTest, ContextualSessionOverridesProperties) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog_session,
                         manager->CatalogSession(client_, properties));

  ICEBERG_UNWRAP_OR_FAIL(
      auto ctx_session,
      manager->ContextualSession({{AuthProperties::kSigV4AccessKeyId, "id2"},
                                  {AuthProperties::kSigV4SecretAccessKey, "secret2"},
                                  {AuthProperties::kSigV4SigningRegion, "eu-west-1"}},
                                 catalog_session));

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      ctx_session->Authenticate(
          {.method = HttpMethod::kGet, .url = "https://example.com/v1/config"}));
  EXPECT_THAT(HeaderValue(signed_request.headers, "authorization"),
              HasSubstr("eu-west-1"));
}

TEST_F(SigV4AuthTest, TableSessionOverridesProperties) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog_session,
                         manager->CatalogSession(client_, properties));

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"db1"}}, .name = "table1"};
  ICEBERG_UNWRAP_OR_FAIL(
      auto table_session,
      manager->TableSession(table_id,
                            {{AuthProperties::kSigV4AccessKeyId, "table-key-id"},
                             {AuthProperties::kSigV4SecretAccessKey, "table-secret"},
                             {AuthProperties::kSigV4SigningRegion, "ap-southeast-1"}},
                            catalog_session));

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      table_session->Authenticate({.method = HttpMethod::kGet,
                                   .url = "https://example.com/v1/db1/tables/table1"}));
  EXPECT_THAT(HeaderValue(signed_request.headers, "authorization"),
              HasSubstr("ap-southeast-1"));
}

TEST_F(SigV4AuthTest, TableSessionIgnoresContextualOverrides) {
  auto properties = MakeSigV4Properties();
  properties[AuthProperties::kSigV4SigningRegion] = "us-west-2";
  ICEBERG_UNWRAP_OR_FAIL(auto manager, AuthManagers::Load("test-catalog", properties));
  ICEBERG_UNWRAP_OR_FAIL(auto catalog_session,
                         manager->CatalogSession(client_, properties));
  ICEBERG_UNWRAP_OR_FAIL(
      auto ctx_session,
      manager->ContextualSession({{AuthProperties::kSigV4SigningRegion, "eu-west-1"}},
                                 catalog_session));

  iceberg::TableIdentifier table_id{.ns = iceberg::Namespace{{"db1"}}, .name = "table1"};
  ICEBERG_UNWRAP_OR_FAIL(auto table_session,
                         manager->TableSession(table_id, {}, ctx_session));

  ICEBERG_UNWRAP_OR_FAIL(
      auto signed_request,
      table_session->Authenticate({.method = HttpMethod::kGet,
                                   .url = "https://example.com/v1/db1/tables/table1"}));
  EXPECT_THAT(HeaderValue(signed_request.headers, "authorization"),
              HasSubstr("us-west-2"));
}

}  // namespace iceberg::rest::auth

#endif  // ICEBERG_SIGV4_ENABLED
