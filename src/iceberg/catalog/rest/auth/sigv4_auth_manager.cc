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

#include "iceberg/catalog/rest/auth/sigv4_auth_manager.h"

#include <cstdlib>
#include <sstream>

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/http/standard/StandardHttpRequest.h>
#include <aws/core/utils/HashingUtils.h>

#include "iceberg/catalog/rest/auth/auth_properties.h"
#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::rest::auth {

namespace {

/// \brief Ensures AWS SDK is initialized exactly once per process.
/// ShutdownAPI is intentionally never called (leak-by-design) to avoid
/// static destruction order issues with objects that may outlive shutdown.
class AwsSdkGuard {
 public:
  static void EnsureInitialized() {
    static AwsSdkGuard instance;
    (void)instance;
  }

 private:
  AwsSdkGuard() {
    Aws::SDKOptions options;
    Aws::InitAPI(options);
  }
};

Aws::Http::HttpMethod ToAwsMethod(HttpMethod method) {
  switch (method) {
    case HttpMethod::kGet:
      return Aws::Http::HttpMethod::HTTP_GET;
    case HttpMethod::kPost:
      return Aws::Http::HttpMethod::HTTP_POST;
    case HttpMethod::kPut:
      return Aws::Http::HttpMethod::HTTP_PUT;
    case HttpMethod::kDelete:
      return Aws::Http::HttpMethod::HTTP_DELETE;
    case HttpMethod::kHead:
      return Aws::Http::HttpMethod::HTTP_HEAD;
  }
  return Aws::Http::HttpMethod::HTTP_GET;
}

std::unordered_map<std::string, std::string> MergeProperties(
    const std::unordered_map<std::string, std::string>& base,
    const std::unordered_map<std::string, std::string>& overrides) {
  auto merged = base;
  for (const auto& [key, value] : overrides) {
    merged.insert_or_assign(key, value);
  }
  return merged;
}

}  // namespace

// ---- SigV4AuthSession ----

SigV4AuthSession::SigV4AuthSession(
    std::shared_ptr<AuthSession> delegate, std::string signing_region,
    std::string signing_name,
    std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider)
    : delegate_(std::move(delegate)),
      signing_region_(std::move(signing_region)),
      signing_name_(std::move(signing_name)),
      credentials_provider_(std::move(credentials_provider)),
      signer_(std::make_unique<Aws::Client::AWSAuthV4Signer>(
          credentials_provider_, signing_name_.c_str(), signing_region_.c_str(),
          Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Always,
          /*urlEscapePath=*/false)) {}

SigV4AuthSession::~SigV4AuthSession() = default;

Status SigV4AuthSession::Authenticate(
    std::unordered_map<std::string, std::string>& headers,
    const HTTPRequestContext& request_context) {
  // 1. Delegate authenticates first (e.g., adds OAuth2 Bearer token).
  ICEBERG_RETURN_UNEXPECTED(delegate_->Authenticate(headers, request_context));

  auto original_headers = headers;

  // 2. Relocate Authorization header (case-insensitive) so SigV4 takes precedence.
  std::unordered_map<std::string, std::string> signing_headers;
  for (const auto& [name, value] : headers) {
    if (StringUtils::EqualsIgnoreCase(name, "Authorization")) {
      signing_headers[std::string(kRelocatedHeaderPrefix) + name] = value;
    } else {
      signing_headers[name] = value;
    }
  }

  // 3. Build AWS SDK request.
  Aws::Http::URI aws_uri(request_context.url.c_str());
  auto aws_request = std::make_shared<Aws::Http::Standard::StandardHttpRequest>(
      aws_uri, ToAwsMethod(request_context.method));

  for (const auto& [name, value] : signing_headers) {
    aws_request->SetHeaderValue(Aws::String(name.c_str()), Aws::String(value.c_str()));
  }

  // 4. Set body content hash (matching Java's RESTSigV4AuthSession).
  // Empty body: set EMPTY_BODY_SHA256 explicitly (Java line 118-121 workaround).
  // Non-empty body: set body stream; the signer (PayloadSigningPolicy::Always)
  // computes the real hex hash. Step 7 converts hex to Base64 after signing.
  if (request_context.body.empty()) {
    aws_request->SetHeaderValue("x-amz-content-sha256", Aws::String(kEmptyBodySha256));
  } else {
    auto body_stream =
        Aws::MakeShared<std::stringstream>("SigV4Body", request_context.body);
    aws_request->AddContentBody(body_stream);
  }

  // 5. Sign.
  if (!signer_->SignRequest(*aws_request)) {
    return std::unexpected<Error>(
        Error{ErrorKind::kAuthenticationFailed, "SigV4 signing failed"});
  }

  // 6. Extract signed headers, relocating conflicts with originals.
  headers.clear();
  auto signed_headers = aws_request->GetHeaders();
  for (auto it = signed_headers.begin(); it != signed_headers.end(); ++it) {
    std::string name_str(it->first.c_str(), it->first.size());
    std::string value_str(it->second.c_str(), it->second.size());

    for (const auto& [orig_name, orig_value] : original_headers) {
      if (StringUtils::EqualsIgnoreCase(orig_name, name_str) && orig_value != value_str) {
        headers[std::string(kRelocatedHeaderPrefix) + orig_name] = orig_value;
        break;
      }
    }

    headers[name_str] = value_str;
  }

  // 7. Convert body hash from hex to Base64 (matching Java's SignerChecksumParams
  // output). Only convert if the value is a valid hex SHA256 (64 hex chars).
  if (!request_context.body.empty()) {
    auto it = headers.find("x-amz-content-sha256");
    if (it != headers.end() && it->second.size() == 64 &&
        it->second != std::string(kEmptyBodySha256)) {
      auto decoded = Aws::Utils::HashingUtils::HexDecode(Aws::String(it->second.c_str()));
      it->second = std::string(Aws::Utils::HashingUtils::Base64Encode(decoded).c_str());
    }
  }

  return {};
}

Status SigV4AuthSession::Close() { return delegate_->Close(); }

// ---- SigV4AuthManager ----

SigV4AuthManager::SigV4AuthManager(std::unique_ptr<AuthManager> delegate)
    : delegate_(std::move(delegate)) {}

SigV4AuthManager::~SigV4AuthManager() = default;

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::InitSession(
    HttpClient& init_client,
    const std::unordered_map<std::string, std::string>& properties) {
  AwsSdkGuard::EnsureInitialized();
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->InitSession(init_client, properties));
  return WrapSession(std::move(delegate_session), properties);
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::CatalogSession(
    HttpClient& shared_client,
    const std::unordered_map<std::string, std::string>& properties) {
  AwsSdkGuard::EnsureInitialized();
  catalog_properties_ = properties;
  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session,
                          delegate_->CatalogSession(shared_client, properties));
  return WrapSession(std::move(delegate_session), properties);
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::ContextualSession(
    const std::unordered_map<std::string, std::string>& context,
    std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = internal::checked_pointer_cast<SigV4AuthSession>(std::move(parent));

  ICEBERG_ASSIGN_OR_RAISE(auto delegate_session, delegate_->ContextualSession(
                                                     context, sigv4_parent->delegate()));

  auto merged = MergeProperties(catalog_properties_, context);
  return WrapSession(std::move(delegate_session), merged);
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::TableSession(
    const TableIdentifier& table,
    const std::unordered_map<std::string, std::string>& properties,
    std::shared_ptr<AuthSession> parent) {
  auto sigv4_parent = internal::checked_pointer_cast<SigV4AuthSession>(std::move(parent));

  ICEBERG_ASSIGN_OR_RAISE(
      auto delegate_session,
      delegate_->TableSession(table, properties, sigv4_parent->delegate()));

  auto merged = MergeProperties(catalog_properties_, properties);
  return WrapSession(std::move(delegate_session), merged);
}

Status SigV4AuthManager::Close() { return delegate_->Close(); }

Result<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>
SigV4AuthManager::MakeCredentialsProvider(
    const std::unordered_map<std::string, std::string>& properties) {
  auto access_key_it = properties.find(AuthProperties::kSigV4AccessKeyId);
  auto secret_key_it = properties.find(AuthProperties::kSigV4SecretAccessKey);
  bool has_ak = access_key_it != properties.end() && !access_key_it->second.empty();
  bool has_sk = secret_key_it != properties.end() && !secret_key_it->second.empty();

  // Reject partial credentials — providing only one of AK/SK is a misconfiguration.
  ICEBERG_PRECHECK(
      has_ak == has_sk, "Both '{}' and '{}' must be set together, or neither",
      AuthProperties::kSigV4AccessKeyId, AuthProperties::kSigV4SecretAccessKey);

  if (has_ak) {
    Aws::Auth::AWSCredentials credentials(access_key_it->second.c_str(),
                                          secret_key_it->second.c_str());
    auto session_token_it = properties.find(AuthProperties::kSigV4SessionToken);
    if (session_token_it != properties.end() && !session_token_it->second.empty()) {
      credentials.SetSessionToken(session_token_it->second.c_str());
    }
    return std::make_shared<Aws::Auth::SimpleAWSCredentialsProvider>(credentials);
  }

  return std::make_shared<Aws::Auth::DefaultAWSCredentialsProviderChain>();
}

std::string SigV4AuthManager::ResolveSigningRegion(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(AuthProperties::kSigV4SigningRegion);
  if (it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  auto legacy_it = properties.find(AuthProperties::kSigV4Region);
  if (legacy_it != properties.end() && !legacy_it->second.empty()) {
    return legacy_it->second;
  }
  if (const char* env = std::getenv("AWS_REGION")) {
    return std::string(env);
  }
  if (const char* env = std::getenv("AWS_DEFAULT_REGION")) {
    return std::string(env);
  }
  return "us-east-1";
}

std::string SigV4AuthManager::ResolveSigningName(
    const std::unordered_map<std::string, std::string>& properties) {
  auto it = properties.find(AuthProperties::kSigV4SigningName);
  if (it != properties.end() && !it->second.empty()) {
    return it->second;
  }
  auto legacy_it = properties.find(AuthProperties::kSigV4Service);
  if (legacy_it != properties.end() && !legacy_it->second.empty()) {
    return legacy_it->second;
  }
  return AuthProperties::kSigV4SigningNameDefault;
}

Result<std::shared_ptr<AuthSession>> SigV4AuthManager::WrapSession(
    std::shared_ptr<AuthSession> delegate_session,
    const std::unordered_map<std::string, std::string>& properties) {
  auto region = ResolveSigningRegion(properties);
  auto service = ResolveSigningName(properties);
  ICEBERG_ASSIGN_OR_RAISE(auto credentials, MakeCredentialsProvider(properties));
  return std::make_shared<SigV4AuthSession>(std::move(delegate_session),
                                            std::move(region), std::move(service),
                                            std::move(credentials));
}

}  // namespace iceberg::rest::auth
