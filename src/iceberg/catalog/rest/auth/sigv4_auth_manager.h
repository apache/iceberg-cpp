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

#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "iceberg/catalog/rest/auth/auth_manager.h"
#include "iceberg/catalog/rest/auth/auth_session.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"

namespace Aws::Auth {
class AWSCredentialsProvider;
}  // namespace Aws::Auth

namespace Aws::Client {
class AWSAuthV4Signer;
}  // namespace Aws::Client

namespace iceberg::rest::auth {

/// \brief An AuthSession that signs requests with AWS SigV4.
///
/// The request is first authenticated by the delegate AuthSession (e.g., OAuth2),
/// then signed with SigV4. In case of conflicting headers, the Authorization header
/// set by the delegate is relocated with an "Original-" prefix, then included in
/// the canonical headers to sign.
///
/// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html
///
/// Thread safety: Authenticate() is NOT thread-safe. Each session should be used
/// from a single thread, or callers must synchronize externally.
class ICEBERG_REST_EXPORT SigV4AuthSession : public AuthSession {
 public:
  /// SHA-256 hash of empty string, used for requests with no body.
  static constexpr std::string_view kEmptyBodySha256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  /// Prefix prepended to relocated headers that conflict with SigV4-signed headers.
  static constexpr std::string_view kRelocatedHeaderPrefix = "Original-";

  SigV4AuthSession(
      std::shared_ptr<AuthSession> delegate, std::string signing_region,
      std::string signing_name,
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider);

  ~SigV4AuthSession() override;

  Status Authenticate(std::unordered_map<std::string, std::string>& headers,
                      const HTTPRequestContext& request_context) override;

  Status Close() override;

  const std::shared_ptr<AuthSession>& delegate() const { return delegate_; }

 private:
  std::shared_ptr<AuthSession> delegate_;
  std::string signing_region_;
  std::string signing_name_;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider_;
  /// Shared signer instance, matching Java's single Aws4Signer per manager.
  std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
};

/// \brief An AuthManager that produces SigV4AuthSession instances.
///
/// Wraps a delegate AuthManager to handle double authentication (e.g., OAuth2 + SigV4).
///
/// Thread safety: CatalogSession() must be called before ContextualSession() or
/// TableSession(). Concurrent calls are NOT safe — callers must synchronize externally.
class ICEBERG_REST_EXPORT SigV4AuthManager : public AuthManager {
 public:
  explicit SigV4AuthManager(std::unique_ptr<AuthManager> delegate);
  ~SigV4AuthManager() override;

  Result<std::shared_ptr<AuthSession>> InitSession(
      HttpClient& init_client,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::shared_ptr<AuthSession>> CatalogSession(
      HttpClient& shared_client,
      const std::unordered_map<std::string, std::string>& properties) override;

  Result<std::shared_ptr<AuthSession>> ContextualSession(
      const std::unordered_map<std::string, std::string>& context,
      std::shared_ptr<AuthSession> parent) override;

  Result<std::shared_ptr<AuthSession>> TableSession(
      const TableIdentifier& table,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<AuthSession> parent) override;

  Status Close() override;

 private:
  static Result<std::shared_ptr<Aws::Auth::AWSCredentialsProvider>>
  MakeCredentialsProvider(const std::unordered_map<std::string, std::string>& properties);
  static std::string ResolveSigningRegion(
      const std::unordered_map<std::string, std::string>& properties);
  static std::string ResolveSigningName(
      const std::unordered_map<std::string, std::string>& properties);
  Result<std::shared_ptr<AuthSession>> WrapSession(
      std::shared_ptr<AuthSession> delegate_session,
      const std::unordered_map<std::string, std::string>& properties);

  std::unique_ptr<AuthManager> delegate_;
  std::unordered_map<std::string, std::string> catalog_properties_;
};

}  // namespace iceberg::rest::auth
