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

/// \brief Initialize the AWS SDK for SigV4 use. Idempotent.
///
/// Normal REST SigV4 users do not need to call this. SigV4 sessions lazily
/// initialize the SDK when needed. This hook exists for tests and for explicit
/// process-shutdown sequencing when an embedding application needs it.
ICEBERG_REST_EXPORT Status InitializeAwsSdk();

/// \brief Shut down the SigV4-owned AWS SDK lifecycle.
///
/// Refuses if any SigV4 sessions are alive.
ICEBERG_REST_EXPORT Status FinalizeAwsSdk();

ICEBERG_REST_EXPORT bool IsAwsSdkInitialized();
ICEBERG_REST_EXPORT bool IsAwsSdkFinalized();

/// \brief An AuthSession that signs requests with AWS SigV4.
///
/// The request is first authenticated by the delegate AuthSession (e.g., OAuth2),
/// then signed with SigV4. In case of conflicting headers, the Authorization header
/// set by the delegate is relocated with an "Original-" prefix, then included in
/// the canonical headers to sign.
///
/// See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv.html
///
/// Thread safety: Authenticate() is thread-safe as long as the delegate
/// session is.
class ICEBERG_REST_EXPORT SigV4AuthSession : public AuthSession {
 public:
  /// SHA-256 hash of empty string, used for requests with no body.
  static constexpr std::string_view kEmptyBodySha256 =
      "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

  /// Prefix prepended to relocated headers that conflict with SigV4-signed headers.
  static constexpr std::string_view kRelocatedHeaderPrefix = "Original-";

  /// \brief Creates a session registered with the AWS SDK lifecycle.
  ///
  /// Fails if the SDK is not initialized. Every session owns its lifecycle
  /// registration and unregisters on destruction.
  static Result<std::shared_ptr<SigV4AuthSession>> Make(
      std::shared_ptr<AuthSession> delegate, std::string signing_region,
      std::string signing_name,
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider);

  ~SigV4AuthSession() override;

  Result<HttpRequest> Authenticate(HttpRequest request) override;

  Status Close() override;

  const std::shared_ptr<AuthSession>& delegate() const { return delegate_; }

  /// Exposed so derived sessions can reuse the chain instead of constructing
  /// a fresh DefaultAWSCredentialsProviderChain per derivation.
  const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& credentials_provider() const {
    return credentials_provider_;
  }

 private:
  SigV4AuthSession(
      std::shared_ptr<AuthSession> delegate, std::string signing_region,
      std::string signing_name,
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider);

  std::shared_ptr<AuthSession> delegate_;
  std::string signing_region_;
  std::string signing_name_;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider_;
  std::unique_ptr<Aws::Client::AWSAuthV4Signer> signer_;
};

/// \brief An AuthManager that produces SigV4AuthSession instances.
///
/// Wraps a delegate AuthManager to handle double authentication (e.g., OAuth2 + SigV4).
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
  Result<std::shared_ptr<AuthSession>> WrapSession(
      std::shared_ptr<AuthSession> delegate_session,
      const std::unordered_map<std::string, std::string>& properties,
      std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider);

  std::unique_ptr<AuthManager> delegate_;
  std::unordered_map<std::string, std::string> catalog_properties_;
};

}  // namespace iceberg::rest::auth
