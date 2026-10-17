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

/// \file iceberg/encryption/key_management_client.h
/// Client of a key management service (KMS), and the registry that creates one from
/// catalog properties.

#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Wraps and unwraps secret keys with master keys kept in a KMS.
///
/// Mirrors Java's org.apache.iceberg.encryption.KeyManagementClient. The table master
/// key (table property `encryption.key-id`) never leaves the KMS; it only wraps and
/// unwraps the table's key encryption keys.
class ICEBERG_EXPORT KeyManagementClient {
 public:
  /// \brief A generated key and its wrapped form.
  struct GeneratedKey {
    std::vector<uint8_t> key;
    std::vector<uint8_t> wrapped_key;
  };

  virtual ~KeyManagementClient() = default;

  /// \brief Initialize the client with the catalog properties.
  virtual Status Initialize(
      const std::unordered_map<std::string, std::string>& properties) {
    return {};
  }

  /// \brief Wrap (encrypt) a secret key with the master key `wrapping_key_id`.
  ///
  /// The result may include KMS-specific metadata needed to unwrap it.
  virtual Result<std::vector<uint8_t>> WrapKey(std::span<const uint8_t> key,
                                               std::string_view wrapping_key_id) = 0;

  /// \brief Unwrap a key returned by WrapKey.
  virtual Result<std::vector<uint8_t>> UnwrapKey(std::span<const uint8_t> wrapped_key,
                                                 std::string_view wrapping_key_id) = 0;

  /// \brief Whether the KMS can generate keys (GenerateKey).
  virtual bool SupportsKeyGeneration() const { return false; }

  /// \brief Generate a new key in the KMS, returned in plain and wrapped form.
  virtual Result<GeneratedKey> GenerateKey(std::string_view wrapping_key_id) {
    return NotSupported("Key generation is not supported by this KMS client");
  }
};

/// \brief Creates KMS clients by name.
///
/// Catalogs create their KMS client from the catalog properties, with the same
/// property names as Java:
/// - `encryption.kms-type`: a built-in client, `aws`, `gcp` or `azure`;
/// - `encryption.kms-impl`: the name a custom client was registered under.
///
/// Java also accepts the type `aliyun`, which has no client here; register one with
/// Register and select it through `encryption.kms-impl`.
///
/// Built-in clients register themselves when their library is linked and initialized;
/// applications register custom clients before creating catalogs.
class ICEBERG_EXPORT KmsRegistry {
 public:
  static constexpr std::string_view kKmsType = "encryption.kms-type";
  static constexpr std::string_view kKmsImpl = "encryption.kms-impl";
  static constexpr std::string_view kKmsTypeAws = "aws";
  static constexpr std::string_view kKmsTypeGcp = "gcp";
  static constexpr std::string_view kKmsTypeAzure = "azure";

  using Properties = std::unordered_map<std::string, std::string>;
  using Factory = std::function<Result<std::unique_ptr<KeyManagementClient>>()>;

  /// \brief Register a client factory under `name`, replacing any previous one.
  static void Register(std::string name, Factory factory);

  /// \brief Create and initialize the client registered under `name`.
  static Result<std::shared_ptr<KeyManagementClient>> Load(std::string_view name,
                                                           const Properties& properties);

  /// \brief Create the KMS client configured by catalog properties.
  ///
  /// \return the initialized client, or nullptr if neither `encryption.kms-type` nor
  /// `encryption.kms-impl` is set.
  static Result<std::shared_ptr<KeyManagementClient>> FromCatalogProperties(
      const Properties& properties);
};

}  // namespace iceberg
