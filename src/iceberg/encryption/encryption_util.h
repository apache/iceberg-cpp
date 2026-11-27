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

/// \file iceberg/encryption/encryption_util.h
/// Helpers that reach standard-specific encryption operations through the generic
/// EncryptionManager interface, mirroring Java's EncryptionUtil.

#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

#include "iceberg/encryption/encryption_manager.h"
#include "iceberg/iceberg_bundle_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief Standard-specific operations reached through a generic EncryptionManager.
class ICEBERG_BUNDLE_EXPORT EncryptionUtil {
 public:
  /// \brief Decrypt manifest list key metadata, as stored in a snapshot's `key-id`.
  ///
  /// Fails with NotSupported if manager is not backed by StandardEncryptionManager.
  static Result<std::vector<uint8_t>> DecryptManifestListKeyMetadata(
      const std::shared_ptr<EncryptionManager>& manager,
      std::string_view manifest_list_key_id);
};

}  // namespace iceberg
