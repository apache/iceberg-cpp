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

#include "iceberg/encryption/encryption_util.h"

#include "iceberg/encryption/standard_encryption_manager.h"

namespace iceberg {

Result<std::vector<uint8_t>> EncryptionUtil::DecryptManifestListKeyMetadata(
    const std::shared_ptr<EncryptionManager>& manager,
    std::string_view manifest_list_key_id) {
  auto* standard = dynamic_cast<StandardEncryptionManager*>(manager.get());
  if (standard == nullptr) {
    return NotSupported(
        "Manifest list key metadata decryption requires StandardEncryptionManager");
  }
  return standard->DecryptManifestListKeyMetadata(manifest_list_key_id);
}

}  // namespace iceberg
