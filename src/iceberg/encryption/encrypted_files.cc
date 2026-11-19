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

#include "iceberg/encryption/encrypted_files.h"

#include <utility>

namespace iceberg {

std::unique_ptr<EncryptedInputFile> EncryptedFiles::Encrypted(
    std::unique_ptr<InputFile> encrypted_file,
    std::shared_ptr<EncryptionKeyMetadata> key_metadata) {
  return std::make_unique<BaseEncryptedInputFile>(std::move(encrypted_file),
                                                  std::move(key_metadata));
}

std::unique_ptr<EncryptedOutputFile> EncryptedFiles::Encrypted(
    std::unique_ptr<OutputFile> output_file,
    std::shared_ptr<EncryptionKeyMetadata> key_metadata) {
  return std::make_unique<BaseEncryptedOutputFile>(std::move(output_file),
                                                   std::move(key_metadata));
}

}  // namespace iceberg
