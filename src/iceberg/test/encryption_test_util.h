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

#include <cstdint>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "iceberg/test/test_resource.h"

namespace iceberg {

/// \brief Read a binary file under test/resources/encryption/vectors, written by
/// Java's EncryptionVectors with fixed keys.
inline std::vector<uint8_t> ReadEncryptionVector(const std::string& name) {
  std::ifstream in(GetResourcePath("encryption/vectors/" + name), std::ios::binary);
  return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
}

/// \brief Deterministic bytes, the same as Java's EncryptionVectors.bytes().
inline std::vector<uint8_t> VectorBytes(size_t length, int seed) {
  std::vector<uint8_t> bytes(length);
  for (size_t i = 0; i < length; ++i) {
    bytes[i] = static_cast<uint8_t>(static_cast<int>(i) * 31 + seed);
  }
  return bytes;
}

}  // namespace iceberg
