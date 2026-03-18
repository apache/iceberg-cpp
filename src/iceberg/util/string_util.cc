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

#include "iceberg/util/string_util.h"

#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::vector<uint8_t>> StringUtils::HexStringToBytes(std::string_view hex) {
  if (hex.size() % 2 != 0) [[unlikely]] {
    return InvalidArgument("Hex string must have even length, got: {}", hex.size());
  }
  std::vector<uint8_t> bytes;
  bytes.reserve(hex.size() / 2);
  auto nibble = [](char c) -> Result<uint8_t> {
    if (c >= '0' && c <= '9') return static_cast<uint8_t>(c - '0');
    if (c >= 'a' && c <= 'f') return static_cast<uint8_t>(c - 'a' + 10);
    if (c >= 'A' && c <= 'F') return static_cast<uint8_t>(c - 'A' + 10);
    return InvalidArgument("Invalid hex character: '{}'", c);
  };
  for (size_t i = 0; i < hex.size(); i += 2) {
    ICEBERG_ASSIGN_OR_RAISE(auto hi, nibble(hex[i]));
    ICEBERG_ASSIGN_OR_RAISE(auto lo, nibble(hex[i + 1]));
    bytes.push_back(static_cast<uint8_t>((hi << 4) | lo));
  }
  return bytes;
}

}  // namespace iceberg
