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

#include <utf8proc.h>

#include <array>

#include "iceberg/util/macros.h"

namespace iceberg {

std::string StringUtils::ToLower(std::string_view str) {
  std::string result;
  result.reserve(str.size());

  // Lower-case ASCII bytes directly; hand non-ASCII bytes to utf8proc. The common inputs
  // (modes, UUIDs, header/property names, enum-like strings) are pure ASCII and never
  // touch utf8proc. utf8proc has no string-level helper, so each non-ASCII code point is
  // decoded, mapped with utf8proc_tolower (simple 1:1 mapping, not casefolding), and
  // re-encoded.
  const auto* data = reinterpret_cast<const utf8proc_uint8_t*>(str.data());
  const auto size = static_cast<utf8proc_ssize_t>(str.size());
  utf8proc_ssize_t offset = 0;
  while (offset < size) {
    // An ASCII byte is a complete 1-byte code point (never a UTF-8 continuation byte),
    // and utf8proc_tolower agrees with ToLowerAscii on it, so handle it without utf8proc.
    if (IsAsciiByte(str[offset])) {
      result.push_back(ToLowerAscii(str[offset]));
      ++offset;
      continue;
    }
    utf8proc_int32_t code_point = 0;
    utf8proc_ssize_t consumed =
        utf8proc_iterate(data + offset, size - offset, &code_point);
    if (consumed < 0) {
      // Invalid UTF-8: pass the offending byte through unchanged and resume decoding at
      // the next byte, so the valid code points around it are still lower-cased.
      result.push_back(str[offset]);
      ++offset;
      continue;
    }
    const utf8proc_int32_t lowered = utf8proc_tolower(code_point);
    std::array<utf8proc_uint8_t, 4> encoded{};
    const utf8proc_ssize_t written = utf8proc_encode_char(lowered, encoded.data());
    result.append(reinterpret_cast<const char*>(encoded.data()),
                  static_cast<size_t>(written));
    offset += consumed;
  }
  return result;
}

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
