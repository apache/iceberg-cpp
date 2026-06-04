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

#include "iceberg/util/base64.h"

#include <array>
#include <cstdint>

namespace iceberg {

namespace {

// Shared base64 encode logic. The encode alphabet maps a 6-bit value -> ASCII char.
// When `pad` is true, the output is padded with '=' to a multiple of 4 characters.
std::string Base64EncodeWithAlphabet(std::string_view input, std::string_view alphabet,
                                     bool pad) {
  std::string output;
  output.reserve((input.size() + 2) / 3 * 4);

  uint32_t buffer = 0;
  int bits_collected = 0;
  for (unsigned char byte : input) {
    buffer = (buffer << 8) | byte;
    bits_collected += 8;
    while (bits_collected >= 6) {
      bits_collected -= 6;
      output.push_back(alphabet[(buffer >> bits_collected) & 0x3F]);
    }
  }
  if (bits_collected > 0) {
    // Pad the remaining bits on the right to form the final 6-bit group.
    output.push_back(alphabet[(buffer << (6 - bits_collected)) & 0x3F]);
  }
  if (pad) {
    while (output.size() % 4 != 0) {
      output.push_back('=');
    }
  }
  return output;
}

// Shared base64 decode logic. The decode table maps ASCII char -> 6-bit value.
// 0xFF means invalid character.
Result<std::string> Base64DecodeWithTable(std::string_view input,
                                          const std::array<uint8_t, 256>& table) {
  auto padded_size = input.size();
  if (auto padding_pos = input.find('='); padding_pos != std::string_view::npos) {
    auto padding = input.substr(padding_pos);
    if (padding.find_first_not_of('=') != std::string_view::npos) {
      return InvalidArgument("Invalid base64 padding");
    }
    auto padding_size = padding.size();
    if (padding_size > 2 || input.size() % 4 != 0) {
      return InvalidArgument("Invalid base64 padding");
    }
  }

  // Strip trailing padding after validating its count and placement.
  while (!input.empty() && input.back() == '=') {
    input.remove_suffix(1);
  }
  auto unpadded_size = input.size();
  if (unpadded_size % 4 == 1 ||
      (padded_size != unpadded_size && unpadded_size % 4 == 0)) {
    return InvalidArgument("Invalid base64 length");
  }
  if (input.empty()) {
    return std::string{};
  }

  std::string output;
  output.reserve((input.size() * 3) / 4);

  uint32_t buffer = 0;
  int bits_collected = 0;

  for (char c : input) {
    uint8_t val = table[static_cast<uint8_t>(c)];
    if (val == 0xFF) {
      return InvalidArgument("Invalid base64 character: '{}'", c);
    }
    buffer = (buffer << 6) | val;
    bits_collected += 6;
    if (bits_collected >= 8) {
      bits_collected -= 8;
      output.push_back(static_cast<char>((buffer >> bits_collected) & 0xFF));
    }
  }

  return output;
}

// Standard base64 alphabet: A-Z=0-25, a-z=26-51, 0-9=52-61, +=62, /=63
constexpr std::string_view kBase64Chars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// Base64url alphabet: same as standard but '-'=62, '_'=63 (RFC 4648 §5)
constexpr std::string_view kBase64UrlChars =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

// Standard base64 decode table: A-Z=0-25, a-z=26-51, 0-9=52-61, +=62, /=63
constexpr std::array<uint8_t, 256> kBase64DecodeTable = [] {
  std::array<uint8_t, 256> table{};
  table.fill(0xFF);
  for (int i = 0; i < 26; ++i) {
    table[static_cast<size_t>('A' + i)] = static_cast<uint8_t>(i);
    table[static_cast<size_t>('a' + i)] = static_cast<uint8_t>(26 + i);
  }
  for (int i = 0; i < 10; ++i) {
    table[static_cast<size_t>('0' + i)] = static_cast<uint8_t>(52 + i);
  }
  table[static_cast<size_t>('+')] = 62;
  table[static_cast<size_t>('/')] = 63;
  return table;
}();

// Base64url decode table: same as standard but '-'=62, '_'=63 (RFC 4648 §5)
constexpr std::array<uint8_t, 256> kBase64UrlDecodeTable = [] {
  auto table = kBase64DecodeTable;
  table[static_cast<size_t>('+')] = 0xFF;  // '+' is invalid in base64url
  table[static_cast<size_t>('/')] = 0xFF;  // '/' is invalid in base64url
  table[static_cast<size_t>('-')] = 62;
  table[static_cast<size_t>('_')] = 63;
  return table;
}();

}  // namespace

std::string Base64::Encode(std::string_view data) {
  return Base64EncodeWithAlphabet(data, kBase64Chars, /*pad=*/true);
}

Result<std::string> Base64::Decode(std::string_view encoded) {
  return Base64DecodeWithTable(encoded, kBase64DecodeTable);
}

std::string Base64::UrlEncode(std::string_view data) {
  return Base64EncodeWithAlphabet(data, kBase64UrlChars, /*pad=*/false);
}

Result<std::string> Base64::UrlDecode(std::string_view encoded) {
  return Base64DecodeWithTable(encoded, kBase64UrlDecodeTable);
}

}  // namespace iceberg
