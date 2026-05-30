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

#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

/// \file iceberg/util/base64.h
/// \brief Base64 and base64url encoding/decoding (RFC 4648).

namespace iceberg {

/// \brief Utilities for base64 and base64url encoding and decoding.
class ICEBERG_EXPORT Base64 {
 public:
  /// \brief Base64 encode a string (standard alphabet: +/).
  ///
  /// The output is padded with '=' to a multiple of 4 characters.
  ///
  /// \param data The string to encode.
  /// \return The base64-encoded string.
  static std::string Encode(std::string_view data);

  /// \brief Base64 decode a string (standard alphabet: +/).
  ///
  /// Handles optional padding ('=').
  /// \param encoded The base64-encoded string.
  /// \return Decoded string, or an error if the input contains invalid characters.
  static Result<std::string> Decode(std::string_view encoded);

  /// \brief Base64url encode a string (URL-safe alphabet: -_).
  ///
  /// This variant uses '-' and '_' instead of '+' and '/' per RFC 4648 §5 and
  /// emits no '=' padding, matching the encoding commonly used by JWTs.
  ///
  /// \param data The string to encode.
  /// \return The base64url-encoded string (without padding).
  static std::string UrlEncode(std::string_view data);

  /// \brief Base64url decode a string (URL-safe alphabet: -_).
  ///
  /// Handles optional padding ('='). This variant uses '-' and '_' instead of
  /// '+' and '/' per RFC 4648 §5.
  /// \param encoded The base64url-encoded string.
  /// \return Decoded string, or an error if the input contains invalid characters.
  static Result<std::string> UrlDecode(std::string_view encoded);
};

}  // namespace iceberg
