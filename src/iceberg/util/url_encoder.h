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

/// \file iceberg/util/url_encoder.h
/// \brief URL encoding and decoding.

namespace iceberg {

/// \brief Utilities for encoding and decoding URLs.
class ICEBERG_EXPORT UrlEncoder {
 public:
  /// \brief URL-encode a string.
  ///
  /// \details This is a simple implementation of url-encode
  /// - Unreserved characters: [A-Z], [a-z], [0-9], "-", "_", ".", "~"
  /// - Space is encoded as "%20" (unlike Java's URLEncoder which uses "+").
  /// - All other characters are percent-encoded (%XX).
  /// \param str_to_encode The string to encode.
  /// \return The URL-encoded string.
  static std::string Encode(std::string_view str_to_encode);

  /// \brief URL-decode a string.
  ///
  /// \details Decodes percent-encoded characters (e.g., "%20" -> space).
  /// \param str_to_decode The encoded string to decode.
  /// \return The decoded string.
  static std::string Decode(std::string_view str_to_decode);
};

}  // namespace iceberg
