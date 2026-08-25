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

/// \file iceberg/arrow/s3/s3_properties.h
/// \brief Define S3 configuration property keys.

#include <algorithm>
#include <array>
#include <string_view>

namespace iceberg::arrow {

/// \brief S3 configuration property keys for ArrowS3FileIO.
///
/// These constants define the property keys used to configure S3 access
/// via the Arrow filesystem integration, following the Iceberg spec for
/// S3 configuration properties.
struct S3Properties {
  /// S3 URI scheme
  static constexpr std::string_view kS3Schema = "s3";
  /// AWS access key ID
  static constexpr std::string_view kAccessKeyId = "s3.access-key-id";
  /// AWS secret access key
  static constexpr std::string_view kSecretAccessKey = "s3.secret-access-key";
  /// AWS session token (for temporary credentials)
  static constexpr std::string_view kSessionToken = "s3.session-token";
  /// AWS region, standard Iceberg client property.
  static constexpr std::string_view kClientRegion = "client.region";
  /// Custom endpoint override (for MinIO, LocalStack, etc.)
  static constexpr std::string_view kEndpoint = "s3.endpoint";
  /// Whether to use path-style access (needed for MinIO)
  static constexpr std::string_view kPathStyleAccess = "s3.path-style-access";
  /// Whether SSL is enabled
  static constexpr std::string_view kSslEnabled = "s3.ssl.enabled";
  /// Connection timeout in milliseconds
  static constexpr std::string_view kConnectTimeoutMs = "s3.connect-timeout-ms";
  /// Socket timeout in milliseconds
  static constexpr std::string_view kSocketTimeoutMs = "s3.socket-timeout-ms";
};

/// \brief URI schemes served by the Arrow S3 FileIO, lower-case.
///
/// Single source of truth: registration, IsS3Scheme and alias canonicalization
/// all derive from this list, so a new alias only has to be added here.
///
/// `oss` is served because the store is S3-compatible; see the FileIO docs.
inline constexpr std::array<std::string_view, 4> kS3Schemes = {"s3", "s3a", "s3n", "oss"};

/// \brief ASCII-only case-insensitive comparison: schemes are ASCII (RFC 3986),
/// and <cctype> is locale-sensitive.
inline constexpr bool EqualsIgnoreAsciiCase(std::string_view left,
                                            std::string_view right) {
  constexpr auto lower = [](char c) {
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c - 'A' + 'a') : c;
  };
  return std::ranges::equal(left, right,
                            [&](char a, char b) { return lower(a) == lower(b); });
}

/// \brief Return whether a URI scheme is S3-compatible; case-insensitive,
/// because scheme routing is.
inline constexpr bool IsS3Scheme(std::string_view scheme) {
  return std::ranges::any_of(kS3Schemes, [scheme](std::string_view alias) {
    return EqualsIgnoreAsciiCase(scheme, alias);
  });
}

/// \brief Return whether a storage credential prefix belongs to S3.
///
/// Accepts the bare `s3` prefix (exactly: a case variant would be stored as a
/// key no canonicalized location can match) or a URI prefix such as
/// `s3a://bucket`. Bare aliases such as `oss` are deliberately rejected,
/// matching Java S3FileIO's credential filter.
inline constexpr bool IsS3CredentialPrefix(std::string_view prefix) {
  if (prefix == S3Properties::kS3Schema) {
    return true;
  }
  const auto delimiter = prefix.find("://");
  return delimiter != std::string_view::npos && IsS3Scheme(prefix.substr(0, delimiter));
}

}  // namespace iceberg::arrow
