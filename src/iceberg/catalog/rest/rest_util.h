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
#include <unordered_map>

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::rest {

/// \brief Trim trailing slashes from a string.
///
/// \param str A string to trim.
/// \return The trimmed string with all trailing slashes removed.
ICEBERG_REST_EXPORT std::string_view TrimTrailingSlash(std::string_view str);

/// \brief URL-encode a string (RFC 3986).
///
/// \details This implementation uses libcurl (via CPR), which follows RFC 3986 strictly:
/// - Unreserved characters: [A-Z], [a-z], [0-9], "-", "_", ".", "~"
/// - Space is encoded as "%20" (unlike Java's URLEncoder which uses "+").
/// - All other characters are percent-encoded (%XX).
/// \param str_to_encode The string to encode.
/// \return The URL-encoded string or InvalidArgument if the string is invalid.
ICEBERG_REST_EXPORT Result<std::string> EncodeString(std::string_view str_to_encode);

/// \brief URL-decode a string.
///
/// \details Decodes percent-encoded characters (e.g., "%20" -> space). Uses libcurl's URL
/// decoding via the CPR library.
/// \param str_to_decode The encoded string to decode.
/// \return The decoded string or InvalidArgument if the string is invalid.
ICEBERG_REST_EXPORT Result<std::string> DecodeString(std::string_view str_to_decode);

/// \brief Encode a Namespace into a URL-safe component.
///
/// \details Encodes each level separately using EncodeString, then joins them with "%1F".
/// \param ns_to_encode The namespace to encode.
/// \return The percent-encoded namespace string suitable for URLs.
ICEBERG_REST_EXPORT Result<std::string> EncodeNamespace(const Namespace& ns_to_encode);

/// \brief Decode a URL-encoded namespace string back to a Namespace.
///
/// \details Splits by "%1F" (the URL-encoded form of ASCII Unit Separator), then decodes
/// each level separately using DecodeString.
/// \param str_to_decode The percent-encoded namespace string.
/// \return The decoded Namespace.
ICEBERG_REST_EXPORT Result<Namespace> DecodeNamespace(std::string_view str_to_decode);

/// \brief Merge catalog configuration properties.
///
/// \details Merges three sets of configuration properties following the precedence order:
/// server overrides > client configs > server defaults.
/// \param server_defaults Default properties provided by the server.
/// \param client_configs Configuration properties from the client.
/// \param server_overrides Override properties enforced by the server.
/// \return A merged map containing all properties with correct precedence.
ICEBERG_REST_EXPORT std::unordered_map<std::string, std::string> MergeConfigs(
    const std::unordered_map<std::string, std::string>& server_defaults,
    const std::unordered_map<std::string, std::string>& client_configs,
    const std::unordered_map<std::string, std::string>& server_overrides);

/// \brief Get the standard HTTP reason phrase for a status code.
///
/// \details Returns the standard English reason phrase for common HTTP status codes.
/// For unknown status codes, returns a generic "HTTP {code}" message.
/// \param status_code The HTTP status code (e.g., 200, 404, 500).
/// \return The standard reason phrase string (e.g., "OK", "Not Found", "Internal Server
/// Error").
ICEBERG_REST_EXPORT std::string GetStandardReasonPhrase(int32_t status_code);

}  // namespace iceberg::rest
