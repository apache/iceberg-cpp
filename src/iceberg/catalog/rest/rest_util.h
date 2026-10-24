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

/// \file iceberg/catalog/rest/rest_util.h
/// \brief Provide utilities for REST catalog path and config handling.

#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/catalog/rest/endpoint.h"
#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/type_fwd.h"
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
/// \details Encodes each level separately using EncodeString, then joins them with the
/// provided separator. The default matches the REST spec's historical "%1F".
/// \param ns_to_encode The namespace to encode.
/// \param separator The encoded separator to place between namespace levels.
/// \return The percent-encoded namespace string suitable for URLs.
ICEBERG_REST_EXPORT Result<std::string> EncodeNamespace(
    const Namespace& ns_to_encode, std::string_view separator = "%1F");

/// \brief Decode a URL-encoded namespace string back to a Namespace.
///
/// \details Splits by the provided separator, then decodes each level separately using
/// DecodeString.
/// \param str_to_decode The percent-encoded namespace string.
/// \param separator The encoded separator used between namespace levels.
/// \return The decoded Namespace.
ICEBERG_REST_EXPORT Result<Namespace> DecodeNamespace(std::string_view str_to_decode,
                                                      std::string_view separator = "%1F");

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

/// \brief Parse the rest.client-only-properties value.
///
/// \details Parses a comma-separated list of property keys. Each key is taken from
/// client configuration only; server-provided values for them are ignored. Leading
/// and trailing whitespace is stripped from each key, and empty entries are ignored.
/// \param value The raw string value of rest.client-only-properties.
/// \return A set of property keys to protect from server override.
ICEBERG_REST_EXPORT std::unordered_set<std::string> ParseClientOnlyProperties(
    std::string_view value);

/// \brief Resolve the effective catalog configuration from server-provided
/// defaults/overrides and client configuration.
///
/// \details Keys listed in rest.client-only-properties are taken from client
/// configuration only: server-provided values for them are ignored. If
/// oauth2-server-uri is listed, it is resolved to an absolute URL before merging
/// to prevent indirect override via the uri property.
/// \param server_defaults Default properties provided by the server.
/// \param server_overrides Override properties enforced by the server.
/// \param client_configs Configuration properties from the client.
/// \return The merged configuration map with client-only properties protected.
ICEBERG_REST_EXPORT Result<std::unordered_map<std::string, std::string>>
MergeServerConfig(const std::unordered_map<std::string, std::string>& server_defaults,
                  const std::unordered_map<std::string, std::string>& server_overrides,
                  const std::unordered_map<std::string, std::string>& client_configs);

/// \brief Get the standard HTTP reason phrase for a status code.
///
/// \details Returns the standard English reason phrase for common HTTP status codes.
/// For unknown status codes, returns a generic "HTTP {code}" message.
/// \param status_code The HTTP status code (e.g., 200, 404, 500).
/// \return The standard reason phrase string (e.g., "OK", "Not Found", "Internal Server
/// Error").
ICEBERG_REST_EXPORT std::string GetStandardReasonPhrase(int32_t status_code);

}  // namespace iceberg::rest
