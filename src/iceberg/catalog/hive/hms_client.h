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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/catalog/hive/hive_catalog_properties.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/hive/hms_client.h
/// \brief Thin wrapper around the generated Hive Metastore Thrift client.
///
/// Thrift types are kept out of the public interface via a pImpl.

namespace iceberg::hive {

/// \brief Hive's well-known metastore port, used when an HMS URI omits one.
inline constexpr int kDefaultHmsPort = 9083;

/// \brief A single host:port pair parsed from an HMS URI.
///
/// HMS URIs commonly take one of the forms:
///   * `thrift://host:port`        (Java HiveCatalog convention)
///   * `host:port`                 (iceberg-rust HmsCatalog convention)
///   * comma-separated list of either form for HA failover
///
/// `port` defaults to 9083 (Hive's well-known metastore port) when the
/// caller omits an explicit port.
struct ICEBERG_HIVE_EXPORT HmsEndpoint {
  std::string host;
  int port = kDefaultHmsPort;
};

/// \brief Parse an HMS URI string into one or more endpoints.
///
/// Each comma-separated segment is treated as an independent endpoint;
/// surrounding whitespace and an optional `thrift://` scheme prefix are
/// stripped. Returns an InvalidArgument error if any segment fails to
/// produce a non-empty host and a port within (0, 65535].
ICEBERG_HIVE_EXPORT Result<std::vector<HmsEndpoint>> ParseHmsUris(std::string_view uri);

/// \brief A live connection to a Hive Metastore over Thrift.
///
/// Construction goes through `Connect`, which parses the URI, selects the
/// transport and opens it so that configuration errors surface as
/// `iceberg::Error` rather than C++ exceptions.
class ICEBERG_HIVE_EXPORT HmsClient {
 public:
  ~HmsClient();

  HmsClient(const HmsClient&) = delete;
  HmsClient& operator=(const HmsClient&) = delete;
  HmsClient(HmsClient&&) = delete;
  HmsClient& operator=(HmsClient&&) = delete;

  /// \brief Connect to the Hive Metastore described by `config`.
  ///
  /// Exactly one endpoint is supported: a multi-endpoint `config.Uri()`
  /// returns InvalidArgument until HA failover is implemented in a future
  /// commit.
  static Result<std::unique_ptr<HmsClient>> Connect(const HiveCatalogProperties& config);

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;

  explicit HmsClient(std::unique_ptr<Impl> impl);
};

}  // namespace iceberg::hive
