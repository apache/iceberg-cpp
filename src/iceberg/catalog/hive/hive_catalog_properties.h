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
#include <unordered_map>

#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"
#include "iceberg/util/config.h"

/// \file iceberg/catalog/hive/hive_catalog_properties.h
/// \brief Configuration for connecting to a Hive Metastore (HMS) over Thrift.

namespace iceberg::hive {

/// \brief Thrift framing mode used to connect to the Hive Metastore.
///
/// Most HMS deployments default to TBufferedTransport. TFramedTransport is
/// required by HMS instances that have been configured to use framed
/// transport (for example, certain Hive 3.x setups with SASL enabled).
enum class HiveThriftTransport : uint8_t { kBuffered, kFramed };

/// \brief Configuration for the iceberg_hive HiveCatalog.
///
/// HMS connection settings (URI, transport, timeouts) plus warehouse / FileIO
/// metadata. Authentication (SASL/Kerberos) and HMS-side locking are
/// introduced in follow-up changes.
class ICEBERG_HIVE_EXPORT HiveCatalogProperties
    : public ConfigBase<HiveCatalogProperties> {
 public:
  template <typename T>
  using Entry = const ConfigBase<HiveCatalogProperties>::Entry<T>;

  /// \brief The URI of the Hive Metastore Thrift endpoint.
  ///
  /// Accepted forms (matching the conventions used by iceberg-java and
  /// iceberg-rust):
  ///   * `thrift://host:port`
  ///   * `host:port`
  ///   * comma-separated list of either form for HA failover
  inline static Entry<std::string> kUri{"uri", ""};

  /// \brief The catalog name reported by `name()`. Defaults to "hive".
  inline static Entry<std::string> kName{"name", "hive"};

  /// \brief The warehouse root path (for example, `s3://bucket/warehouse`
  /// or `hdfs://nn/path`). Used as the default base location for new
  /// tables that do not specify their own location.
  inline static Entry<std::string> kWarehouse{"warehouse", ""};

  /// \brief The FileIO implementation name used to read and write Iceberg
  /// metadata files.
  inline static Entry<std::string> kIOImpl{"io-impl", ""};

  /// \brief Thrift framing for the HMS connection ("buffered" or "framed").
  inline static Entry<std::string> kThriftTransport{"thrift-transport", "buffered"};

  /// \brief HMS connect timeout, in milliseconds. Stored as a string so a
  /// malformed value surfaces as an error from `ConnectTimeoutMs()` rather
  /// than throwing during `Get`.
  inline static Entry<std::string> kConnectTimeoutMs{"connect-timeout-ms", "30000"};

  /// \brief HMS socket / RPC timeout, in milliseconds. Stored as a string for
  /// the same reason as `kConnectTimeoutMs`.
  inline static Entry<std::string> kSocketTimeoutMs{"socket-timeout-ms", "60000"};

  /// \brief When true, wrap the commit path with HMS `lock` / `unlock` for
  /// extra safety on top of the metadata_location CAS. Defaults to false
  /// because CAS already handles single-writer correctness; turn this on
  /// for environments with high write concurrency.
  inline static Entry<bool> kLockEnabled{"hive.lock-enabled", false};

  /// \brief Build a HiveCatalogProperties with defaults applied.
  static HiveCatalogProperties default_properties();

  /// \brief Build a HiveCatalogProperties from a property map.
  static HiveCatalogProperties FromMap(
      std::unordered_map<std::string, std::string> properties);

  /// \brief Resolve `kUri`. Returns an error if the URI is unset or empty.
  Result<std::string_view> Uri() const;

  /// \brief Parse `kThriftTransport` into a HiveThriftTransport. Comparison
  /// is case-insensitive to match the conventions used by other Iceberg
  /// language ports.
  Result<HiveThriftTransport> ThriftTransport() const;

  /// \brief Parse `kConnectTimeoutMs` into a non-negative millisecond value.
  /// Returns InvalidArgument for malformed or negative values.
  Result<int> ConnectTimeoutMs() const;

  /// \brief Parse `kSocketTimeoutMs` into a non-negative millisecond value.
  /// Returns InvalidArgument for malformed or negative values.
  Result<int> SocketTimeoutMs() const;
};

}  // namespace iceberg::hive
