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
#include <vector>

#include "iceberg/catalog/hive/hive_schema.h"
#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"

/// \file iceberg/catalog/hive/hive_utils.h
/// \brief Convert Iceberg catalog objects to HMS records.

namespace iceberg::hive {

/// \brief Iceberg-on-HMS parameter keys.
inline constexpr std::string_view kMetadataLocationKey = "metadata_location";
inline constexpr std::string_view kTableTypeKey = "table_type";
inline constexpr std::string_view kTableTypeIceberg = "ICEBERG";
inline constexpr std::string_view kExternalKey = "EXTERNAL";
inline constexpr std::string_view kExternalTrue = "TRUE";

/// \brief Property keys lifted into HMS fields.
inline constexpr std::string_view kCommentProperty = "comment";
inline constexpr std::string_view kLocationProperty = "location";

/// \brief HMS table owner key.
inline constexpr std::string_view kHmsTableOwnerProperty = "hive.metastore.table.owner";

/// \brief HMS database owner keys.
inline constexpr std::string_view kHmsDbOwnerProperty = "hive.metastore.database.owner";
inline constexpr std::string_view kHmsDbOwnerTypeProperty =
    "hive.metastore.database.owner-type";

/// \brief Fallback storage descriptor classes.
inline constexpr std::string_view kLazySimpleSerDe =
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
inline constexpr std::string_view kFileInputFormat =
    "org.apache.hadoop.mapred.FileInputFormat";
inline constexpr std::string_view kFileOutputFormat =
    "org.apache.hadoop.mapred.FileOutputFormat";

/// \brief Subset of HMS Database used by HiveCatalog.
struct ICEBERG_HIVE_EXPORT HiveDatabase {
  std::string name;
  std::string description;
  std::string location_uri;
  std::unordered_map<std::string, std::string> parameters;
  std::string owner_name;
  std::string owner_type;  // "USER", "GROUP" or "ROLE"; empty means HMS-default
};

/// \brief Subset of HMS Table used by HiveCatalog.
struct ICEBERG_HIVE_EXPORT HiveTable {
  std::string db_name;
  std::string table_name;
  std::string owner;
  std::string table_type;  // "EXTERNAL_TABLE" for Iceberg tables
  std::string location;
  std::vector<HiveColumn> columns;
  std::unordered_map<std::string, std::string> parameters;
  std::string serde;
  std::string input_format;
  std::string output_format;
};

/// \brief Namespace and properties read from HMS.
struct ICEBERG_HIVE_EXPORT HiveNamespace {
  Namespace ns;
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Lift reserved properties into HMS fields. Caller resolves defaults.
ICEBERG_HIVE_EXPORT Result<HiveDatabase> ConvertToHiveDatabase(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties);

/// \brief Extract namespace properties from an HMS database.
ICEBERG_HIVE_EXPORT HiveNamespace ConvertFromHiveDatabase(const HiveDatabase& database);

/// \brief Build an external Iceberg table with a LazySimpleSerDe descriptor.
/// Translate gc.enabled to external.table.purge. Catalog markers override user
/// properties. metadata_location must be non-empty; caller resolves the owner.
ICEBERG_HIVE_EXPORT Result<HiveTable> ConvertToHiveTable(
    const TableIdentifier& identifier, const std::vector<HiveColumn>& columns,
    std::string_view metadata_location, std::string_view location,
    const std::unordered_map<std::string, std::string>& table_properties);

/// \brief Return metadata_location, or kNotFound if absent or empty.
ICEBERG_HIVE_EXPORT Result<std::string> GetMetadataLocation(
    const std::unordered_map<std::string, std::string>& table_parameters);

/// \brief Require table_type=ICEBERG (case-insensitive).
ICEBERG_HIVE_EXPORT Status ValidateIcebergTable(
    const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& table_parameters);

/// \brief Return <warehouse>/<namespace>.db/<table_name>.
ICEBERG_HIVE_EXPORT std::string GetDefaultTableLocation(std::string_view warehouse,
                                                        const Namespace& ns,
                                                        std::string_view table_name);

/// \brief Require a single-level namespace.
ICEBERG_HIVE_EXPORT Status ValidateNamespace(const Namespace& ns);

/// \brief Require owner when owner-type is set.
/// owner-type must be USER, GROUP or ROLE.
ICEBERG_HIVE_EXPORT Status
ValidateOwnerSettings(const std::unordered_map<std::string, std::string>& properties);

}  // namespace iceberg::hive
