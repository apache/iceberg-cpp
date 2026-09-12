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
/// \brief Conversion helpers between Iceberg's catalog model
///        (Namespace, TableIdentifier, Schema) and the Hive Metastore
///        record shape that HmsClient expects on the wire.
///
/// These helpers deliberately produce plain C++ structs rather than
/// Thrift `Database` / `Table` instances so that the public iceberg_hive
/// API stays free of Thrift includes. HmsClient is the only translation
/// unit that converts these structs into Thrift requests.

namespace iceberg::hive {

/// \brief Iceberg table parameter keys recognised by both
///        iceberg-java's HiveCatalog and iceberg-rust's HmsCatalog.
inline constexpr std::string_view kMetadataLocationKey = "metadata_location";
inline constexpr std::string_view kTableTypeKey = "table_type";
inline constexpr std::string_view kTableTypeIceberg = "ICEBERG";
inline constexpr std::string_view kExternalKey = "EXTERNAL";
inline constexpr std::string_view kExternalTrue = "TRUE";

/// \brief Iceberg namespace / table property keys used during conversion.
inline constexpr std::string_view kCommentProperty = "comment";
inline constexpr std::string_view kLocationProperty = "location";

/// \brief HMS table owner property key.
inline constexpr std::string_view kOwnerProperty = "owner";

/// \brief HMS *database* owner property keys.
///
/// Both iceberg-java's `HiveCatalog` and iceberg-rust's `HmsCatalog`
/// namespace these, so a bare `owner` set on a namespace stays an
/// ordinary Hive parameter and only the qualified key reaches the
/// `Database.ownerName` / `Database.ownerType` fields.
inline constexpr std::string_view kHmsDbOwnerProperty = "hive.metastore.database.owner";
inline constexpr std::string_view kHmsDbOwnerTypeProperty =
    "hive.metastore.database.owner-type";

/// \brief Hive storage descriptor values written into table parameters.
///        These match what Spark/Trino expect to find on Iceberg-backed
///        Hive tables, so cross-engine reads keep working.
inline constexpr std::string_view kLazySimpleSerDe =
    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe";
inline constexpr std::string_view kFileInputFormat =
    "org.apache.hadoop.mapred.FileInputFormat";
inline constexpr std::string_view kFileOutputFormat =
    "org.apache.hadoop.mapred.FileOutputFormat";

/// \brief Plain mirror of the Hive Metastore `Database` record fields
///        that iceberg_hive needs to construct or read.
struct ICEBERG_HIVE_EXPORT HiveDatabase {
  std::string name;
  std::string description;
  std::string location_uri;
  std::unordered_map<std::string, std::string> parameters;
  std::string owner_name;
  std::string owner_type;  // "USER", "GROUP" or "ROLE"; empty means HMS-default
};

/// \brief Plain mirror of the Hive Metastore `Table` record fields
///        that iceberg_hive cares about. HmsClient maps it to Thrift.
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

/// \brief Result of converting a `HiveDatabase` back to Iceberg's model.
struct ICEBERG_HIVE_EXPORT HiveNamespace {
  Namespace ns;
  std::unordered_map<std::string, std::string> properties;
};

/// \brief Build the `HiveDatabase` that represents `ns` together with
///        `properties`. Reserved Iceberg property keys (`comment`,
///        `location`, `hive.metastore.database.owner`,
///        `hive.metastore.database.owner-type`) are lifted into dedicated
///        Hive fields; everything else becomes an entry in
///        `parameters`.
///
/// Returns `kInvalidArgument` when the namespace has more than one
/// level (HMS only supports flat namespaces) or when the owner
/// settings are inconsistent (see `ValidateOwnerSettings`).
ICEBERG_HIVE_EXPORT Result<HiveDatabase> ConvertToHiveDatabase(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties);

/// \brief Inverse of `ConvertToHiveDatabase`: split a `HiveDatabase`
///        into the Iceberg namespace plus the property map a caller
///        of `GetNamespaceProperties` would see.
ICEBERG_HIVE_EXPORT HiveNamespace ConvertFromHiveDatabase(const HiveDatabase& database);

/// \brief Assemble the `HiveTable` describing an Iceberg table backed
///        by a metadata file at `metadata_location`. Sets the standard
///        Iceberg table parameters (`table_type=ICEBERG`,
///        `EXTERNAL=TRUE`, `metadata_location`) plus a `LazySimpleSerDe`
///        storage descriptor so Spark / Trino can read the table.
///
/// `location` is the table's filesystem root (typically
/// `<warehouse>/<ns>.db/<table>`); see `GetDefaultTableLocation`.
ICEBERG_HIVE_EXPORT Result<HiveTable> ConvertToHiveTable(
    const TableIdentifier& identifier, const std::vector<HiveColumn>& columns,
    std::string_view metadata_location, std::string_view location,
    const std::unordered_map<std::string, std::string>& table_properties);

/// \brief Extract the `metadata_location` parameter from a HMS Table
///        parameter map. Returns `kNotFound` if absent (the table is
///        not an Iceberg table).
ICEBERG_HIVE_EXPORT Result<std::string> GetMetadataLocation(
    const std::unordered_map<std::string, std::string>& table_parameters);

/// \brief Validate that a HMS Table parameter map represents an Iceberg
///        table by checking `table_type == "ICEBERG"` (case-insensitive).
///        Returns `kNoSuchTable` when the parameter is absent or holds
///        any other value, matching the guard Java's
///        `BaseMetastoreTableOperations.refresh()` applies before reading
///        `metadata_location`.
ICEBERG_HIVE_EXPORT Status ValidateIcebergTable(
    const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& table_parameters);

/// \brief Compute a default table location under `warehouse`. The
///        layout matches what iceberg-java and iceberg-rust pick:
///        `<warehouse>/<ns_with_dots>.db/<table_name>`.
ICEBERG_HIVE_EXPORT std::string GetDefaultTableLocation(std::string_view warehouse,
                                                        const Namespace& ns,
                                                        std::string_view table_name);

/// \brief Enforce that `ns` is a single-level namespace (HMS only
///        supports flat databases).
ICEBERG_HIVE_EXPORT Status ValidateNamespace(const Namespace& ns);

/// \brief Enforce the owner invariants shared with iceberg-java and
///        iceberg-rust: `hive.metastore.database.owner-type` requires
///        `hive.metastore.database.owner`, and its value must be one of
///        HMS's `PrincipalType` names -- USER, GROUP or ROLE, compared
///        case-insensitively.
ICEBERG_HIVE_EXPORT Status
ValidateOwnerSettings(const std::unordered_map<std::string, std::string>& properties);

}  // namespace iceberg::hive
