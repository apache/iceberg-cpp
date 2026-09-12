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

#include "iceberg/catalog/hive/hive_utils.h"

#include <algorithm>
#include <array>
#include <format>
#include <string>
#include <string_view>
#include <vector>

#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg::hive {

namespace {

// Keys lifted into dedicated `Database` fields, so they must not be
// duplicated into `parameters`.
constexpr std::array<std::string_view, 4> kDatabaseReservedKeys = {
    kCommentProperty, kLocationProperty, kHmsDbOwnerProperty, kHmsDbOwnerTypeProperty};

// The table equivalent: `location` and `owner` have dedicated `Table`
// fields, and `comment` is not part of the Iceberg-on-HMS contract.
constexpr std::array<std::string_view, 3> kTableReservedKeys = {
    kCommentProperty, kLocationProperty, kOwnerProperty};

// HMS `PrincipalType` names accepted for a database owner.
constexpr std::array<std::string_view, 3> kPrincipalTypes = {"USER", "GROUP", "ROLE"};

bool IsDatabaseReservedKey(std::string_view key) {
  return std::ranges::find(kDatabaseReservedKeys, key) != kDatabaseReservedKeys.end();
}

bool IsTableReservedKey(std::string_view key) {
  return std::ranges::find(kTableReservedKeys, key) != kTableReservedKeys.end();
}

std::string GetOrEmpty(const std::unordered_map<std::string, std::string>& properties,
                       std::string_view key) {
  auto it = properties.find(std::string(key));
  return it == properties.end() ? std::string() : it->second;
}

std::string TrimTrailingSlash(std::string_view path) {
  while (path.size() > 1 && path.back() == '/') {
    path.remove_suffix(1);
  }
  return std::string(path);
}

}  // namespace

Status ValidateNamespace(const Namespace& ns) {
  if (ns.levels.size() != 1) {
    return InvalidArgument(
        "Hive Metastore only supports single-level namespaces; got {} level(s).",
        ns.levels.size());
  }
  if (ns.levels[0].empty()) {
    return InvalidArgument("Hive namespace cannot have an empty name.");
  }
  return {};
}

Status ValidateOwnerSettings(
    const std::unordered_map<std::string, std::string>& properties) {
  auto owner_type = properties.find(std::string(kHmsDbOwnerTypeProperty));
  if (owner_type == properties.end()) {
    return {};
  }
  if (!properties.contains(std::string(kHmsDbOwnerProperty))) {
    return InvalidArgument("Hive namespace property '{}' requires '{}' to also be set.",
                           kHmsDbOwnerTypeProperty, kHmsDbOwnerProperty);
  }
  const bool known = std::ranges::any_of(kPrincipalTypes, [&](std::string_view name) {
    return StringUtils::EqualsIgnoreCase(owner_type->second, name);
  });
  if (!known) {
    return InvalidArgument(
        "Hive namespace property '{}' has value '{}'; expected one of "
        "USER, GROUP or ROLE.",
        kHmsDbOwnerTypeProperty, owner_type->second);
  }
  return {};
}

Result<HiveDatabase> ConvertToHiveDatabase(
    const Namespace& ns, const std::unordered_map<std::string, std::string>& properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(ns));
  ICEBERG_RETURN_UNEXPECTED(ValidateOwnerSettings(properties));

  HiveDatabase database;
  database.name = ns.levels[0];
  database.description = GetOrEmpty(properties, kCommentProperty);
  database.location_uri = GetOrEmpty(properties, kLocationProperty);
  database.owner_name = GetOrEmpty(properties, kHmsDbOwnerProperty);
  database.owner_type = GetOrEmpty(properties, kHmsDbOwnerTypeProperty);

  for (const auto& [key, value] : properties) {
    if (!IsDatabaseReservedKey(key)) {
      database.parameters.emplace(key, value);
    }
  }
  return database;
}

HiveNamespace ConvertFromHiveDatabase(const HiveDatabase& database) {
  HiveNamespace result;
  result.ns.levels = {database.name};
  result.properties = database.parameters;
  if (!database.description.empty()) {
    result.properties[std::string(kCommentProperty)] = database.description;
  }
  if (!database.location_uri.empty()) {
    result.properties[std::string(kLocationProperty)] = database.location_uri;
  }
  if (!database.owner_name.empty()) {
    result.properties[std::string(kHmsDbOwnerProperty)] = database.owner_name;
  }
  if (!database.owner_type.empty()) {
    result.properties[std::string(kHmsDbOwnerTypeProperty)] = database.owner_type;
  }
  return result;
}

Result<HiveTable> ConvertToHiveTable(
    const TableIdentifier& identifier, const std::vector<HiveColumn>& columns,
    std::string_view metadata_location, std::string_view location,
    const std::unordered_map<std::string, std::string>& table_properties) {
  ICEBERG_RETURN_UNEXPECTED(ValidateNamespace(identifier.ns));
  ICEBERG_RETURN_UNEXPECTED(identifier.Validate());

  HiveTable table;
  table.db_name = identifier.ns.levels[0];
  table.table_name = identifier.name;
  table.owner = GetOrEmpty(table_properties, kOwnerProperty);
  table.table_type = "EXTERNAL_TABLE";
  table.location = std::string(location);
  table.columns = columns;
  table.serde = std::string(kLazySimpleSerDe);
  table.input_format = std::string(kFileInputFormat);
  table.output_format = std::string(kFileOutputFormat);

  // Mandatory Iceberg-on-HMS marker parameters.
  table.parameters.emplace(std::string(kMetadataLocationKey), metadata_location);
  table.parameters.emplace(std::string(kTableTypeKey), std::string(kTableTypeIceberg));
  table.parameters.emplace(std::string(kExternalKey), std::string(kExternalTrue));

  // Forward any user-supplied table properties that aren't reserved.
  for (const auto& [key, value] : table_properties) {
    if (!IsTableReservedKey(key) && !table.parameters.contains(key)) {
      table.parameters.emplace(key, value);
    }
  }
  return table;
}

Result<std::string> GetMetadataLocation(
    const std::unordered_map<std::string, std::string>& table_parameters) {
  auto it = table_parameters.find(std::string(kMetadataLocationKey));
  if (it == table_parameters.end() || it->second.empty()) {
    return NotFound("HMS table is missing '{}' parameter; not an Iceberg table.",
                    kMetadataLocationKey);
  }
  return it->second;
}

Status ValidateIcebergTable(
    const TableIdentifier& identifier,
    const std::unordered_map<std::string, std::string>& table_parameters) {
  auto it = table_parameters.find(std::string(kTableTypeKey));
  if (it == table_parameters.end() || it->second.empty()) {
    return NoSuchTable(
        "HMS table {} has no '{}' parameter; refusing to treat it as an Iceberg table.",
        identifier.ToString(), kTableTypeKey);
  }
  if (!StringUtils::EqualsIgnoreCase(it->second, kTableTypeIceberg)) {
    return NoSuchTable("HMS table {} has '{}={}'; expected '{}' (case-insensitive).",
                       identifier.ToString(), kTableTypeKey, it->second,
                       kTableTypeIceberg);
  }
  return {};
}

std::string GetDefaultTableLocation(std::string_view warehouse, const Namespace& ns,
                                    std::string_view table_name) {
  std::string base = TrimTrailingSlash(warehouse);
  std::string ns_part;
  for (std::size_t i = 0; i < ns.levels.size(); ++i) {
    if (i > 0) {
      ns_part += ".";
    }
    ns_part += ns.levels[i];
  }
  return std::format("{}/{}.db/{}", base, ns_part, table_name);
}

}  // namespace iceberg::hive
