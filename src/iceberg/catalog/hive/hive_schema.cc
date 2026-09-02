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

#include "iceberg/catalog/hive/hive_schema.h"

#include <format>
#include <string>
#include <vector>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {

// Hive doesn't have a precise counterpart to every Iceberg primitive
// type; the closest equivalent is chosen for each case below. Mapping
// matches iceberg-rust's `HiveSchemaBuilder::primitive`.
Result<std::string> PrimitiveToHive(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kBoolean:
      return std::string("boolean");
    case TypeId::kInt:
      return std::string("int");
    case TypeId::kLong:
      return std::string("bigint");
    case TypeId::kFloat:
      return std::string("float");
    case TypeId::kDouble:
      return std::string("double");
    case TypeId::kDate:
      return std::string("date");
    case TypeId::kTimestamp:
      return std::string("timestamp");
    case TypeId::kTimestampTz:
      // Hive's TIMESTAMPLOCALTZ, the spelling iceberg-java emits on
      // Hive 3+. Engines reading through HMS must see the tz-adjusted
      // type, otherwise they read an Iceberg timestamptz column as a
      // naive timestamp. See the header for why the Hive 2 fallback is
      // not reproduced.
      return std::string("timestamp with local time zone");
    case TypeId::kTime:
    case TypeId::kString:
    case TypeId::kUuid:
      return std::string("string");
    case TypeId::kBinary:
    case TypeId::kFixed:
      return std::string("binary");
    case TypeId::kDecimal: {
      const auto& decimal = internal::checked_cast<const DecimalType&>(type);
      return std::format("decimal({},{})", decimal.precision(), decimal.scale());
    }
    default:
      return NotImplemented("Unhandled primitive Iceberg type id: {}",
                            static_cast<int>(type.type_id()));
  }
}

Result<std::string> StructToHive(const StructType& type) {
  std::string out = "struct<";
  bool first = true;
  for (const auto& field : type.fields()) {
    if (!first) {
      out += ",";
    }
    ICEBERG_ASSIGN_OR_RAISE(auto field_type, TypeToHiveString(*field.type()));
    out += std::string(field.name());
    out += ":";
    out += field_type;
    first = false;
  }
  out += ">";
  return out;
}

Result<std::string> ListToHive(const ListType& type) {
  ICEBERG_ASSIGN_OR_RAISE(auto inner, TypeToHiveString(*type.element().type()));
  return std::format("array<{}>", inner);
}

Result<std::string> MapToHive(const MapType& type) {
  ICEBERG_ASSIGN_OR_RAISE(auto key_type, TypeToHiveString(*type.key().type()));
  ICEBERG_ASSIGN_OR_RAISE(auto value_type, TypeToHiveString(*type.value().type()));
  return std::format("map<{},{}>", key_type, value_type);
}

}  // namespace

Result<std::string> TypeToHiveString(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kStruct:
      return StructToHive(internal::checked_cast<const StructType&>(type));
    case TypeId::kList:
      return ListToHive(internal::checked_cast<const ListType&>(type));
    case TypeId::kMap:
      return MapToHive(internal::checked_cast<const MapType&>(type));
    default:
      return PrimitiveToHive(type);
  }
}

Result<std::vector<HiveColumn>> SchemaToHiveColumns(const Schema& schema) {
  std::vector<HiveColumn> columns;
  columns.reserve(schema.fields().size());
  for (const auto& field : schema.fields()) {
    ICEBERG_ASSIGN_OR_RAISE(auto type_string, TypeToHiveString(*field.type()));
    columns.push_back(HiveColumn{.name = std::string(field.name()),
                                 .type_string = std::move(type_string),
                                 .comment = std::string(field.doc())});
  }
  return columns;
}

}  // namespace iceberg::hive
