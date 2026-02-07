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

#include <algorithm>
#include <cstdint>
#include <format>
#include <regex>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "iceberg/constants.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/name_mapping.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter.h"  // IWYU pragma: keep
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {

// Transform constants
constexpr std::string_view kTransform = "transform";
constexpr std::string_view kSourceId = "source-id";
constexpr std::string_view kDirection = "direction";
constexpr std::string_view kNullOrder = "null-order";

// Sort order constants
constexpr std::string_view kOrderId = "order-id";
constexpr std::string_view kFields = "fields";

// Schema constants
constexpr std::string_view kSchemaId = "schema-id";
constexpr std::string_view kIdentifierFieldIds = "identifier-field-ids";

// Type constants
constexpr std::string_view kType = "type";
constexpr std::string_view kStruct = "struct";
constexpr std::string_view kList = "list";
constexpr std::string_view kMap = "map";
constexpr std::string_view kElement = "element";
constexpr std::string_view kKey = "key";
constexpr std::string_view kValue = "value";
constexpr std::string_view kDoc = "doc";
constexpr std::string_view kName = "name";
constexpr std::string_view kNamespace = "namespace";
constexpr std::string_view kNames = "names";
constexpr std::string_view kId = "id";
constexpr std::string_view kInitialDefault = "initial-default";
constexpr std::string_view kWriteDefault = "write-default";
constexpr std::string_view kFieldId = "field-id";
constexpr std::string_view kElementId = "element-id";
constexpr std::string_view kKeyId = "key-id";
constexpr std::string_view kValueId = "value-id";
constexpr std::string_view kRequired = "required";
constexpr std::string_view kElementRequired = "element-required";
constexpr std::string_view kValueRequired = "value-required";

// Snapshot constants
constexpr std::string_view kSpecId = "spec-id";
constexpr std::string_view kSnapshotId = "snapshot-id";
constexpr std::string_view kParentSnapshotId = "parent-snapshot-id";
constexpr std::string_view kSequenceNumber = "sequence-number";
constexpr std::string_view kTimestampMs = "timestamp-ms";
constexpr std::string_view kManifestList = "manifest-list";
constexpr std::string_view kSummary = "summary";
constexpr std::string_view kMinSnapshotsToKeep = "min-snapshots-to-keep";
constexpr std::string_view kMaxSnapshotAgeMs = "max-snapshot-age-ms";
constexpr std::string_view kMaxRefAgeMs = "max-ref-age-ms";

const std::unordered_set<std::string_view> kValidSnapshotSummaryFields = {
    SnapshotSummaryFields::kOperation,
    SnapshotSummaryFields::kAddedDataFiles,
    SnapshotSummaryFields::kDeletedDataFiles,
    SnapshotSummaryFields::kTotalDataFiles,
    SnapshotSummaryFields::kAddedDeleteFiles,
    SnapshotSummaryFields::kAddedEqDeleteFiles,
    SnapshotSummaryFields::kRemovedEqDeleteFiles,
    SnapshotSummaryFields::kAddedPosDeleteFiles,
    SnapshotSummaryFields::kRemovedPosDeleteFiles,
    SnapshotSummaryFields::kAddedDVs,
    SnapshotSummaryFields::kRemovedDVs,
    SnapshotSummaryFields::kRemovedDeleteFiles,
    SnapshotSummaryFields::kTotalDeleteFiles,
    SnapshotSummaryFields::kAddedRecords,
    SnapshotSummaryFields::kDeletedRecords,
    SnapshotSummaryFields::kTotalRecords,
    SnapshotSummaryFields::kAddedFileSize,
    SnapshotSummaryFields::kRemovedFileSize,
    SnapshotSummaryFields::kTotalFileSize,
    SnapshotSummaryFields::kAddedPosDeletes,
    SnapshotSummaryFields::kRemovedPosDeletes,
    SnapshotSummaryFields::kTotalPosDeletes,
    SnapshotSummaryFields::kAddedEqDeletes,
    SnapshotSummaryFields::kRemovedEqDeletes,
    SnapshotSummaryFields::kTotalEqDeletes,
    SnapshotSummaryFields::kDeletedDuplicatedFiles,
    SnapshotSummaryFields::kChangedPartitionCountProp,
    SnapshotSummaryFields::kWAPId,
    SnapshotSummaryFields::kPublishedWAPId,
    SnapshotSummaryFields::kSourceSnapshotId,
    SnapshotSummaryFields::kEngineName,
    SnapshotSummaryFields::kEngineVersion};

const std::unordered_set<std::string_view> kValidDataOperation = {
    DataOperation::kAppend, DataOperation::kReplace, DataOperation::kOverwrite,
    DataOperation::kDelete};

// TableMetadata constants
constexpr std::string_view kFormatVersion = "format-version";
constexpr std::string_view kTableUuid = "table-uuid";
constexpr std::string_view kLocation = "location";
constexpr std::string_view kLastSequenceNumber = "last-sequence-number";
constexpr std::string_view kLastUpdatedMs = "last-updated-ms";
constexpr std::string_view kLastColumnId = "last-column-id";
constexpr std::string_view kSchema = "schema";
constexpr std::string_view kSchemas = "schemas";
constexpr std::string_view kCurrentSchemaId = "current-schema-id";
constexpr std::string_view kPartitionSpec = "partition-spec";
constexpr std::string_view kPartitionSpecs = "partition-specs";
constexpr std::string_view kDefaultSpecId = "default-spec-id";
constexpr std::string_view kLastPartitionId = "last-partition-id";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kCurrentSnapshotId = "current-snapshot-id";
constexpr std::string_view kSnapshots = "snapshots";
constexpr std::string_view kSnapshotLog = "snapshot-log";
constexpr std::string_view kMetadataLog = "metadata-log";
constexpr std::string_view kSortOrders = "sort-orders";
constexpr std::string_view kDefaultSortOrderId = "default-sort-order-id";
constexpr std::string_view kRefs = "refs";
constexpr std::string_view kStatistics = "statistics";
constexpr std::string_view kPartitionStatistics = "partition-statistics";
constexpr std::string_view kNextRowId = "next-row-id";
constexpr std::string_view kMetadataFile = "metadata-file";
constexpr std::string_view kStatisticsPath = "statistics-path";
constexpr std::string_view kFileSizeInBytes = "file-size-in-bytes";
constexpr std::string_view kFileFooterSizeInBytes = "file-footer-size-in-bytes";
constexpr std::string_view kBlobMetadata = "blob-metadata";

// TableUpdate action constants
constexpr std::string_view kAction = "action";
constexpr std::string_view kActionAssignUUID = "assign-uuid";
constexpr std::string_view kActionUpgradeFormatVersion = "upgrade-format-version";
constexpr std::string_view kActionAddSchema = "add-schema";
constexpr std::string_view kActionSetCurrentSchema = "set-current-schema";
constexpr std::string_view kActionAddPartitionSpec = "add-spec";
constexpr std::string_view kActionSetDefaultPartitionSpec = "set-default-spec";
constexpr std::string_view kActionRemovePartitionSpecs = "remove-partition-specs";
constexpr std::string_view kActionRemoveSchemas = "remove-schemas";
constexpr std::string_view kActionAddSortOrder = "add-sort-order";
constexpr std::string_view kActionSetDefaultSortOrder = "set-default-sort-order";
constexpr std::string_view kActionAddSnapshot = "add-snapshot";
constexpr std::string_view kActionRemoveSnapshots = "remove-snapshots";
constexpr std::string_view kActionRemoveSnapshotRef = "remove-snapshot-ref";
constexpr std::string_view kActionSetSnapshotRef = "set-snapshot-ref";
constexpr std::string_view kActionSetProperties = "set-properties";
constexpr std::string_view kActionRemoveProperties = "remove-properties";
constexpr std::string_view kActionSetLocation = "set-location";
constexpr std::string_view kActionSetStatistics = "set-statistics";
constexpr std::string_view kActionRemoveStatistics = "remove-statistics";
constexpr std::string_view kActionSetPartitionStatistics = "set-partition-statistics";
constexpr std::string_view kActionRemovePartitionStatistics =
    "remove-partition-statistics";

// TableUpdate field constants
constexpr std::string_view kUUID = "uuid";
constexpr std::string_view kSpec = "spec";
constexpr std::string_view kSpecIds = "spec-ids";
constexpr std::string_view kSchemaIds = "schema-ids";
constexpr std::string_view kSortOrder = "sort-order";
constexpr std::string_view kSortOrderId = "sort-order-id";
constexpr std::string_view kSnapshot = "snapshot";
constexpr std::string_view kSnapshotIds = "snapshot-ids";
constexpr std::string_view kRefName = "ref-name";
constexpr std::string_view kUpdates = "updates";
constexpr std::string_view kRemovals = "removals";

// TableRequirement type constants
constexpr std::string_view kRequirementAssertDoesNotExist = "assert-create";
constexpr std::string_view kRequirementAssertUUID = "assert-table-uuid";
constexpr std::string_view kRequirementAssertRefSnapshotID = "assert-ref-snapshot-id";
constexpr std::string_view kRequirementAssertLastAssignedFieldId =
    "assert-last-assigned-field-id";
constexpr std::string_view kRequirementAssertCurrentSchemaID = "assert-current-schema-id";
constexpr std::string_view kRequirementAssertLastAssignedPartitionId =
    "assert-last-assigned-partition-id";
constexpr std::string_view kRequirementAssertDefaultSpecID = "assert-default-spec-id";
constexpr std::string_view kRequirementAssertDefaultSortOrderID =
    "assert-default-sort-order-id";
constexpr std::string_view kLastAssignedFieldId = "last-assigned-field-id";
constexpr std::string_view kLastAssignedPartitionId = "last-assigned-partition-id";

}  // namespace

nlohmann::json ToJson(const SortField& sort_field) {
  nlohmann::json json;
  json[kTransform] = std::format("{}", *sort_field.transform());
  json[kSourceId] = sort_field.source_id();
  json[kDirection] = std::format("{}", sort_field.direction());
  json[kNullOrder] = std::format("{}", sort_field.null_order());
  return json;
}

nlohmann::json ToJson(const SortOrder& sort_order) {
  nlohmann::json json;
  json[kOrderId] = sort_order.order_id();

  nlohmann::json fields_json = nlohmann::json::array();
  for (const auto& field : sort_order.fields()) {
    fields_json.push_back(ToJson(field));
  }
  json[kFields] = fields_json;
  return json;
}

Result<std::unique_ptr<SortField>> SortFieldFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto source_id, GetJsonValue<int32_t>(json, kSourceId));
  ICEBERG_ASSIGN_OR_RAISE(
      auto transform,
      GetJsonValue<std::string>(json, kTransform).and_then(TransformFromString));
  ICEBERG_ASSIGN_OR_RAISE(
      auto direction,
      GetJsonValue<std::string>(json, kDirection).and_then(SortDirectionFromString));
  ICEBERG_ASSIGN_OR_RAISE(
      auto null_order,
      GetJsonValue<std::string>(json, kNullOrder).and_then(NullOrderFromString));
  return std::make_unique<SortField>(source_id, std::move(transform), direction,
                                     null_order);
}

Result<std::unique_ptr<SortOrder>> SortOrderFromJson(
    const nlohmann::json& json, const std::shared_ptr<Schema>& current_schema) {
  ICEBERG_ASSIGN_OR_RAISE(auto order_id, GetJsonValue<int32_t>(json, kOrderId));
  ICEBERG_ASSIGN_OR_RAISE(auto fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<SortField> sort_fields;
  for (const auto& field_json : fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_field, SortFieldFromJson(field_json));
    sort_fields.push_back(std::move(*sort_field));
  }
  return SortOrder::Make(*current_schema, order_id, std::move(sort_fields));
}

Result<std::unique_ptr<SortOrder>> SortOrderFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto order_id, GetJsonValue<int32_t>(json, kOrderId));
  ICEBERG_ASSIGN_OR_RAISE(auto fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<SortField> sort_fields;
  for (const auto& field_json : fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_field, SortFieldFromJson(field_json));
    sort_fields.push_back(std::move(*sort_field));
  }
  return SortOrder::Make(order_id, std::move(sort_fields));
}

nlohmann::json ToJson(const SchemaField& field) {
  nlohmann::json json;
  json[kId] = field.field_id();
  json[kName] = field.name();
  json[kRequired] = !field.optional();
  json[kType] = ToJson(*field.type());
  if (!field.doc().empty()) {
    json[kDoc] = field.doc();
  }
  return json;
}

nlohmann::json ToJson(const Type& type) {
  switch (type.type_id()) {
    case TypeId::kStruct: {
      const auto& struct_type = internal::checked_cast<const StructType&>(type);
      nlohmann::json json;
      json[kType] = kStruct;
      nlohmann::json fields_json = nlohmann::json::array();
      for (const auto& field : struct_type.fields()) {
        fields_json.push_back(ToJson(field));
        // TODO(gangwu): add default values
      }
      json[kFields] = fields_json;
      return json;
    }
    case TypeId::kList: {
      const auto& list_type = internal::checked_cast<const ListType&>(type);
      nlohmann::json json;
      json[kType] = kList;

      const auto& element_field = list_type.fields().front();
      json[kElementId] = element_field.field_id();
      json[kElementRequired] = !element_field.optional();
      json[kElement] = ToJson(*element_field.type());
      return json;
    }
    case TypeId::kMap: {
      const auto& map_type = internal::checked_cast<const MapType&>(type);
      nlohmann::json json;
      json[std::string(kType)] = kMap;

      const auto& key_field = map_type.key();
      json[kKeyId] = key_field.field_id();
      json[kKey] = ToJson(*key_field.type());

      const auto& value_field = map_type.value();
      json[kValueId] = value_field.field_id();
      json[kValueRequired] = !value_field.optional();
      json[kValue] = ToJson(*value_field.type());
      return json;
    }
    case TypeId::kBoolean:
      return "boolean";
    case TypeId::kInt:
      return "int";
    case TypeId::kLong:
      return "long";
    case TypeId::kFloat:
      return "float";
    case TypeId::kDouble:
      return "double";
    case TypeId::kDecimal: {
      const auto& decimal_type = internal::checked_cast<const DecimalType&>(type);
      return std::format("decimal({},{})", decimal_type.precision(),
                         decimal_type.scale());
    }
    case TypeId::kDate:
      return "date";
    case TypeId::kTime:
      return "time";
    case TypeId::kTimestamp:
      return "timestamp";
    case TypeId::kTimestampTz:
      return "timestamptz";
    case TypeId::kString:
      return "string";
    case TypeId::kBinary:
      return "binary";
    case TypeId::kFixed: {
      const auto& fixed_type = internal::checked_cast<const FixedType&>(type);
      return std::format("fixed[{}]", fixed_type.length());
    }
    case TypeId::kUuid:
      return "uuid";
  }
  std::unreachable();
}

nlohmann::json ToJson(const Schema& schema) {
  nlohmann::json json = ToJson(internal::checked_cast<const Type&>(schema));
  json[kSchemaId] = schema.schema_id();
  if (!schema.IdentifierFieldIds().empty()) {
    json[kIdentifierFieldIds] = schema.IdentifierFieldIds();
  }
  return json;
}

Result<std::string> ToJsonString(const Schema& schema) {
  return ToJsonString(ToJson(schema));
}

nlohmann::json ToJson(const SnapshotRef& ref) {
  nlohmann::json json;
  json[kSnapshotId] = ref.snapshot_id;
  json[kType] = std::format("{}", ref.type());
  if (ref.type() == SnapshotRefType::kBranch) {
    const auto& branch = std::get<SnapshotRef::Branch>(ref.retention);
    SetOptionalField(json, kMinSnapshotsToKeep, branch.min_snapshots_to_keep);
    SetOptionalField(json, kMaxSnapshotAgeMs, branch.max_snapshot_age_ms);
    SetOptionalField(json, kMaxRefAgeMs, branch.max_ref_age_ms);
  } else if (ref.type() == SnapshotRefType::kTag) {
    const auto& tag = std::get<SnapshotRef::Tag>(ref.retention);
    SetOptionalField(json, kMaxRefAgeMs, tag.max_ref_age_ms);
  }
  return json;
}

nlohmann::json ToJson(const Snapshot& snapshot) {
  nlohmann::json json;
  json[kSnapshotId] = snapshot.snapshot_id;
  SetOptionalField(json, kParentSnapshotId, snapshot.parent_snapshot_id);
  if (snapshot.sequence_number > TableMetadata::kInitialSequenceNumber) {
    json[kSequenceNumber] = snapshot.sequence_number;
  }
  json[kTimestampMs] = UnixMsFromTimePointMs(snapshot.timestamp_ms);
  json[kManifestList] = snapshot.manifest_list;
  // If there is an operation, write the summary map
  if (snapshot.Operation().has_value()) {
    json[kSummary] = snapshot.summary;
  }
  SetOptionalField(json, kSchemaId, snapshot.schema_id);
  return json;
}

namespace {

Result<std::unique_ptr<Type>> StructTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto json_fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<SchemaField> fields;
  for (const auto& field_json : json_fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, FieldFromJson(field_json));
    fields.emplace_back(std::move(*field));
  }

  return std::make_unique<StructType>(std::move(fields));
}

Result<std::unique_ptr<Type>> ListTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto element_type, TypeFromJson(json[kElement]));
  ICEBERG_ASSIGN_OR_RAISE(auto element_id, GetJsonValue<int32_t>(json, kElementId));
  ICEBERG_ASSIGN_OR_RAISE(auto element_required,
                          GetJsonValue<bool>(json, kElementRequired));

  return std::make_unique<ListType>(
      SchemaField(element_id, std::string(ListType::kElementName),
                  std::move(element_type), !element_required));
}

Result<std::unique_ptr<Type>> MapTypeFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto key_type, GetJsonValue<nlohmann::json>(json, kKey).and_then(TypeFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      auto value_type, GetJsonValue<nlohmann::json>(json, kValue).and_then(TypeFromJson));

  ICEBERG_ASSIGN_OR_RAISE(auto key_id, GetJsonValue<int32_t>(json, kKeyId));
  ICEBERG_ASSIGN_OR_RAISE(auto value_id, GetJsonValue<int32_t>(json, kValueId));
  ICEBERG_ASSIGN_OR_RAISE(auto value_required, GetJsonValue<bool>(json, kValueRequired));

  SchemaField key_field(key_id, std::string(MapType::kKeyName), std::move(key_type),
                        /*optional=*/false);
  SchemaField value_field(value_id, std::string(MapType::kValueName),
                          std::move(value_type), !value_required);
  return std::make_unique<MapType>(std::move(key_field), std::move(value_field));
}

}  // namespace

Result<std::unique_ptr<Type>> TypeFromJson(const nlohmann::json& json) {
  if (json.is_string()) {
    std::string type_str = json.get<std::string>();
    if (type_str == "boolean") {
      return std::make_unique<BooleanType>();
    } else if (type_str == "int") {
      return std::make_unique<IntType>();
    } else if (type_str == "long") {
      return std::make_unique<LongType>();
    } else if (type_str == "float") {
      return std::make_unique<FloatType>();
    } else if (type_str == "double") {
      return std::make_unique<DoubleType>();
    } else if (type_str == "date") {
      return std::make_unique<DateType>();
    } else if (type_str == "time") {
      return std::make_unique<TimeType>();
    } else if (type_str == "timestamp") {
      return std::make_unique<TimestampType>();
    } else if (type_str == "timestamptz") {
      return std::make_unique<TimestampTzType>();
    } else if (type_str == "string") {
      return std::make_unique<StringType>();
    } else if (type_str == "binary") {
      return std::make_unique<BinaryType>();
    } else if (type_str == "uuid") {
      return std::make_unique<UuidType>();
    } else if (type_str.starts_with("fixed")) {
      std::regex fixed_regex(R"(fixed\[\s*(\d+)\s*\])");
      std::smatch match;
      if (std::regex_match(type_str, match, fixed_regex)) {
        ICEBERG_ASSIGN_OR_RAISE(auto length,
                                StringUtils::ParseNumber<int32_t>(match[1].str()));
        return std::make_unique<FixedType>(length);
      }
      return JsonParseError("Invalid fixed type: {}", type_str);
    } else if (type_str.starts_with("decimal")) {
      std::regex decimal_regex(R"(decimal\(\s*(\d+)\s*,\s*(\d+)\s*\))");
      std::smatch match;
      if (std::regex_match(type_str, match, decimal_regex)) {
        ICEBERG_ASSIGN_OR_RAISE(auto precision,
                                StringUtils::ParseNumber<int32_t>(match[1].str()));
        ICEBERG_ASSIGN_OR_RAISE(auto scale,
                                StringUtils::ParseNumber<int32_t>(match[2].str()));
        return std::make_unique<DecimalType>(precision, scale);
      }
      return JsonParseError("Invalid decimal type: {}", type_str);
    } else {
      return JsonParseError("Unknown primitive type: {}", type_str);
    }
  }

  // For complex types like struct, list, and map
  ICEBERG_ASSIGN_OR_RAISE(auto type_str, GetJsonValue<std::string>(json, kType));
  if (type_str == kStruct) {
    return StructTypeFromJson(json);
  } else if (type_str == kList) {
    return ListTypeFromJson(json);
  } else if (type_str == kMap) {
    return MapTypeFromJson(json);
  } else {
    return JsonParseError("Unknown complex type: {}", type_str);
  }
}

Result<std::unique_ptr<SchemaField>> FieldFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto type, GetJsonValue<nlohmann::json>(json, kType).and_then(TypeFromJson));
  ICEBERG_ASSIGN_OR_RAISE(auto field_id, GetJsonValue<int32_t>(json, kId));
  ICEBERG_ASSIGN_OR_RAISE(auto name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(auto required, GetJsonValue<bool>(json, kRequired));
  ICEBERG_ASSIGN_OR_RAISE(auto doc, GetJsonValueOrDefault<std::string>(json, kDoc));

  return std::make_unique<SchemaField>(field_id, std::move(name), std::move(type),
                                       !required, doc);
}

Result<std::unique_ptr<Schema>> SchemaFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema_id_opt,
                          GetJsonValueOptional<int32_t>(json, kSchemaId));
  ICEBERG_ASSIGN_OR_RAISE(auto type, TypeFromJson(json));

  if (type->type_id() != TypeId::kStruct) [[unlikely]] {
    return JsonParseError("Schema must be a struct type, but got {}", SafeDumpJson(json));
  }
  auto& struct_type = internal::checked_cast<StructType&>(*type);

  std::vector<SchemaField> fields;
  fields.reserve(struct_type.fields().size());
  for (auto& field : struct_type.fields()) {
    fields.emplace_back(std::move(field));
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto identifier_field_ids,
      GetJsonValueOrDefault<std::vector<int32_t>>(json, kIdentifierFieldIds));

  return Schema::Make(std::move(fields), schema_id_opt.value_or(Schema::kInitialSchemaId),
                      std::move(identifier_field_ids));
}

nlohmann::json ToJson(const PartitionField& partition_field) {
  nlohmann::json json;
  json[kSourceId] = partition_field.source_id();
  json[kFieldId] = partition_field.field_id();
  json[kTransform] = std::format("{}", *partition_field.transform());
  json[kName] = partition_field.name();
  return json;
}

nlohmann::json ToJson(const PartitionSpec& partition_spec) {
  nlohmann::json json;
  json[kSpecId] = partition_spec.spec_id();

  nlohmann::json fields_json = nlohmann::json::array();
  for (const auto& field : partition_spec.fields()) {
    fields_json.push_back(ToJson(field));
  }
  json[kFields] = fields_json;
  return json;
}

Result<std::string> ToJsonString(const PartitionSpec& partition_spec) {
  return ToJsonString(ToJson(partition_spec));
}

Result<std::unique_ptr<PartitionField>> PartitionFieldFromJson(
    const nlohmann::json& json, bool allow_field_id_missing) {
  ICEBERG_ASSIGN_OR_RAISE(auto source_id, GetJsonValue<int32_t>(json, kSourceId));
  int32_t field_id;
  if (allow_field_id_missing) {
    // Partition field id in v1 is not tracked, so we use -1 to indicate that.
    ICEBERG_ASSIGN_OR_RAISE(field_id, GetJsonValueOrDefault<int32_t>(
                                          json, kFieldId, SchemaField::kInvalidFieldId));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(field_id, GetJsonValue<int32_t>(json, kFieldId));
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto transform,
      GetJsonValue<std::string>(json, kTransform).and_then(TransformFromString));
  ICEBERG_ASSIGN_OR_RAISE(auto name, GetJsonValue<std::string>(json, kName));
  return std::make_unique<PartitionField>(source_id, field_id, name,
                                          std::move(transform));
}

Result<std::unique_ptr<PartitionSpec>> PartitionSpecFromJson(
    const std::shared_ptr<Schema>& schema, const nlohmann::json& json,
    int32_t default_spec_id) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kSpecId));
  ICEBERG_ASSIGN_OR_RAISE(auto fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<PartitionField> partition_fields;
  for (const auto& field_json : fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_field, PartitionFieldFromJson(field_json));
    partition_fields.push_back(std::move(*partition_field));
  }

  std::unique_ptr<PartitionSpec> spec;
  if (default_spec_id == spec_id) {
    ICEBERG_ASSIGN_OR_RAISE(
        spec, PartitionSpec::Make(*schema, spec_id, std::move(partition_fields),
                                  /*allow_missing_fields=*/false));
  } else {
    ICEBERG_ASSIGN_OR_RAISE(
        spec, PartitionSpec::Make(*schema, spec_id, std::move(partition_fields),
                                  /*allow_missing_fields=*/true));
  }
  return spec;
}

Result<std::unique_ptr<PartitionSpec>> PartitionSpecFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kSpecId));
  ICEBERG_ASSIGN_OR_RAISE(auto fields, GetJsonValue<nlohmann::json>(json, kFields));

  std::vector<PartitionField> partition_fields;
  for (const auto& field_json : fields) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_field, PartitionFieldFromJson(field_json));
    partition_fields.push_back(std::move(*partition_field));
  }

  return PartitionSpec::Make(spec_id, std::move(partition_fields));
}

Result<std::unique_ptr<SnapshotRef>> SnapshotRefFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(
      auto type,
      GetJsonValue<std::string>(json, kType).and_then(SnapshotRefTypeFromString));
  if (type == SnapshotRefType::kBranch) {
    ICEBERG_ASSIGN_OR_RAISE(auto min_snapshots_to_keep,
                            GetJsonValueOptional<int32_t>(json, kMinSnapshotsToKeep));
    ICEBERG_ASSIGN_OR_RAISE(auto max_snapshot_age_ms,
                            GetJsonValueOptional<int64_t>(json, kMaxSnapshotAgeMs));
    ICEBERG_ASSIGN_OR_RAISE(auto max_ref_age_ms,
                            GetJsonValueOptional<int64_t>(json, kMaxRefAgeMs));

    return std::make_unique<SnapshotRef>(
        snapshot_id, SnapshotRef::Branch{.min_snapshots_to_keep = min_snapshots_to_keep,
                                         .max_snapshot_age_ms = max_snapshot_age_ms,
                                         .max_ref_age_ms = max_ref_age_ms});
  } else {
    ICEBERG_ASSIGN_OR_RAISE(auto max_ref_age_ms,
                            GetJsonValueOptional<int64_t>(json, kMaxRefAgeMs));

    return std::make_unique<SnapshotRef>(
        snapshot_id, SnapshotRef::Tag{.max_ref_age_ms = max_ref_age_ms});
  }
}

Result<std::unique_ptr<Snapshot>> SnapshotFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(auto sequence_number,
                          GetJsonValueOptional<int64_t>(json, kSequenceNumber));
  ICEBERG_ASSIGN_OR_RAISE(auto unix_ms, GetJsonValue<int64_t>(json, kTimestampMs));
  auto timestamp_ms = TimePointMsFromUnixMs(unix_ms);
  ICEBERG_ASSIGN_OR_RAISE(auto manifest_list,
                          GetJsonValue<std::string>(json, kManifestList));

  ICEBERG_ASSIGN_OR_RAISE(auto parent_snapshot_id,
                          GetJsonValueOptional<int64_t>(json, kParentSnapshotId));

  ICEBERG_ASSIGN_OR_RAISE(auto summary_json,
                          GetJsonValueOptional<nlohmann::json>(json, kSummary));
  std::unordered_map<std::string, std::string> summary;
  if (summary_json.has_value()) {
    for (const auto& [key, value] : summary_json->items()) {
      // if (!kValidSnapshotSummaryFields.contains(key)) {
      //   return JsonParseError("Invalid snapshot summary field: {}", key);
      // }
      if (!value.is_string()) {
        return JsonParseError("Invalid snapshot summary field value: {}",
                              SafeDumpJson(value));
      }
      if (key == SnapshotSummaryFields::kOperation &&
          !kValidDataOperation.contains(value.get<std::string>())) {
        return JsonParseError("Invalid snapshot operation: {}", SafeDumpJson(value));
      }
      summary[key] = value.get<std::string>();
    }
    // If summary is available but operation is missing, set operation to overwrite.
    if (!summary.contains(SnapshotSummaryFields::kOperation)) {
      summary[SnapshotSummaryFields::kOperation] = DataOperation::kOverwrite;
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto schema_id, GetJsonValueOptional<int32_t>(json, kSchemaId));

  return std::make_unique<Snapshot>(
      snapshot_id, parent_snapshot_id,
      sequence_number.value_or(TableMetadata::kInitialSequenceNumber), timestamp_ms,
      manifest_list, std::move(summary), schema_id);
}

nlohmann::json ToJson(const BlobMetadata& blob_metadata) {
  nlohmann::json json;
  json[kType] = blob_metadata.type;
  json[kSnapshotId] = blob_metadata.source_snapshot_id;
  json[kSequenceNumber] = blob_metadata.source_snapshot_sequence_number;
  json[kFields] = blob_metadata.fields;
  if (!blob_metadata.properties.empty()) {
    json[kProperties] = blob_metadata.properties;
  }
  return json;
}

Result<BlobMetadata> BlobMetadataFromJson(const nlohmann::json& json) {
  BlobMetadata blob_metadata;
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.type, GetJsonValue<std::string>(json, kType));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.source_snapshot_id,
                          GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.source_snapshot_sequence_number,
                          GetJsonValue<int64_t>(json, kSequenceNumber));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.fields,
                          GetJsonValue<std::vector<int32_t>>(json, kFields));
  ICEBERG_ASSIGN_OR_RAISE(
      blob_metadata.properties,
      (GetJsonValueOrDefault<std::unordered_map<std::string, std::string>>(json,
                                                                           kProperties)));
  return blob_metadata;
}

nlohmann::json ToJson(const StatisticsFile& statistics_file) {
  nlohmann::json json;
  json[kSnapshotId] = statistics_file.snapshot_id;
  json[kStatisticsPath] = statistics_file.path;
  json[kFileSizeInBytes] = statistics_file.file_size_in_bytes;
  json[kFileFooterSizeInBytes] = statistics_file.file_footer_size_in_bytes;

  nlohmann::json blob_metadata_array = nlohmann::json::array();
  for (const auto& blob_metadata : statistics_file.blob_metadata) {
    blob_metadata_array.push_back(ToJson(blob_metadata));
  }
  json[kBlobMetadata] = blob_metadata_array;

  return json;
}

Result<std::unique_ptr<StatisticsFile>> StatisticsFileFromJson(
    const nlohmann::json& json) {
  auto stats_file = std::make_unique<StatisticsFile>();
  ICEBERG_ASSIGN_OR_RAISE(stats_file->snapshot_id,
                          GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(stats_file->path,
                          GetJsonValue<std::string>(json, kStatisticsPath));
  ICEBERG_ASSIGN_OR_RAISE(stats_file->file_size_in_bytes,
                          GetJsonValue<int64_t>(json, kFileSizeInBytes));
  ICEBERG_ASSIGN_OR_RAISE(stats_file->file_footer_size_in_bytes,
                          GetJsonValue<int64_t>(json, kFileFooterSizeInBytes));

  ICEBERG_ASSIGN_OR_RAISE(auto blob_metadata_array,
                          GetJsonValue<nlohmann::json>(json, kBlobMetadata));
  for (const auto& blob_json : blob_metadata_array) {
    ICEBERG_ASSIGN_OR_RAISE(auto blob, BlobMetadataFromJson(blob_json));
    stats_file->blob_metadata.push_back(std::move(blob));
  }

  return stats_file;
}

nlohmann::json ToJson(const PartitionStatisticsFile& partition_statistics_file) {
  nlohmann::json json;
  json[kSnapshotId] = partition_statistics_file.snapshot_id;
  json[kStatisticsPath] = partition_statistics_file.path;
  json[kFileSizeInBytes] = partition_statistics_file.file_size_in_bytes;
  return json;
}

Result<std::unique_ptr<PartitionStatisticsFile>> PartitionStatisticsFileFromJson(
    const nlohmann::json& json) {
  auto stats_file = std::make_unique<PartitionStatisticsFile>();
  ICEBERG_ASSIGN_OR_RAISE(stats_file->snapshot_id,
                          GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(stats_file->path,
                          GetJsonValue<std::string>(json, kStatisticsPath));
  ICEBERG_ASSIGN_OR_RAISE(stats_file->file_size_in_bytes,
                          GetJsonValue<int64_t>(json, kFileSizeInBytes));
  return stats_file;
}

nlohmann::json ToJson(const SnapshotLogEntry& snapshot_log_entry) {
  nlohmann::json json;
  json[kTimestampMs] = UnixMsFromTimePointMs(snapshot_log_entry.timestamp_ms);
  json[kSnapshotId] = snapshot_log_entry.snapshot_id;
  return json;
}

Result<SnapshotLogEntry> SnapshotLogEntryFromJson(const nlohmann::json& json) {
  SnapshotLogEntry snapshot_log_entry;
  ICEBERG_ASSIGN_OR_RAISE(auto unix_ms, GetJsonValue<int64_t>(json, kTimestampMs));
  snapshot_log_entry.timestamp_ms = TimePointMsFromUnixMs(unix_ms);
  ICEBERG_ASSIGN_OR_RAISE(snapshot_log_entry.snapshot_id,
                          GetJsonValue<int64_t>(json, kSnapshotId));
  return snapshot_log_entry;
}

nlohmann::json ToJson(const MetadataLogEntry& metadata_log_entry) {
  nlohmann::json json;
  json[kTimestampMs] = UnixMsFromTimePointMs(metadata_log_entry.timestamp_ms);
  json[kMetadataFile] = metadata_log_entry.metadata_file;
  return json;
}

Result<MetadataLogEntry> MetadataLogEntryFromJson(const nlohmann::json& json) {
  MetadataLogEntry metadata_log_entry;
  ICEBERG_ASSIGN_OR_RAISE(auto unix_ms, GetJsonValue<int64_t>(json, kTimestampMs));
  metadata_log_entry.timestamp_ms = TimePointMsFromUnixMs(unix_ms);
  ICEBERG_ASSIGN_OR_RAISE(metadata_log_entry.metadata_file,
                          GetJsonValue<std::string>(json, kMetadataFile));
  return metadata_log_entry;
}

nlohmann::json ToJson(const TableMetadata& table_metadata) {
  nlohmann::json json;

  json[kFormatVersion] = table_metadata.format_version;
  json[kTableUuid] = table_metadata.table_uuid;
  json[kLocation] = table_metadata.location;
  if (table_metadata.format_version > 1) {
    json[kLastSequenceNumber] = table_metadata.last_sequence_number;
  }
  json[kLastUpdatedMs] = UnixMsFromTimePointMs(table_metadata.last_updated_ms);
  json[kLastColumnId] = table_metadata.last_column_id;

  // for older readers, continue writing the current schema as "schema".
  // this is only needed for v1 because support for schemas and current-schema-id
  // is required in v2 and later.
  if (table_metadata.format_version == 1) {
    for (const auto& schema : table_metadata.schemas) {
      if (schema->schema_id() == table_metadata.current_schema_id) {
        json[kSchema] = ToJson(*schema);
        break;
      }
    }
  }

  // write the current schema ID and schema list
  json[kCurrentSchemaId] = table_metadata.current_schema_id;
  json[kSchemas] = ToJsonList(table_metadata.schemas);

  // for older readers, continue writing the default spec as "partition-spec"
  if (table_metadata.format_version == 1) {
    for (const auto& partition_spec : table_metadata.partition_specs) {
      if (partition_spec->spec_id() == table_metadata.default_spec_id) {
        json[kPartitionSpec] = ToJson(*partition_spec);
        break;
      }
    }
  }

  // write the default spec ID and spec list
  json[kDefaultSpecId] = table_metadata.default_spec_id;
  json[kPartitionSpecs] = ToJsonList(table_metadata.partition_specs);
  json[kLastPartitionId] = table_metadata.last_partition_id;

  // write the default order ID and sort order list
  json[kDefaultSortOrderId] = table_metadata.default_sort_order_id;
  json[kSortOrders] = ToJsonList(table_metadata.sort_orders);

  // write properties map
  json[kProperties] = table_metadata.properties.configs();

  if (std::ranges::any_of(table_metadata.snapshots, [&](const auto& snapshot) {
        return snapshot->snapshot_id == table_metadata.current_snapshot_id;
      })) {
    json[kCurrentSnapshotId] = table_metadata.current_snapshot_id;
  } else {
    json[kCurrentSnapshotId] = nlohmann::json::value_t::null;
  }

  if (table_metadata.format_version >= 3) {
    json[kNextRowId] = table_metadata.next_row_id;
  }

  json[kRefs] = ToJsonMap(table_metadata.refs);
  json[kSnapshots] = ToJsonList(table_metadata.snapshots);
  json[kStatistics] = ToJsonList(table_metadata.statistics);
  json[kPartitionStatistics] = ToJsonList(table_metadata.partition_statistics);
  json[kSnapshotLog] = ToJsonList(table_metadata.snapshot_log);
  json[kMetadataLog] = ToJsonList(table_metadata.metadata_log);

  return json;
}

Result<std::string> ToJsonString(const TableMetadata& table_metadata) {
  return ToJsonString(ToJson(table_metadata));
}

namespace {

/// \brief Parse the schemas from the JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] format_version The format version of the table.
/// \param[out] current_schema_id The current schema ID.
/// \param[out] schemas The list of schemas.
///
/// \return The current schema or parse error.
Result<std::shared_ptr<Schema>> ParseSchemas(
    const nlohmann::json& json, int8_t format_version, int32_t& current_schema_id,
    std::vector<std::shared_ptr<Schema>>& schemas) {
  std::shared_ptr<Schema> current_schema;
  if (json.contains(kSchemas)) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_array,
                            GetJsonValue<nlohmann::json>(json, kSchemas));
    if (!schema_array.is_array()) {
      return JsonParseError("Cannot parse schemas from non-array: {}",
                            SafeDumpJson(schema_array));
    }

    ICEBERG_ASSIGN_OR_RAISE(current_schema_id,
                            GetJsonValue<int32_t>(json, kCurrentSchemaId));
    for (const auto& schema_json : schema_array) {
      ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<Schema> schema,
                              SchemaFromJson(schema_json));
      if (schema->schema_id() == current_schema_id) {
        current_schema = schema;
      }
      schemas.push_back(std::move(schema));
    }
    if (!current_schema) {
      return JsonParseError("Cannot find schema with {}={} from {}", kCurrentSchemaId,
                            current_schema_id, SafeDumpJson(schema_array));
    }
  } else {
    if (format_version != 1) {
      return JsonParseError("{} must exist in format v{}", kSchemas, format_version);
    }
    ICEBERG_ASSIGN_OR_RAISE(auto schema_json,
                            GetJsonValue<nlohmann::json>(json, kSchema));
    ICEBERG_ASSIGN_OR_RAISE(current_schema, SchemaFromJson(schema_json));
    current_schema_id = current_schema->schema_id();
    schemas.push_back(current_schema);
  }
  return current_schema;
}

/// \brief Parse the partition specs from the JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] format_version The format version of the table.
/// \param[in] current_schema The current schema.
/// \param[out] default_spec_id The default partition spec ID.
/// \param[out] partition_specs The list of partition specs.
Status ParsePartitionSpecs(const nlohmann::json& json, int8_t format_version,
                           const std::shared_ptr<Schema>& current_schema,
                           int32_t& default_spec_id,
                           std::vector<std::shared_ptr<PartitionSpec>>& partition_specs) {
  if (json.contains(kPartitionSpecs)) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_array,
                            GetJsonValue<nlohmann::json>(json, kPartitionSpecs));
    if (!spec_array.is_array()) {
      return JsonParseError("Cannot parse partition specs from non-array: {}",
                            SafeDumpJson(spec_array));
    }
    ICEBERG_ASSIGN_OR_RAISE(default_spec_id, GetJsonValue<int32_t>(json, kDefaultSpecId));

    for (const auto& spec_json : spec_array) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto spec, PartitionSpecFromJson(current_schema, spec_json, default_spec_id));
      partition_specs.push_back(std::move(spec));
    }
  } else {
    if (format_version != 1) {
      return JsonParseError("{} must exist in format v{}", kPartitionSpecs,
                            format_version);
    }

    ICEBERG_ASSIGN_OR_RAISE(auto partition_spec_json,
                            GetJsonValue<nlohmann::json>(json, kPartitionSpec));
    if (!partition_spec_json.is_array()) {
      return JsonParseError("Cannot parse v1 partition spec from non-array: {}",
                            SafeDumpJson(partition_spec_json));
    }

    int32_t next_partition_field_id = PartitionSpec::kLegacyPartitionDataIdStart;
    std::vector<PartitionField> fields;
    for (const auto& entry_json : partition_spec_json) {
      ICEBERG_ASSIGN_OR_RAISE(
          auto field, PartitionFieldFromJson(
                          entry_json, /*allow_field_id_missing=*/format_version == 1));
      int32_t field_id = field->field_id();
      if (field_id == SchemaField::kInvalidFieldId) {
        // If the field ID is not set, we need to assign a new one
        field_id = next_partition_field_id++;
      }
      fields.emplace_back(field->source_id(), field_id, std::string(field->name()),
                          std::move(field->transform()));
    }

    // Create partition spec with schema validation
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec,
        PartitionSpec::Make(*current_schema, PartitionSpec::kInitialSpecId,
                            std::move(fields), /*allow_missing_fields=*/false));
    default_spec_id = spec->spec_id();
    partition_specs.push_back(std::move(spec));
  }

  return {};
}

/// \brief Parse the sort orders from the JSON object.
///
/// \param[in] json The JSON object to parse.
/// \param[in] format_version The format version of the table.
/// \param[in] current_schema The current schema.
/// \param[out] default_sort_order_id The default sort order ID.
/// \param[out] sort_orders The list of sort orders.
Status ParseSortOrders(const nlohmann::json& json, int8_t format_version,
                       const std::shared_ptr<Schema>& current_schema,
                       int32_t& default_sort_order_id,
                       std::vector<std::shared_ptr<SortOrder>>& sort_orders) {
  if (json.contains(kSortOrders)) {
    ICEBERG_ASSIGN_OR_RAISE(default_sort_order_id,
                            GetJsonValue<int32_t>(json, kDefaultSortOrderId));
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order_array,
                            GetJsonValue<nlohmann::json>(json, kSortOrders));
    for (const auto& sort_order_json : sort_order_array) {
      ICEBERG_ASSIGN_OR_RAISE(auto sort_order,
                              SortOrderFromJson(sort_order_json, current_schema));
      sort_orders.push_back(std::move(sort_order));
    }
  } else {
    if (format_version > 1) {
      return JsonParseError("{} must exist in format v{}", kSortOrders, format_version);
    }
    auto sort_order = SortOrder::Unsorted();
    default_sort_order_id = sort_order->order_id();
    sort_orders.push_back(std::move(sort_order));
  }
  return {};
}

}  // namespace

Result<std::unique_ptr<TableMetadata>> TableMetadataFromJson(const nlohmann::json& json) {
  if (!json.is_object()) {
    return JsonParseError("Cannot parse metadata from a non-object: {}",
                          SafeDumpJson(json));
  }

  auto table_metadata = std::make_unique<TableMetadata>();

  ICEBERG_ASSIGN_OR_RAISE(table_metadata->format_version,
                          GetJsonValue<int8_t>(json, kFormatVersion));
  if (table_metadata->format_version < 1 ||
      table_metadata->format_version > TableMetadata::kSupportedTableFormatVersion) {
    return JsonParseError("Cannot read unsupported version: {}",
                          table_metadata->format_version);
  }

  ICEBERG_ASSIGN_OR_RAISE(table_metadata->table_uuid,
                          GetJsonValueOrDefault<std::string>(json, kTableUuid));
  ICEBERG_ASSIGN_OR_RAISE(table_metadata->location,
                          GetJsonValue<std::string>(json, kLocation));

  if (table_metadata->format_version > 1) {
    ICEBERG_ASSIGN_OR_RAISE(table_metadata->last_sequence_number,
                            GetJsonValue<int64_t>(json, kLastSequenceNumber));
  } else {
    table_metadata->last_sequence_number = TableMetadata::kInitialSequenceNumber;
  }
  ICEBERG_ASSIGN_OR_RAISE(table_metadata->last_column_id,
                          GetJsonValue<int32_t>(json, kLastColumnId));

  ICEBERG_ASSIGN_OR_RAISE(
      auto current_schema,
      ParseSchemas(json, table_metadata->format_version,
                   table_metadata->current_schema_id, table_metadata->schemas));

  ICEBERG_RETURN_UNEXPECTED(ParsePartitionSpecs(
      json, table_metadata->format_version, current_schema,
      table_metadata->default_spec_id, table_metadata->partition_specs));

  if (json.contains(kLastPartitionId)) {
    ICEBERG_ASSIGN_OR_RAISE(table_metadata->last_partition_id,
                            GetJsonValue<int32_t>(json, kLastPartitionId));
  } else {
    if (table_metadata->format_version > 1) {
      return JsonParseError("{} must exist in format v{}", kLastPartitionId,
                            table_metadata->format_version);
    }

    if (table_metadata->partition_specs.empty()) {
      table_metadata->last_partition_id =
          PartitionSpec::Unpartitioned()->last_assigned_field_id();
    } else {
      table_metadata->last_partition_id =
          std::ranges::max(table_metadata->partition_specs, {}, [](const auto& spec) {
            return spec->last_assigned_field_id();
          })->last_assigned_field_id();
    }
  }

  ICEBERG_RETURN_UNEXPECTED(ParseSortOrders(
      json, table_metadata->format_version, current_schema,
      table_metadata->default_sort_order_id, table_metadata->sort_orders));

  if (json.contains(kProperties)) {
    ICEBERG_ASSIGN_OR_RAISE(auto properties, FromJsonMap(json, kProperties));
    table_metadata->properties = TableProperties::FromMap(std::move(properties));
  }

  // This field is optional, but internally we set this to -1 when not set
  ICEBERG_ASSIGN_OR_RAISE(
      table_metadata->current_snapshot_id,
      GetJsonValueOrDefault<int64_t>(json, kCurrentSnapshotId, kInvalidSnapshotId));

  if (table_metadata->format_version >= 3) {
    ICEBERG_ASSIGN_OR_RAISE(table_metadata->next_row_id,
                            GetJsonValue<int64_t>(json, kNextRowId));
  } else {
    table_metadata->next_row_id = TableMetadata::kInitialRowId;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto last_updated_ms,
                          GetJsonValue<int64_t>(json, kLastUpdatedMs));
  table_metadata->last_updated_ms =
      TimePointMs{std::chrono::milliseconds(last_updated_ms)};

  if (json.contains(kRefs)) {
    ICEBERG_ASSIGN_OR_RAISE(
        table_metadata->refs,
        FromJsonMap<std::shared_ptr<SnapshotRef>>(json, kRefs, SnapshotRefFromJson));
  } else if (table_metadata->current_snapshot_id != kInvalidSnapshotId) {
    table_metadata->refs["main"] = std::make_unique<SnapshotRef>(SnapshotRef{
        .snapshot_id = table_metadata->current_snapshot_id,
        .retention = SnapshotRef::Branch{},
    });
  }

  ICEBERG_ASSIGN_OR_RAISE(table_metadata->snapshots,
                          FromJsonList<Snapshot>(json, kSnapshots, SnapshotFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      table_metadata->statistics,
      FromJsonList<StatisticsFile>(json, kStatistics, StatisticsFileFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      table_metadata->partition_statistics,
      FromJsonList<PartitionStatisticsFile>(json, kPartitionStatistics,
                                            PartitionStatisticsFileFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      table_metadata->snapshot_log,
      FromJsonList<SnapshotLogEntry>(json, kSnapshotLog, SnapshotLogEntryFromJson));
  ICEBERG_ASSIGN_OR_RAISE(
      table_metadata->metadata_log,
      FromJsonList<MetadataLogEntry>(json, kMetadataLog, MetadataLogEntryFromJson));

  return table_metadata;
}

Result<nlohmann::json> FromJsonString(const std::string& json_string) {
  auto json =
      nlohmann::json::parse(json_string, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (json.is_discarded()) [[unlikely]] {
    return JsonParseError("Failed to parse JSON string: {}", json_string);
  }
  return json;
}

Result<std::string> ToJsonString(const nlohmann::json& json) {
  try {
    return json.dump();
  } catch (const std::exception& e) {
    return JsonParseError("Failed to serialize to JSON string: {}", e.what());
  }
}

nlohmann::json ToJson(const MappedField& field) {
  nlohmann::json json;
  if (field.field_id.has_value()) {
    json[kFieldId] = field.field_id.value();
  }

  nlohmann::json names = nlohmann::json::array();
  for (const auto& name : field.names) {
    names.push_back(name);
  }
  json[kNames] = names;

  if (field.nested_mapping != nullptr) {
    json[kFields] = ToJson(*field.nested_mapping);
  }
  return json;
}

Result<MappedField> MappedFieldFromJson(const nlohmann::json& json) {
  if (!json.is_object()) [[unlikely]] {
    return JsonParseError("Cannot parse non-object mapping field: {}",
                          SafeDumpJson(json));
  }

  ICEBERG_ASSIGN_OR_RAISE(std::optional<int32_t> field_id,
                          GetJsonValueOptional<int32_t>(json, kFieldId));

  std::vector<std::string> names;
  if (json.contains(kNames)) {
    ICEBERG_ASSIGN_OR_RAISE(names, GetJsonValue<std::vector<std::string>>(json, kNames));
  }

  std::unique_ptr<MappedFields> nested_mapping;
  if (json.contains(kFields)) {
    ICEBERG_ASSIGN_OR_RAISE(auto fields_json,
                            GetJsonValue<nlohmann::json>(json, kFields));
    ICEBERG_ASSIGN_OR_RAISE(nested_mapping, MappedFieldsFromJson(fields_json));
  }

  return MappedField{.names = {names.cbegin(), names.cend()},
                     .field_id = field_id,
                     .nested_mapping = std::move(nested_mapping)};
}

nlohmann::json ToJson(const MappedFields& mapped_fields) {
  nlohmann::json array = nlohmann::json::array();
  for (const auto& field : mapped_fields.fields()) {
    array.push_back(ToJson(field));
  }
  return array;
}

Result<std::unique_ptr<MappedFields>> MappedFieldsFromJson(const nlohmann::json& json) {
  if (!json.is_array()) [[unlikely]] {
    return JsonParseError("Cannot parse non-array mapping fields: {}",
                          SafeDumpJson(json));
  }

  std::vector<MappedField> fields;
  for (const auto& field_json : json) {
    ICEBERG_ASSIGN_OR_RAISE(auto field, MappedFieldFromJson(field_json));
    fields.push_back(std::move(field));
  }

  return MappedFields::Make(std::move(fields));
}

nlohmann::json ToJson(const NameMapping& name_mapping) {
  return ToJson(name_mapping.AsMappedFields());
}

Result<std::unique_ptr<NameMapping>> NameMappingFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto mapped_fields, MappedFieldsFromJson(json));
  return NameMapping::Make(std::move(mapped_fields));
}

nlohmann::json ToJson(const TableIdentifier& identifier) {
  nlohmann::json json;
  json[kNamespace] = identifier.ns.levels;
  json[kName] = identifier.name;
  return json;
}

Result<TableIdentifier> TableIdentifierFromJson(const nlohmann::json& json) {
  TableIdentifier identifier;
  ICEBERG_ASSIGN_OR_RAISE(
      identifier.ns.levels,
      GetJsonValueOrDefault<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(identifier.name, GetJsonValue<std::string>(json, kName));

  return identifier;
}

nlohmann::json ToJson(const Namespace& ns) { return ns.levels; }

Result<Namespace> NamespaceFromJson(const nlohmann::json& json) {
  if (!json.is_array()) [[unlikely]] {
    return JsonParseError("Cannot parse namespace from non-array:{}", SafeDumpJson(json));
  }
  Namespace ns;
  ICEBERG_ASSIGN_OR_RAISE(ns.levels, GetTypedJsonValue<std::vector<std::string>>(json));
  return ns;
}

nlohmann::json ToJson(const TableUpdate& update) {
  nlohmann::json json;
  switch (update.kind()) {
    case TableUpdate::Kind::kAssignUUID: {
      const auto& u = internal::checked_cast<const table::AssignUUID&>(update);
      json[kAction] = kActionAssignUUID;
      json[kUUID] = u.uuid();
      break;
    }
    case TableUpdate::Kind::kUpgradeFormatVersion: {
      const auto& u = internal::checked_cast<const table::UpgradeFormatVersion&>(update);
      json[kAction] = kActionUpgradeFormatVersion;
      json[kFormatVersion] = u.format_version();
      break;
    }
    case TableUpdate::Kind::kAddSchema: {
      const auto& u = internal::checked_cast<const table::AddSchema&>(update);
      json[kAction] = kActionAddSchema;
      if (u.schema()) {
        json[kSchema] = ToJson(*u.schema());
      } else {
        json[kSchema] = nlohmann::json::value_t::null;
      }
      json[kLastColumnId] = u.last_column_id();
      break;
    }
    case TableUpdate::Kind::kSetCurrentSchema: {
      const auto& u = internal::checked_cast<const table::SetCurrentSchema&>(update);
      json[kAction] = kActionSetCurrentSchema;
      json[kSchemaId] = u.schema_id();
      break;
    }
    case TableUpdate::Kind::kAddPartitionSpec: {
      const auto& u = internal::checked_cast<const table::AddPartitionSpec&>(update);
      json[kAction] = kActionAddPartitionSpec;
      if (u.spec()) {
        json[kSpec] = ToJson(*u.spec());
      } else {
        json[kSpec] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableUpdate::Kind::kSetDefaultPartitionSpec: {
      const auto& u =
          internal::checked_cast<const table::SetDefaultPartitionSpec&>(update);
      json[kAction] = kActionSetDefaultPartitionSpec;
      json[kSpecId] = u.spec_id();
      break;
    }
    case TableUpdate::Kind::kRemovePartitionSpecs: {
      const auto& u = internal::checked_cast<const table::RemovePartitionSpecs&>(update);
      json[kAction] = kActionRemovePartitionSpecs;
      json[kSpecIds] = u.spec_ids();
      break;
    }
    case TableUpdate::Kind::kRemoveSchemas: {
      const auto& u = internal::checked_cast<const table::RemoveSchemas&>(update);
      json[kAction] = kActionRemoveSchemas;
      json[kSchemaIds] = u.schema_ids();
      break;
    }
    case TableUpdate::Kind::kAddSortOrder: {
      const auto& u = internal::checked_cast<const table::AddSortOrder&>(update);
      json[kAction] = kActionAddSortOrder;
      if (u.sort_order()) {
        json[kSortOrder] = ToJson(*u.sort_order());
      } else {
        json[kSortOrder] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableUpdate::Kind::kSetDefaultSortOrder: {
      const auto& u = internal::checked_cast<const table::SetDefaultSortOrder&>(update);
      json[kAction] = kActionSetDefaultSortOrder;
      json[kSortOrderId] = u.sort_order_id();
      break;
    }
    case TableUpdate::Kind::kAddSnapshot: {
      const auto& u = internal::checked_cast<const table::AddSnapshot&>(update);
      json[kAction] = kActionAddSnapshot;
      if (u.snapshot()) {
        json[kSnapshot] = ToJson(*u.snapshot());
      } else {
        json[kSnapshot] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableUpdate::Kind::kRemoveSnapshots: {
      const auto& u = internal::checked_cast<const table::RemoveSnapshots&>(update);
      json[kAction] = kActionRemoveSnapshots;
      json[kSnapshotIds] = u.snapshot_ids();
      break;
    }
    case TableUpdate::Kind::kRemoveSnapshotRef: {
      const auto& u = internal::checked_cast<const table::RemoveSnapshotRef&>(update);
      json[kAction] = kActionRemoveSnapshotRef;
      json[kRefName] = u.ref_name();
      break;
    }
    case TableUpdate::Kind::kSetSnapshotRef: {
      const auto& u = internal::checked_cast<const table::SetSnapshotRef&>(update);
      json[kAction] = kActionSetSnapshotRef;
      json[kRefName] = u.ref_name();
      json[kSnapshotId] = u.snapshot_id();
      json[kType] = ToString(u.type());
      SetOptionalField(json, kMinSnapshotsToKeep, u.min_snapshots_to_keep());
      SetOptionalField(json, kMaxSnapshotAgeMs, u.max_snapshot_age_ms());
      SetOptionalField(json, kMaxRefAgeMs, u.max_ref_age_ms());
      break;
    }
    case TableUpdate::Kind::kSetProperties: {
      const auto& u = internal::checked_cast<const table::SetProperties&>(update);
      json[kAction] = kActionSetProperties;
      json[kUpdates] = u.updated();
      break;
    }
    case TableUpdate::Kind::kRemoveProperties: {
      const auto& u = internal::checked_cast<const table::RemoveProperties&>(update);
      json[kAction] = kActionRemoveProperties;
      json[kRemovals] = std::vector<std::string>(u.removed().begin(), u.removed().end());
      break;
    }
    case TableUpdate::Kind::kSetLocation: {
      const auto& u = internal::checked_cast<const table::SetLocation&>(update);
      json[kAction] = kActionSetLocation;
      json[kLocation] = u.location();
      break;
    }
    case TableUpdate::Kind::kSetStatistics: {
      const auto& u = internal::checked_cast<const table::SetStatistics&>(update);
      json[kAction] = kActionSetStatistics;
      if (u.statistics_file()) {
        json[kStatistics] = ToJson(*u.statistics_file());
      } else {
        json[kStatistics] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableUpdate::Kind::kRemoveStatistics: {
      const auto& u = internal::checked_cast<const table::RemoveStatistics&>(update);
      json[kAction] = kActionRemoveStatistics;
      json[kSnapshotId] = u.snapshot_id();
      break;
    }
    case TableUpdate::Kind::kSetPartitionStatistics: {
      const auto& u =
          internal::checked_cast<const table::SetPartitionStatistics&>(update);
      json[kAction] = kActionSetPartitionStatistics;
      if (u.partition_statistics_file()) {
        json[kPartitionStatistics] = ToJson(*u.partition_statistics_file());
      } else {
        json[kPartitionStatistics] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableUpdate::Kind::kRemovePartitionStatistics: {
      const auto& u =
          internal::checked_cast<const table::RemovePartitionStatistics&>(update);
      json[kAction] = kActionRemovePartitionStatistics;
      json[kSnapshotId] = u.snapshot_id();
      break;
    }
  }
  return json;
}

nlohmann::json ToJson(const TableRequirement& requirement) {
  nlohmann::json json;
  switch (requirement.kind()) {
    case TableRequirement::Kind::kAssertDoesNotExist:
      json[kType] = kRequirementAssertDoesNotExist;
      break;
    case TableRequirement::Kind::kAssertUUID: {
      const auto& r = internal::checked_cast<const table::AssertUUID&>(requirement);
      json[kType] = kRequirementAssertUUID;
      json[kUUID] = r.uuid();
      break;
    }
    case TableRequirement::Kind::kAssertRefSnapshotID: {
      const auto& r =
          internal::checked_cast<const table::AssertRefSnapshotID&>(requirement);
      json[kType] = kRequirementAssertRefSnapshotID;
      json[kRefName] = r.ref_name();
      if (r.snapshot_id().has_value()) {
        json[kSnapshotId] = r.snapshot_id().value();
      } else {
        json[kSnapshotId] = nlohmann::json::value_t::null;
      }
      break;
    }
    case TableRequirement::Kind::kAssertLastAssignedFieldId: {
      const auto& r =
          internal::checked_cast<const table::AssertLastAssignedFieldId&>(requirement);
      json[kType] = kRequirementAssertLastAssignedFieldId;
      json[kLastAssignedFieldId] = r.last_assigned_field_id();
      break;
    }
    case TableRequirement::Kind::kAssertCurrentSchemaID: {
      const auto& r =
          internal::checked_cast<const table::AssertCurrentSchemaID&>(requirement);
      json[kType] = kRequirementAssertCurrentSchemaID;
      json[kCurrentSchemaId] = r.schema_id();
      break;
    }
    case TableRequirement::Kind::kAssertLastAssignedPartitionId: {
      const auto& r = internal::checked_cast<const table::AssertLastAssignedPartitionId&>(
          requirement);
      json[kType] = kRequirementAssertLastAssignedPartitionId;
      json[kLastAssignedPartitionId] = r.last_assigned_partition_id();
      break;
    }
    case TableRequirement::Kind::kAssertDefaultSpecID: {
      const auto& r =
          internal::checked_cast<const table::AssertDefaultSpecID&>(requirement);
      json[kType] = kRequirementAssertDefaultSpecID;
      json[kDefaultSpecId] = r.spec_id();
      break;
    }
    case TableRequirement::Kind::kAssertDefaultSortOrderID: {
      const auto& r =
          internal::checked_cast<const table::AssertDefaultSortOrderID&>(requirement);
      json[kType] = kRequirementAssertDefaultSortOrderID;
      json[kDefaultSortOrderId] = r.sort_order_id();
      break;
    }
  }
  return json;
}

Result<std::unique_ptr<TableUpdate>> TableUpdateFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto action, GetJsonValue<std::string>(json, kAction));

  if (action == kActionAssignUUID) {
    ICEBERG_ASSIGN_OR_RAISE(auto uuid, GetJsonValue<std::string>(json, kUUID));
    return std::make_unique<table::AssignUUID>(std::move(uuid));
  }
  if (action == kActionUpgradeFormatVersion) {
    ICEBERG_ASSIGN_OR_RAISE(auto format_version,
                            GetJsonValue<int8_t>(json, kFormatVersion));
    return std::make_unique<table::UpgradeFormatVersion>(format_version);
  }
  if (action == kActionAddSchema) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_json,
                            GetJsonValue<nlohmann::json>(json, kSchema));
    ICEBERG_ASSIGN_OR_RAISE(auto parsed_schema, SchemaFromJson(schema_json));
    ICEBERG_ASSIGN_OR_RAISE(auto last_column_id,
                            GetJsonValue<int32_t>(json, kLastColumnId));
    return std::make_unique<table::AddSchema>(std::move(parsed_schema), last_column_id);
  }
  if (action == kActionSetCurrentSchema) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_id, GetJsonValue<int32_t>(json, kSchemaId));
    return std::make_unique<table::SetCurrentSchema>(schema_id);
  }
  if (action == kActionAddPartitionSpec) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_json, GetJsonValue<nlohmann::json>(json, kSpec));
    ICEBERG_ASSIGN_OR_RAISE(auto spec, PartitionSpecFromJson(spec_json));
    return std::make_unique<table::AddPartitionSpec>(std::move(spec));
  }
  if (action == kActionSetDefaultPartitionSpec) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kSpecId));
    return std::make_unique<table::SetDefaultPartitionSpec>(spec_id);
  }
  if (action == kActionRemovePartitionSpecs) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_ids,
                            GetJsonValue<std::vector<int32_t>>(json, kSpecIds));
    return std::make_unique<table::RemovePartitionSpecs>(std::move(spec_ids));
  }
  if (action == kActionRemoveSchemas) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_ids_vec,
                            GetJsonValue<std::vector<int32_t>>(json, kSchemaIds));
    std::unordered_set<int32_t> schema_ids(schema_ids_vec.begin(), schema_ids_vec.end());
    return std::make_unique<table::RemoveSchemas>(std::move(schema_ids));
  }
  if (action == kActionAddSortOrder) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order_json,
                            GetJsonValue<nlohmann::json>(json, kSortOrder));
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order, SortOrderFromJson(sort_order_json));
    return std::make_unique<table::AddSortOrder>(std::move(sort_order));
  }
  if (action == kActionSetDefaultSortOrder) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order_id,
                            GetJsonValue<int32_t>(json, kSortOrderId));
    return std::make_unique<table::SetDefaultSortOrder>(sort_order_id);
  }
  if (action == kActionAddSnapshot) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_json,
                            GetJsonValue<nlohmann::json>(json, kSnapshot));
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, SnapshotFromJson(snapshot_json));
    return std::make_unique<table::AddSnapshot>(std::move(snapshot));
  }
  if (action == kActionRemoveSnapshots) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_ids,
                            GetJsonValue<std::vector<int64_t>>(json, kSnapshotIds));
    return std::make_unique<table::RemoveSnapshots>(std::move(snapshot_ids));
  }
  if (action == kActionRemoveSnapshotRef) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref_name, GetJsonValue<std::string>(json, kRefName));
    return std::make_unique<table::RemoveSnapshotRef>(std::move(ref_name));
  }
  if (action == kActionSetSnapshotRef) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref_name, GetJsonValue<std::string>(json, kRefName));
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
    ICEBERG_ASSIGN_OR_RAISE(
        auto type,
        GetJsonValue<std::string>(json, kType).and_then(SnapshotRefTypeFromString));
    ICEBERG_ASSIGN_OR_RAISE(auto min_snapshots,
                            GetJsonValueOptional<int32_t>(json, kMinSnapshotsToKeep));
    ICEBERG_ASSIGN_OR_RAISE(auto max_snapshot_age,
                            GetJsonValueOptional<int64_t>(json, kMaxSnapshotAgeMs));
    ICEBERG_ASSIGN_OR_RAISE(auto max_ref_age,
                            GetJsonValueOptional<int64_t>(json, kMaxRefAgeMs));
    if (type == SnapshotRefType::kTag) {
      ICEBERG_ASSIGN_OR_RAISE(auto tag, SnapshotRef::MakeTag(snapshot_id, max_ref_age));
      return std::make_unique<table::SetSnapshotRef>(std::move(ref_name), *tag);
    } else {
      ICEBERG_CHECK(type == SnapshotRefType::kBranch,
                    "Expected branch type for snapshot ref");
      ICEBERG_ASSIGN_OR_RAISE(auto branch,
                              SnapshotRef::MakeBranch(snapshot_id, min_snapshots,
                                                      max_snapshot_age, max_ref_age));
      return std::make_unique<table::SetSnapshotRef>(std::move(ref_name), *branch);
    }
  }
  if (action == kActionSetProperties) {
    using StringMap = std::unordered_map<std::string, std::string>;
    ICEBERG_ASSIGN_OR_RAISE(auto updates, GetJsonValue<StringMap>(json, kUpdates));
    return std::make_unique<table::SetProperties>(std::move(updates));
  }
  if (action == kActionRemoveProperties) {
    ICEBERG_ASSIGN_OR_RAISE(auto removals_vec,
                            GetJsonValue<std::vector<std::string>>(json, kRemovals));
    std::unordered_set<std::string> removals(
        std::make_move_iterator(removals_vec.begin()),
        std::make_move_iterator(removals_vec.end()));
    return std::make_unique<table::RemoveProperties>(std::move(removals));
  }
  if (action == kActionSetLocation) {
    ICEBERG_ASSIGN_OR_RAISE(auto location, GetJsonValue<std::string>(json, kLocation));
    return std::make_unique<table::SetLocation>(std::move(location));
  }
  if (action == kActionSetStatistics) {
    ICEBERG_ASSIGN_OR_RAISE(auto statistics_json,
                            GetJsonValue<nlohmann::json>(json, kStatistics));
    ICEBERG_ASSIGN_OR_RAISE(auto statistics_file,
                            StatisticsFileFromJson(statistics_json));
    return std::make_unique<table::SetStatistics>(std::move(statistics_file));
  }
  if (action == kActionRemoveStatistics) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
    return std::make_unique<table::RemoveStatistics>(snapshot_id);
  }
  if (action == kActionSetPartitionStatistics) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_statistics_json,
                            GetJsonValue<nlohmann::json>(json, kPartitionStatistics));
    ICEBERG_ASSIGN_OR_RAISE(auto partition_statistics_file,
                            PartitionStatisticsFileFromJson(partition_statistics_json));
    return std::make_unique<table::SetPartitionStatistics>(
        std::move(partition_statistics_file));
  }
  if (action == kActionRemovePartitionStatistics) {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id, GetJsonValue<int64_t>(json, kSnapshotId));
    return std::make_unique<table::RemovePartitionStatistics>(snapshot_id);
  }

  return JsonParseError("Unknown table update action: {}", action);
}

Result<std::unique_ptr<TableRequirement>> TableRequirementFromJson(
    const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto type, GetJsonValue<std::string>(json, kType));

  if (type == kRequirementAssertDoesNotExist) {
    return std::make_unique<table::AssertDoesNotExist>();
  }
  if (type == kRequirementAssertUUID) {
    ICEBERG_ASSIGN_OR_RAISE(auto uuid, GetJsonValue<std::string>(json, kUUID));
    return std::make_unique<table::AssertUUID>(std::move(uuid));
  }
  if (type == kRequirementAssertRefSnapshotID) {
    ICEBERG_ASSIGN_OR_RAISE(auto ref_name, GetJsonValue<std::string>(json, kRefName));
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_id_opt,
                            GetJsonValueOptional<int64_t>(json, kSnapshotId));
    return std::make_unique<table::AssertRefSnapshotID>(std::move(ref_name),
                                                        snapshot_id_opt);
  }
  if (type == kRequirementAssertLastAssignedFieldId) {
    ICEBERG_ASSIGN_OR_RAISE(auto last_assigned_field_id,
                            GetJsonValue<int32_t>(json, kLastAssignedFieldId));
    return std::make_unique<table::AssertLastAssignedFieldId>(last_assigned_field_id);
  }
  if (type == kRequirementAssertCurrentSchemaID) {
    ICEBERG_ASSIGN_OR_RAISE(auto schema_id,
                            GetJsonValue<int32_t>(json, kCurrentSchemaId));
    return std::make_unique<table::AssertCurrentSchemaID>(schema_id);
  }
  if (type == kRequirementAssertLastAssignedPartitionId) {
    ICEBERG_ASSIGN_OR_RAISE(auto last_assigned_partition_id,
                            GetJsonValue<int32_t>(json, kLastAssignedPartitionId));
    return std::make_unique<table::AssertLastAssignedPartitionId>(
        last_assigned_partition_id);
  }
  if (type == kRequirementAssertDefaultSpecID) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kDefaultSpecId));
    return std::make_unique<table::AssertDefaultSpecID>(spec_id);
  }
  if (type == kRequirementAssertDefaultSortOrderID) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order_id,
                            GetJsonValue<int32_t>(json, kDefaultSortOrderId));
    return std::make_unique<table::AssertDefaultSortOrderID>(sort_order_id);
  }

  return JsonParseError("Unknown table requirement type: {}", type);
}

}  // namespace iceberg
