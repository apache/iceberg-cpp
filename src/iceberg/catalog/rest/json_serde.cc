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

#include <iterator>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "iceberg/catalog/rest/json_serde_internal.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/file_format.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_scan.h"
#include "iceberg/table_update.h"
#include "iceberg/util/data_file_set.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::rest {

namespace {

// REST API JSON field constants
constexpr std::string_view kNamespace = "namespace";
constexpr std::string_view kNamespaces = "namespaces";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kRemovals = "removals";
constexpr std::string_view kUpdates = "updates";
constexpr std::string_view kUpdated = "updated";
constexpr std::string_view kRemoved = "removed";
constexpr std::string_view kMissing = "missing";
constexpr std::string_view kNextPageToken = "next-page-token";
constexpr std::string_view kName = "name";
constexpr std::string_view kLocation = "location";
constexpr std::string_view kSchema = "schema";
constexpr std::string_view kPartitionSpec = "partition-spec";
constexpr std::string_view kWriteOrder = "write-order";
constexpr std::string_view kStageCreate = "stage-create";
constexpr std::string_view kMetadataLocation = "metadata-location";
constexpr std::string_view kOverwrite = "overwrite";
constexpr std::string_view kSource = "source";
constexpr std::string_view kDestination = "destination";
constexpr std::string_view kMetadata = "metadata";
constexpr std::string_view kConfig = "config";
constexpr std::string_view kStorageCredentials = "storage-credentials";
constexpr std::string_view kPrefix = "prefix";
constexpr std::string_view kIdentifiers = "identifiers";
constexpr std::string_view kOverrides = "overrides";
constexpr std::string_view kDefaults = "defaults";
constexpr std::string_view kEndpoints = "endpoints";
constexpr std::string_view kMessage = "message";
constexpr std::string_view kType = "type";
constexpr std::string_view kCode = "code";
constexpr std::string_view kStack = "stack";
constexpr std::string_view kError = "error";
constexpr std::string_view kIdentifier = "identifier";
constexpr std::string_view kRequirements = "requirements";
constexpr std::string_view kAccessToken = "access_token";
constexpr std::string_view kTokenType = "token_type";
constexpr std::string_view kExpiresIn = "expires_in";
constexpr std::string_view kIssuedTokenType = "issued_token_type";
constexpr std::string_view kRefreshToken = "refresh_token";
constexpr std::string_view kOAuthScope = "scope";
constexpr std::string_view kPlanStatus = "status";
constexpr std::string_view kPlanId = "plan-id";
constexpr std::string_view kPlanTasks = "plan-tasks";
constexpr std::string_view kFileScanTasks = "file-scan-tasks";
constexpr std::string_view kDeleteFiles = "delete-files";
constexpr std::string_view kSnapshotId = "snapshot-id";
constexpr std::string_view kSelect = "select";
constexpr std::string_view kFilter = "filter";
constexpr std::string_view kCaseSensitive = "case-sensitive";
constexpr std::string_view kUseSnapshotSchema = "use-snapshot-schema";
constexpr std::string_view kStartSnapshotId = "start-snapshot-id";
constexpr std::string_view kEndSnapshotId = "end-snapshot-id";
constexpr std::string_view kStatsFields = "stats-fields";
constexpr std::string_view kMinRowsRequested = "min-rows-requested";
constexpr std::string_view kPlanTask = "plan-task";
constexpr std::string_view kContent = "content";
constexpr std::string_view kContentData = "data";
constexpr std::string_view kContentPositionDeletes = "position-deletes";
constexpr std::string_view kContentEqualityDeletes = "equality-deletes";
constexpr std::string_view kFilePath = "file-path";
constexpr std::string_view kFileFormat = "file-format";
constexpr std::string_view kSpecId = "spec-id";
constexpr std::string_view kPartition = "partition";
constexpr std::string_view kRecordCount = "record-count";
constexpr std::string_view kFileSizeInBytes = "file-size-in-bytes";
constexpr std::string_view kColumnSizes = "column-sizes";
constexpr std::string_view kValueCounts = "value-counts";
constexpr std::string_view kNullValueCounts = "null-value-counts";
constexpr std::string_view kNanValueCounts = "nan-value-counts";
constexpr std::string_view kLowerBounds = "lower-bounds";
constexpr std::string_view kUpperBounds = "upper-bounds";
constexpr std::string_view kKeyMetadata = "key-metadata";
constexpr std::string_view kSplitOffsets = "split-offsets";
constexpr std::string_view kEqualityIds = "equality-ids";
constexpr std::string_view kSortOrderId = "sort-order-id";
constexpr std::string_view kFirstRowId = "first-row-id";
constexpr std::string_view kReferencedDataFile = "referenced-data-file";
constexpr std::string_view kContentOffset = "content-offset";
constexpr std::string_view kContentSizeInBytes = "content-size-in-bytes";
constexpr std::string_view kDataFile = "data-file";
constexpr std::string_view kDeleteFileReferences = "delete-file-references";
constexpr std::string_view kResidualFilter = "residual-filter";
constexpr std::string_view kMapKeys = "keys";
constexpr std::string_view kMapValues = "values";

template <typename Value>
Result<std::map<int32_t, Value>> KeyValueMapFromJson(const nlohmann::json& json,
                                                     std::string_view key) {
  std::map<int32_t, Value> result;
  if (!json.contains(key) || json.at(key).is_null()) {
    return result;
  }

  ICEBERG_ASSIGN_OR_RAISE(auto map_json, GetJsonValue<nlohmann::json>(json, key));
  ICEBERG_ASSIGN_OR_RAISE(auto keys,
                          GetJsonValue<std::vector<int32_t>>(map_json, kMapKeys));
  ICEBERG_ASSIGN_OR_RAISE(auto values,
                          GetJsonValue<std::vector<Value>>(map_json, kMapValues));
  if (keys.size() != values.size()) {
    return JsonParseError("'{}' map keys and values have different lengths", key);
  }

  for (size_t i = 0; i < keys.size(); ++i) {
    result[keys[i]] = std::move(values[i]);
  }
  return result;
}

template <typename Value>
void SetKeyValueMap(nlohmann::json& json, std::string_view key,
                    const std::map<int32_t, Value>& map) {
  if (map.empty()) {
    return;
  }

  std::vector<int32_t> keys;
  std::vector<Value> values;
  keys.reserve(map.size());
  values.reserve(map.size());
  for (const auto& [field_id, value] : map) {
    keys.push_back(field_id);
    values.push_back(value);
  }
  json[key] = {{kMapKeys, std::move(keys)}, {kMapValues, std::move(values)}};
}

}  // namespace

Result<DataFile> DataFileFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_spec_by_id,
    const Schema& schema) {
  if (!json.is_object()) {
    return JsonParseError("DataFile must be a JSON object: {}", SafeDumpJson(json));
  }
  DataFile data_file;

  ICEBERG_ASSIGN_OR_RAISE(auto content_str, GetJsonValue<std::string>(json, kContent));
  if (content_str == kContentData) {
    data_file.content = DataFile::Content::kData;
  } else if (content_str == kContentPositionDeletes) {
    data_file.content = DataFile::Content::kPositionDeletes;
  } else if (content_str == kContentEqualityDeletes) {
    data_file.content = DataFile::Content::kEqualityDeletes;
  } else {
    return JsonParseError("Unknown data file content: {}", content_str);
  }

  ICEBERG_ASSIGN_OR_RAISE(data_file.file_path,
                          GetJsonValue<std::string>(json, kFilePath));
  ICEBERG_ASSIGN_OR_RAISE(auto format_str, GetJsonValue<std::string>(json, kFileFormat));
  ICEBERG_ASSIGN_OR_RAISE(data_file.file_format, FileFormatTypeFromString(format_str));

  ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kSpecId));
  data_file.partition_spec_id = spec_id;

  ICEBERG_ASSIGN_OR_RAISE(auto partition_vals,
                          GetJsonValue<nlohmann::json>(json, kPartition));
  if (!partition_vals.is_array()) {
    return JsonParseError("PartitionValues must be a JSON array: {}",
                          SafeDumpJson(partition_vals));
  }
  std::vector<Literal> literals;
  auto it = partition_spec_by_id.find(spec_id);
  if (it == partition_spec_by_id.end()) {
    return JsonParseError("Invalid partition spec id: {}", spec_id);
  }
  ICEBERG_ASSIGN_OR_RAISE(auto struct_type, it->second->PartitionType(schema));
  auto fields = struct_type->fields();
  if (partition_vals.size() != fields.size()) {
    return JsonParseError("Invalid partition data size: expected = {}, actual = {}",
                          fields.size(), partition_vals.size());
  }
  for (size_t pos = 0; pos < fields.size(); ++pos) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto literal, LiteralFromJson(partition_vals[pos], fields[pos].type().get()));
    literals.push_back(std::move(literal));
  }
  data_file.partition = PartitionValues(std::move(literals));

  ICEBERG_ASSIGN_OR_RAISE(data_file.record_count,
                          GetJsonValue<int64_t>(json, kRecordCount));
  ICEBERG_ASSIGN_OR_RAISE(data_file.file_size_in_bytes,
                          GetJsonValue<int64_t>(json, kFileSizeInBytes));

  ICEBERG_ASSIGN_OR_RAISE(data_file.column_sizes,
                          KeyValueMapFromJson<int64_t>(json, kColumnSizes));
  ICEBERG_ASSIGN_OR_RAISE(data_file.value_counts,
                          KeyValueMapFromJson<int64_t>(json, kValueCounts));
  ICEBERG_ASSIGN_OR_RAISE(data_file.null_value_counts,
                          KeyValueMapFromJson<int64_t>(json, kNullValueCounts));
  ICEBERG_ASSIGN_OR_RAISE(data_file.nan_value_counts,
                          KeyValueMapFromJson<int64_t>(json, kNanValueCounts));
  ICEBERG_ASSIGN_OR_RAISE(data_file.lower_bounds,
                          KeyValueMapFromJson<std::vector<uint8_t>>(json, kLowerBounds));
  ICEBERG_ASSIGN_OR_RAISE(data_file.upper_bounds,
                          KeyValueMapFromJson<std::vector<uint8_t>>(json, kUpperBounds));

  if (json.contains(kKeyMetadata) && !json.at(kKeyMetadata).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.key_metadata,
                            GetJsonValue<std::vector<uint8_t>>(json, kKeyMetadata));
  }
  if (json.contains(kSplitOffsets) && !json.at(kSplitOffsets).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.split_offsets,
                            GetJsonValue<std::vector<int64_t>>(json, kSplitOffsets));
  }
  if (json.contains(kEqualityIds) && !json.at(kEqualityIds).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.equality_ids,
                            GetJsonValue<std::vector<int32_t>>(json, kEqualityIds));
  }
  if (json.contains(kSortOrderId) && !json.at(kSortOrderId).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.sort_order_id,
                            GetJsonValue<int32_t>(json, kSortOrderId));
  }
  if (json.contains(kFirstRowId) && !json.at(kFirstRowId).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.first_row_id,
                            GetJsonValue<int64_t>(json, kFirstRowId));
  }
  if (json.contains(kReferencedDataFile) && !json.at(kReferencedDataFile).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.referenced_data_file,
                            GetJsonValue<std::string>(json, kReferencedDataFile));
  }
  if (json.contains(kContentOffset) && !json.at(kContentOffset).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.content_offset,
                            GetJsonValue<int64_t>(json, kContentOffset));
  }
  if (json.contains(kContentSizeInBytes) && !json.at(kContentSizeInBytes).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(data_file.content_size_in_bytes,
                            GetJsonValue<int64_t>(json, kContentSizeInBytes));
  }

  return data_file;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FileScanTasksFromJson(
    const nlohmann::json& json,
    const std::vector<std::shared_ptr<DataFile>>& delete_files,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_spec_by_id,
    const Schema& schema) {
  if (!json.is_array()) {
    return JsonParseError("Cannot parse file scan tasks from non-array: {}",
                          SafeDumpJson(json));
  }
  std::vector<std::shared_ptr<FileScanTask>> file_scan_tasks;
  for (const auto& task_json : json) {
    if (!task_json.is_object()) {
      return JsonParseError("Cannot parse file scan task from a non-object: {}",
                            SafeDumpJson(task_json));
    }

    ICEBERG_ASSIGN_OR_RAISE(auto data_file_json,
                            GetJsonValue<nlohmann::json>(task_json, kDataFile));
    ICEBERG_ASSIGN_OR_RAISE(
        auto data_file, DataFileFromJson(data_file_json, partition_spec_by_id, schema));

    std::vector<std::shared_ptr<DataFile>> task_delete_files;
    if (task_json.contains(kDeleteFileReferences) &&
        !task_json.at(kDeleteFileReferences).is_null()) {
      ICEBERG_ASSIGN_OR_RAISE(auto refs, GetJsonValue<std::vector<int32_t>>(
                                             task_json, kDeleteFileReferences));
      for (int32_t ref : refs) {
        if (ref < 0 || static_cast<size_t>(ref) >= delete_files.size()) {
          return JsonParseError(
              "delete-file-references index {} is out of range (delete_files size: {})",
              ref, delete_files.size());
        }
        task_delete_files.push_back(delete_files[ref]);
      }
    }

    std::shared_ptr<Expression> residual_filter;
    if (task_json.contains(kResidualFilter) && !task_json.at(kResidualFilter).is_null()) {
      ICEBERG_ASSIGN_OR_RAISE(auto filter_json,
                              GetJsonValue<nlohmann::json>(task_json, kResidualFilter));
      ICEBERG_ASSIGN_OR_RAISE(residual_filter, ExpressionFromJson(filter_json));
    }

    file_scan_tasks.push_back(std::make_shared<FileScanTask>(
        std::make_shared<DataFile>(std::move(data_file)), std::move(task_delete_files),
        std::move(residual_filter)));
  }
  return file_scan_tasks;
}

Result<nlohmann::json> DataFileToJsonUnchecked(const DataFile& data_file) {
  nlohmann::json json;
  switch (data_file.content) {
    case DataFile::Content::kData:
      json[kContent] = kContentData;
      break;
    case DataFile::Content::kPositionDeletes:
      json[kContent] = kContentPositionDeletes;
      break;
    case DataFile::Content::kEqualityDeletes:
      json[kContent] = kContentEqualityDeletes;
      break;
  }
  json[kFilePath] = data_file.file_path;
  json[kFileFormat] = ToString(data_file.file_format);

  if (!data_file.partition_spec_id.has_value()) {
    return ValidationFailed("Cannot serialize REST content file without 'spec-id'");
  }
  json[kSpecId] = data_file.partition_spec_id.value();

  nlohmann::json partition_json = nlohmann::json::array();
  for (const auto& literal : data_file.partition.values()) {
    ICEBERG_ASSIGN_OR_RAISE(auto lit_json, iceberg::ToJson(literal));
    partition_json.push_back(std::move(lit_json));
  }
  json[kPartition] = std::move(partition_json);

  json[kRecordCount] = data_file.record_count;
  json[kFileSizeInBytes] = data_file.file_size_in_bytes;

  SetKeyValueMap(json, kColumnSizes, data_file.column_sizes);
  SetKeyValueMap(json, kValueCounts, data_file.value_counts);
  SetKeyValueMap(json, kNullValueCounts, data_file.null_value_counts);
  SetKeyValueMap(json, kNanValueCounts, data_file.nan_value_counts);
  SetKeyValueMap(json, kLowerBounds, data_file.lower_bounds);
  SetKeyValueMap(json, kUpperBounds, data_file.upper_bounds);

  if (!data_file.key_metadata.empty()) {
    json[kKeyMetadata] = data_file.key_metadata;
  }
  if (!data_file.split_offsets.empty()) {
    json[kSplitOffsets] = data_file.split_offsets;
  }
  if (!data_file.equality_ids.empty()) {
    json[kEqualityIds] = data_file.equality_ids;
  }
  if (data_file.sort_order_id.has_value()) {
    json[kSortOrderId] = data_file.sort_order_id.value();
  }
  if (data_file.first_row_id.has_value()) {
    json[kFirstRowId] = data_file.first_row_id.value();
  }
  if (data_file.referenced_data_file.has_value()) {
    json[kReferencedDataFile] = data_file.referenced_data_file.value();
  }
  if (data_file.content_offset.has_value()) {
    json[kContentOffset] = data_file.content_offset.value();
  }
  if (data_file.content_size_in_bytes.has_value()) {
    json[kContentSizeInBytes] = data_file.content_size_in_bytes.value();
  }

  return json;
}

Result<nlohmann::json> ToJson(
    const DataFile& data_file,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  if (!data_file.partition_spec_id.has_value()) {
    return ValidationFailed("Invalid partition spec id from content file: null");
  }
  auto it = partition_specs_by_id.find(data_file.partition_spec_id.value());
  if (it == partition_specs_by_id.end() || !it->second) {
    return ValidationFailed("Invalid partition spec: null");
  }
  if (data_file.partition_spec_id.value() != it->second->spec_id()) {
    return ValidationFailed(
        "Invalid partition spec id from content file: expected = {}, actual = {}",
        it->second->spec_id(), data_file.partition_spec_id.value());
  }
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, it->second->PartitionType(schema));
  if (data_file.partition.num_fields() != partition_type->fields().size()) {
    return ValidationFailed(
        "Invalid partition data from content file: expected = {}, actual = {}",
        partition_type->fields().empty() ? "unpartitioned" : "partitioned",
        data_file.partition.num_fields() == 0 ? "unpartitioned" : "partitioned");
  }
  return DataFileToJsonUnchecked(data_file);
}

namespace {

template <typename Response>
Result<nlohmann::json> ScanTaskFieldsToJson(
    const Response& response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  nlohmann::json json;

  if (response.plan_tasks.has_value()) {
    json[kPlanTasks] = *response.plan_tasks;
  }

  DeleteFileSet response_delete_files;
  for (const auto& file : response.delete_files) {
    response_delete_files.insert(file);
  }

  DeleteFileSet delete_files;
  auto add_delete_file = [&](const std::shared_ptr<DataFile>& task_file) {
    if (!task_file) {
      return;
    }
    auto [response_file, inserted_response_file] =
        response_delete_files.insert(task_file);
    delete_files.insert(inserted_response_file ? task_file : *response_file);
  };
  if (response.file_scan_tasks.has_value()) {
    for (const auto& task : *response.file_scan_tasks) {
      if (!task) continue;
      for (const auto& file : task->delete_files()) {
        add_delete_file(file);
      }
    }
  }

  nlohmann::json tasks_json = nlohmann::json::array();
  if (response.file_scan_tasks.has_value()) {
    for (const auto& task : *response.file_scan_tasks) {
      if (!task) continue;
      nlohmann::json task_json;
      if (task->data_file()) {
        ICEBERG_ASSIGN_OR_RAISE(
            auto data_file_json,
            ToJson(*task->data_file(), partition_specs_by_id, schema));
        task_json[kDataFile] = std::move(data_file_json);
      }
      if (!task->delete_files().empty()) {
        std::vector<int32_t> refs;
        for (const auto& delete_file : task->delete_files()) {
          if (delete_file) {
            auto [file, _] = delete_files.insert(delete_file);
            refs.push_back(
                static_cast<int32_t>(std::distance(delete_files.begin(), file)));
          }
        }
        if (!refs.empty()) {
          task_json[kDeleteFileReferences] = std::move(refs);
        }
      }
      if (task->residual_filter()) {
        ICEBERG_ASSIGN_OR_RAISE(auto residual_json,
                                iceberg::ToJson(*task->residual_filter()));
        task_json[kResidualFilter] = std::move(residual_json);
      }
      tasks_json.push_back(std::move(task_json));
    }
  }
  nlohmann::json delete_files_json = nlohmann::json::array();
  for (const auto& file : delete_files) {
    ICEBERG_ASSIGN_OR_RAISE(auto df_json, ToJson(*file, partition_specs_by_id, schema));
    delete_files_json.push_back(std::move(df_json));
  }
  if (!delete_files_json.empty()) {
    json[kDeleteFiles] = std::move(delete_files_json);
  }
  if (response.file_scan_tasks.has_value()) {
    json[kFileScanTasks] = std::move(tasks_json);
  }

  return json;
}

template <typename Response>
Status ScanTaskFieldsFromJson(
    const nlohmann::json& json, Response& response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  // 1. plan_tasks
  if (json.contains(kPlanTasks)) {
    ICEBERG_ASSIGN_OR_RAISE(response.plan_tasks,
                            GetJsonValue<std::vector<std::string>>(json, kPlanTasks));
  }

  // 2. delete_files
  nlohmann::json delete_files_json = nlohmann::json::array();
  if (json.contains(kDeleteFiles)) {
    ICEBERG_ASSIGN_OR_RAISE(delete_files_json,
                            GetJsonValue<nlohmann::json>(json, kDeleteFiles));
  }
  if (!delete_files_json.is_array()) {
    return JsonParseError("Cannot parse delete files from non-array: {}",
                          SafeDumpJson(delete_files_json));
  }
  for (const auto& entry_json : delete_files_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_file,
                            DataFileFromJson(entry_json, partition_specs_by_id, schema));
    response.delete_files.push_back(std::make_shared<DataFile>(std::move(delete_file)));
  }

  // 3. file_scan_tasks
  if (json.contains(kFileScanTasks)) {
    ICEBERG_ASSIGN_OR_RAISE(auto file_scan_tasks_json,
                            GetJsonValue<nlohmann::json>(json, kFileScanTasks));
    ICEBERG_ASSIGN_OR_RAISE(
        response.file_scan_tasks,
        FileScanTasksFromJson(file_scan_tasks_json, response.delete_files,
                              partition_specs_by_id, schema));
  }
  return {};
}

}  // namespace

nlohmann::json ToJson(const CatalogConfig& config) {
  nlohmann::json json;
  json[kOverrides] = config.overrides;
  json[kDefaults] = config.defaults;
  for (const auto& endpoint : config.endpoints) {
    json[kEndpoints].emplace_back(endpoint.ToString());
  }
  return json;
}

Result<CatalogConfig> CatalogConfigFromJson(const nlohmann::json& json) {
  CatalogConfig config;
  ICEBERG_ASSIGN_OR_RAISE(
      config.overrides,
      GetJsonValueOrDefault<decltype(config.overrides)>(json, kOverrides));
  ICEBERG_ASSIGN_OR_RAISE(
      config.defaults, GetJsonValueOrDefault<decltype(config.defaults)>(json, kDefaults));
  ICEBERG_ASSIGN_OR_RAISE(
      auto endpoints, GetJsonValueOrDefault<std::vector<std::string>>(json, kEndpoints));
  config.endpoints.reserve(endpoints.size());
  for (const auto& endpoint_str : endpoints) {
    auto endpoint_result = Endpoint::FromString(endpoint_str);
    if (!endpoint_result.has_value()) {
      // Convert to JsonParseError in JSON deserialization context
      return JsonParseError("{}", endpoint_result.error().message);
    }
    config.endpoints.emplace_back(std::move(endpoint_result.value()));
  }
  ICEBERG_RETURN_UNEXPECTED(config.Validate());
  return config;
}

nlohmann::json ToJson(const ErrorResponse& error) {
  nlohmann::json error_json;
  error_json[kMessage] = error.message;
  error_json[kType] = error.type;
  error_json[kCode] = error.code;
  SetContainerField(error_json, kStack, error.stack);

  nlohmann::json json;
  json[kError] = std::move(error_json);
  return json;
}

Result<ErrorResponse> ErrorResponseFromJson(const nlohmann::json& json) {
  ICEBERG_ASSIGN_OR_RAISE(auto error_json, GetJsonValue<nlohmann::json>(json, kError));

  ErrorResponse error;
  // NOTE: Iceberg's Java implementation allows missing required fields (message, type,
  // code) during deserialization, which deviates from the REST spec. We enforce strict
  // validation here.
  ICEBERG_ASSIGN_OR_RAISE(error.message, GetJsonValue<std::string>(error_json, kMessage));
  ICEBERG_ASSIGN_OR_RAISE(error.type, GetJsonValue<std::string>(error_json, kType));
  ICEBERG_ASSIGN_OR_RAISE(error.code, GetJsonValue<uint32_t>(error_json, kCode));
  ICEBERG_ASSIGN_OR_RAISE(
      error.stack, GetJsonValueOrDefault<std::vector<std::string>>(error_json, kStack));
  ICEBERG_RETURN_UNEXPECTED(error.Validate());
  return error;
}

nlohmann::json ToJson(const CreateNamespaceRequest& request) {
  nlohmann::json json;
  json[kNamespace] = request.namespace_.levels;
  SetContainerField(json, kProperties, request.properties);
  return json;
}

Result<CreateNamespaceRequest> CreateNamespaceRequestFromJson(
    const nlohmann::json& json) {
  CreateNamespaceRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      request.properties,
      GetJsonValueOrDefault<decltype(request.properties)>(json, kProperties));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

nlohmann::json ToJson(const UpdateNamespacePropertiesRequest& request) {
  nlohmann::json json = nlohmann::json::object();
  SetContainerField(json, kRemovals, request.removals);
  SetContainerField(json, kUpdates, request.updates);
  return json;
}

Result<UpdateNamespacePropertiesRequest> UpdateNamespacePropertiesRequestFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropertiesRequest request;
  ICEBERG_ASSIGN_OR_RAISE(
      request.removals, GetJsonValueOrDefault<std::vector<std::string>>(json, kRemovals));
  ICEBERG_ASSIGN_OR_RAISE(
      request.updates, GetJsonValueOrDefault<decltype(request.updates)>(json, kUpdates));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

nlohmann::json ToJson(const RegisterTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  json[kMetadataLocation] = request.metadata_location;
  if (request.overwrite) {
    json[kOverwrite] = request.overwrite;
  }
  return json;
}

Result<RegisterTableRequest> RegisterTableRequestFromJson(const nlohmann::json& json) {
  RegisterTableRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(request.metadata_location,
                          GetJsonValue<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(request.overwrite,
                          GetJsonValueOrDefault<bool>(json, kOverwrite, false));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

nlohmann::json ToJson(const RenameTableRequest& request) {
  nlohmann::json json;
  json[kSource] = ToJson(request.source);
  json[kDestination] = ToJson(request.destination);
  return json;
}

Result<RenameTableRequest> RenameTableRequestFromJson(const nlohmann::json& json) {
  RenameTableRequest request;
  ICEBERG_ASSIGN_OR_RAISE(auto source_json, GetJsonValue<nlohmann::json>(json, kSource));
  ICEBERG_ASSIGN_OR_RAISE(request.source, TableIdentifierFromJson(source_json));
  ICEBERG_ASSIGN_OR_RAISE(auto dest_json,
                          GetJsonValue<nlohmann::json>(json, kDestination));
  ICEBERG_ASSIGN_OR_RAISE(request.destination, TableIdentifierFromJson(dest_json));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

// LoadTableResult (used by CreateTableResponse, LoadTableResponse)
Result<nlohmann::json> ToJson(const LoadTableResult& result) {
  nlohmann::json json;
  SetOptionalStringField(json, kMetadataLocation, result.metadata_location);
  ICEBERG_ASSIGN_OR_RAISE(json[kMetadata], ToJson(*result.metadata));
  SetContainerField(json, kConfig, result.config);
  if (!result.storage_credentials.empty()) {
    nlohmann::json creds = nlohmann::json::array();
    for (const auto& cred : result.storage_credentials) {
      nlohmann::json entry;
      entry[kPrefix] = cred.prefix;
      // config is required, so always write it (matches Java).
      entry[kConfig] = cred.config;
      creds.push_back(std::move(entry));
    }
    json[kStorageCredentials] = std::move(creds);
  }
  return json;
}

Result<LoadTableResult> LoadTableResultFromJson(const nlohmann::json& json) {
  LoadTableResult result;
  ICEBERG_ASSIGN_OR_RAISE(result.metadata_location,
                          GetJsonValueOrDefault<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          GetJsonValue<nlohmann::json>(json, kMetadata));
  ICEBERG_ASSIGN_OR_RAISE(result.metadata, TableMetadataFromJson(metadata_json));
  ICEBERG_ASSIGN_OR_RAISE(result.config,
                          GetJsonValueOrDefault<decltype(result.config)>(json, kConfig));
  if (auto it = json.find(kStorageCredentials); it != json.end() && !it->is_null()) {
    if (!it->is_array()) {
      return JsonParseError("Cannot parse storage credentials from non-array: {}",
                            SafeDumpJson(*it));
    }
    for (const auto& entry : *it) {
      StorageCredential cred;
      ICEBERG_ASSIGN_OR_RAISE(cred.prefix, GetJsonValue<std::string>(entry, kPrefix));
      ICEBERG_ASSIGN_OR_RAISE(cred.config,
                              GetJsonValue<decltype(cred.config)>(entry, kConfig));
      // prefix and config are required by the REST spec; non-empty matches the
      // Java reference implementation (Credential.validate()).
      if (cred.prefix.empty()) {
        return JsonParseError("Invalid storage credential: prefix must be non-empty");
      }
      if (cred.config.empty()) {
        return JsonParseError("Invalid storage credential: config must be non-empty");
      }
      result.storage_credentials.push_back(std::move(cred));
    }
  }
  ICEBERG_RETURN_UNEXPECTED(result.Validate());
  return result;
}

nlohmann::json ToJson(const ListNamespacesResponse& response) {
  nlohmann::json json;
  SetOptionalStringField(json, kNextPageToken, response.next_page_token);
  nlohmann::json namespaces = nlohmann::json::array();
  for (const auto& ns : response.namespaces) {
    namespaces.push_back(ToJson(ns));
  }
  json[kNamespaces] = std::move(namespaces);
  return json;
}

Result<ListNamespacesResponse> ListNamespacesResponseFromJson(
    const nlohmann::json& json) {
  ListNamespacesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.next_page_token,
                          GetJsonValueOrDefault<std::string>(json, kNextPageToken));
  ICEBERG_ASSIGN_OR_RAISE(auto namespaces_json,
                          GetJsonValue<nlohmann::json>(json, kNamespaces));
  for (const auto& ns_json : namespaces_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto ns, NamespaceFromJson(ns_json));
    response.namespaces.push_back(std::move(ns));
  }
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const CreateNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespace] = response.namespace_.levels;
  SetContainerField(json, kProperties, response.properties);
  return json;
}

Result<CreateNamespaceResponse> CreateNamespaceResponseFromJson(
    const nlohmann::json& json) {
  CreateNamespaceResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      response.properties,
      GetJsonValueOrDefault<decltype(response.properties)>(json, kProperties));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const GetNamespaceResponse& response) {
  nlohmann::json json;
  json[kNamespace] = response.namespace_.levels;
  SetContainerField(json, kProperties, response.properties);
  return json;
}

Result<GetNamespaceResponse> GetNamespaceResponseFromJson(const nlohmann::json& json) {
  GetNamespaceResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.namespace_.levels,
                          GetJsonValue<std::vector<std::string>>(json, kNamespace));
  ICEBERG_ASSIGN_OR_RAISE(
      response.properties,
      GetJsonValueOrDefault<decltype(response.properties)>(json, kProperties));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const UpdateNamespacePropertiesResponse& response) {
  nlohmann::json json;
  json[kUpdated] = response.updated;
  json[kRemoved] = response.removed;
  SetContainerField(json, kMissing, response.missing);
  return json;
}

Result<UpdateNamespacePropertiesResponse> UpdateNamespacePropertiesResponseFromJson(
    const nlohmann::json& json) {
  UpdateNamespacePropertiesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(
      response.updated, GetJsonValueOrDefault<std::vector<std::string>>(json, kUpdated));
  ICEBERG_ASSIGN_OR_RAISE(
      response.removed, GetJsonValueOrDefault<std::vector<std::string>>(json, kRemoved));
  ICEBERG_ASSIGN_OR_RAISE(
      response.missing, GetJsonValueOrDefault<std::vector<std::string>>(json, kMissing));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const ListTablesResponse& response) {
  nlohmann::json json;
  SetOptionalStringField(json, kNextPageToken, response.next_page_token);
  nlohmann::json identifiers_json = nlohmann::json::array();
  for (const auto& identifier : response.identifiers) {
    identifiers_json.push_back(ToJson(identifier));
  }
  json[kIdentifiers] = identifiers_json;
  return json;
}

Result<ListTablesResponse> ListTablesResponseFromJson(const nlohmann::json& json) {
  ListTablesResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.next_page_token,
                          GetJsonValueOrDefault<std::string>(json, kNextPageToken));
  ICEBERG_ASSIGN_OR_RAISE(auto identifiers_json,
                          GetJsonValue<nlohmann::json>(json, kIdentifiers));
  for (const auto& id_json : identifiers_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto identifier, TableIdentifierFromJson(id_json));
    response.identifiers.push_back(std::move(identifier));
  }
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<nlohmann::json> ToJson(const CreateTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  SetOptionalStringField(json, kLocation, request.location);
  if (request.schema) {
    ICEBERG_ASSIGN_OR_RAISE(json[kSchema], ToJson(*request.schema));
  }
  if (request.partition_spec) {
    json[kPartitionSpec] = ToJson(*request.partition_spec);
  }
  if (request.write_order) {
    json[kWriteOrder] = ToJson(*request.write_order);
  }
  if (request.stage_create) {
    json[kStageCreate] = request.stage_create;
  }
  SetContainerField(json, kProperties, request.properties);
  return json;
}

Result<CreateTableRequest> CreateTableRequestFromJson(const nlohmann::json& json) {
  CreateTableRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.name, GetJsonValue<std::string>(json, kName));
  ICEBERG_ASSIGN_OR_RAISE(request.location,
                          GetJsonValueOrDefault<std::string>(json, kLocation));
  ICEBERG_ASSIGN_OR_RAISE(auto schema, GetJsonValue<nlohmann::json>(json, kSchema));
  ICEBERG_ASSIGN_OR_RAISE(request.schema, SchemaFromJson(schema));

  if (json.contains(kPartitionSpec)) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_spec,
                            GetJsonValue<nlohmann::json>(json, kPartitionSpec));
    ICEBERG_ASSIGN_OR_RAISE(request.partition_spec,
                            PartitionSpecFromJson(request.schema, partition_spec,
                                                  PartitionSpec::kInitialSpecId));
  }
  if (json.contains(kWriteOrder)) {
    ICEBERG_ASSIGN_OR_RAISE(auto sort_order,
                            GetJsonValue<nlohmann::json>(json, kWriteOrder));
    ICEBERG_ASSIGN_OR_RAISE(request.write_order,
                            SortOrderFromJson(sort_order, request.schema));
  }

  ICEBERG_ASSIGN_OR_RAISE(request.stage_create,
                          GetJsonValueOrDefault<bool>(json, kStageCreate, false));
  ICEBERG_ASSIGN_OR_RAISE(
      request.properties,
      GetJsonValueOrDefault<decltype(request.properties)>(json, kProperties));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

// CommitTableRequest serialization
Result<nlohmann::json> ToJson(const CommitTableRequest& request) {
  nlohmann::json json;
  if (!request.identifier.name.empty()) {
    json[kIdentifier] = ToJson(request.identifier);
  }

  nlohmann::json requirements_json = nlohmann::json::array();
  for (const auto& req : request.requirements) {
    requirements_json.push_back(ToJson(*req));
  }
  json[kRequirements] = std::move(requirements_json);

  nlohmann::json updates_json = nlohmann::json::array();
  for (const auto& update : request.updates) {
    ICEBERG_ASSIGN_OR_RAISE(auto update_json, ToJson(*update));
    updates_json.push_back(std::move(update_json));
  }
  json[kUpdates] = std::move(updates_json);

  return json;
}

Result<CommitTableRequest> CommitTableRequestFromJson(const nlohmann::json& json) {
  CommitTableRequest request;
  if (json.contains(kIdentifier)) {
    ICEBERG_ASSIGN_OR_RAISE(auto identifier_json,
                            GetJsonValue<nlohmann::json>(json, kIdentifier));
    ICEBERG_ASSIGN_OR_RAISE(request.identifier, TableIdentifierFromJson(identifier_json));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto requirements_json,
                          GetJsonValueOptional<nlohmann::json>(json, kRequirements));
  if (requirements_json.has_value()) {
    if (!requirements_json->is_array()) {
      return JsonParseError("Expected '{}' to be an array, got {}", kRequirements,
                            SafeDumpJson(*requirements_json));
    }
    for (const auto& req_json : *requirements_json) {
      ICEBERG_ASSIGN_OR_RAISE(auto requirement, TableRequirementFromJson(req_json));
      request.requirements.push_back(std::move(requirement));
    }
  }

  ICEBERG_ASSIGN_OR_RAISE(auto updates_json,
                          GetJsonValueOptional<nlohmann::json>(json, kUpdates));
  if (updates_json.has_value()) {
    if (!updates_json->is_array()) {
      return JsonParseError("Expected '{}' to be an array, got {}", kUpdates,
                            SafeDumpJson(*updates_json));
    }
    for (const auto& update_json : *updates_json) {
      ICEBERG_ASSIGN_OR_RAISE(auto update, TableUpdateFromJson(update_json));
      request.updates.push_back(std::move(update));
    }
  }

  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

// CommitTableResponse serialization
Result<nlohmann::json> ToJson(const CommitTableResponse& response) {
  nlohmann::json json;
  json[kMetadataLocation] = response.metadata_location;
  if (response.metadata) {
    ICEBERG_ASSIGN_OR_RAISE(json[kMetadata], ToJson(*response.metadata));
  }
  return json;
}

Result<CommitTableResponse> CommitTableResponseFromJson(const nlohmann::json& json) {
  CommitTableResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.metadata_location,
                          GetJsonValue<std::string>(json, kMetadataLocation));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata_json,
                          GetJsonValue<nlohmann::json>(json, kMetadata));
  ICEBERG_ASSIGN_OR_RAISE(response.metadata, TableMetadataFromJson(metadata_json));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const OAuthTokenResponse& response) {
  nlohmann::json json;
  json[kAccessToken] = response.access_token;
  json[kTokenType] = response.token_type;
  if (response.expires_in_secs.has_value()) {
    json[kExpiresIn] = response.expires_in_secs.value();
  }
  if (!response.issued_token_type.empty()) {
    json[kIssuedTokenType] = response.issued_token_type;
  }
  if (!response.scope.empty()) {
    json[kOAuthScope] = response.scope;
  }
  return json;
}

Result<OAuthTokenResponse> OAuthTokenResponseFromJson(const nlohmann::json& json) {
  OAuthTokenResponse response;
  ICEBERG_ASSIGN_OR_RAISE(response.access_token,
                          GetJsonValue<std::string>(json, kAccessToken));
  ICEBERG_ASSIGN_OR_RAISE(response.token_type,
                          GetJsonValue<std::string>(json, kTokenType));
  // TODO(lishuxu): When implementing auto-refresh, extract exp claim
  // from JWT if expires_in is missing.
  if (json.contains(std::string(kExpiresIn))) {
    ICEBERG_ASSIGN_OR_RAISE(auto val, GetJsonValue<int64_t>(json, kExpiresIn));
    response.expires_in_secs = val;
  }
  ICEBERG_ASSIGN_OR_RAISE(response.issued_token_type,
                          GetJsonValueOrDefault<std::string>(json, kIssuedTokenType));
  ICEBERG_ASSIGN_OR_RAISE(response.refresh_token,
                          GetJsonValueOrDefault<std::string>(json, kRefreshToken));
  ICEBERG_ASSIGN_OR_RAISE(response.scope,
                          GetJsonValueOrDefault<std::string>(json, kOAuthScope));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<PlanTableScanRequest> PlanTableScanRequestFromJson(const nlohmann::json& json) {
  PlanTableScanRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.snapshot_id,
                          GetJsonValueOptional<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(request.select,
                          GetJsonValueOrDefault<std::vector<std::string>>(json, kSelect));
  if (json.contains(kFilter)) {
    ICEBERG_ASSIGN_OR_RAISE(request.filter, ExpressionFromJson(json.at(kFilter)));
  }
  ICEBERG_ASSIGN_OR_RAISE(request.case_sensitive,
                          GetJsonValueOrDefault<bool>(json, kCaseSensitive, true));
  ICEBERG_ASSIGN_OR_RAISE(request.use_snapshot_schema,
                          GetJsonValueOrDefault<bool>(json, kUseSnapshotSchema, false));
  ICEBERG_ASSIGN_OR_RAISE(request.start_snapshot_id,
                          GetJsonValueOptional<int64_t>(json, kStartSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(request.end_snapshot_id,
                          GetJsonValueOptional<int64_t>(json, kEndSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(
      request.stats_fields,
      GetJsonValueOrDefault<std::vector<std::string>>(json, kStatsFields));
  ICEBERG_ASSIGN_OR_RAISE(request.min_rows_requested,
                          GetJsonValueOptional<int64_t>(json, kMinRowsRequested));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

Result<nlohmann::json> ToJson(const PlanTableScanRequest& request) {
  nlohmann::json json;
  if (request.snapshot_id.has_value()) {
    json[kSnapshotId] = request.snapshot_id.value();
  }
  if (!request.select.empty()) {
    json[kSelect] = request.select;
  }
  if (request.filter) {
    ICEBERG_ASSIGN_OR_RAISE(auto filter_json, iceberg::ToJson(*request.filter));
    json[kFilter] = std::move(filter_json);
  }
  json[kCaseSensitive] = request.case_sensitive;
  json[kUseSnapshotSchema] = request.use_snapshot_schema;
  if (request.start_snapshot_id.has_value()) {
    json[kStartSnapshotId] = request.start_snapshot_id.value();
  }
  if (request.end_snapshot_id.has_value()) {
    json[kEndSnapshotId] = request.end_snapshot_id.value();
  }
  if (!request.stats_fields.empty()) {
    json[kStatsFields] = request.stats_fields;
  }
  if (request.min_rows_requested.has_value()) {
    json[kMinRowsRequested] = request.min_rows_requested.value();
  }
  return json;
}

Result<FetchScanTasksRequest> FetchScanTasksRequestFromJson(const nlohmann::json& json) {
  FetchScanTasksRequest request;
  ICEBERG_ASSIGN_OR_RAISE(request.planTask, GetJsonValue<std::string>(json, kPlanTask));
  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

nlohmann::json ToJson(const FetchScanTasksRequest& request) {
  nlohmann::json json;
  json[kPlanTask] = request.planTask;
  return json;
}

Result<PlanTableScanResponse> PlanTableScanResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  PlanTableScanResponse response;
  ICEBERG_ASSIGN_OR_RAISE(auto plan_status_str,
                          GetJsonValue<std::string>(json, kPlanStatus));
  ICEBERG_ASSIGN_OR_RAISE(response.plan_status, PlanStatusFromString(plan_status_str));
  ICEBERG_ASSIGN_OR_RAISE(response.plan_id,
                          GetJsonValueOrDefault<std::string>(json, kPlanId));
  if (response.plan_status == PlanStatus::kFailed) {
    ICEBERG_ASSIGN_OR_RAISE(response.error, ErrorResponseFromJson(json));
  } else if (json.contains(kError)) {
    return ValidationFailed("error can only be present when status is 'failed'");
  }
  ICEBERG_RETURN_UNEXPECTED(
      ScanTaskFieldsFromJson(json, response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<nlohmann::json> ToJson(
    const PlanTableScanResponse& response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  ICEBERG_ASSIGN_OR_RAISE(nlohmann::json json,
                          ScanTaskFieldsToJson(response, partition_specs_by_id, schema));
  json[kPlanStatus] = ToString(response.plan_status);
  if (!response.plan_id.empty()) {
    json[kPlanId] = response.plan_id;
  }
  if (response.error.has_value()) {
    json[kError] = ToJson(*response.error)[kError];
  }
  return json;
}

Result<FetchPlanningResultResponse> FetchPlanningResultResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  FetchPlanningResultResponse response;
  ICEBERG_ASSIGN_OR_RAISE(auto plan_status_str,
                          GetJsonValue<std::string>(json, kPlanStatus));
  ICEBERG_ASSIGN_OR_RAISE(response.plan_status, PlanStatusFromString(plan_status_str));
  if (response.plan_status == PlanStatus::kFailed) {
    ICEBERG_ASSIGN_OR_RAISE(response.error, ErrorResponseFromJson(json));
  } else if (json.contains(kError)) {
    return ValidationFailed("error can only be present when status is 'failed'");
  }
  ICEBERG_RETURN_UNEXPECTED(
      ScanTaskFieldsFromJson(json, response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<nlohmann::json> ToJson(
    const FetchPlanningResultResponse& response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  ICEBERG_ASSIGN_OR_RAISE(nlohmann::json json,
                          ScanTaskFieldsToJson(response, partition_specs_by_id, schema));
  json[kPlanStatus] = ToString(response.plan_status);
  if (response.error.has_value()) {
    json[kError] = ToJson(*response.error)[kError];
  }
  return json;
}

Result<FetchScanTasksResponse> FetchScanTasksResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  FetchScanTasksResponse response;
  ICEBERG_RETURN_UNEXPECTED(
      ScanTaskFieldsFromJson(json, response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<nlohmann::json> ToJson(
    const FetchScanTasksResponse& response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  return ScanTaskFieldsToJson(response, partition_specs_by_id, schema);
}

#define ICEBERG_DEFINE_FROM_JSON(Model)                       \
  template <>                                                 \
  Result<Model> FromJson<Model>(const nlohmann::json& json) { \
    return Model##FromJson(json);                             \
  }

ICEBERG_DEFINE_FROM_JSON(CatalogConfig)
ICEBERG_DEFINE_FROM_JSON(ErrorResponse)
ICEBERG_DEFINE_FROM_JSON(ListNamespacesResponse)
ICEBERG_DEFINE_FROM_JSON(CreateNamespaceRequest)
ICEBERG_DEFINE_FROM_JSON(CreateNamespaceResponse)
ICEBERG_DEFINE_FROM_JSON(GetNamespaceResponse)
ICEBERG_DEFINE_FROM_JSON(UpdateNamespacePropertiesRequest)
ICEBERG_DEFINE_FROM_JSON(UpdateNamespacePropertiesResponse)
ICEBERG_DEFINE_FROM_JSON(ListTablesResponse)
ICEBERG_DEFINE_FROM_JSON(LoadTableResult)
ICEBERG_DEFINE_FROM_JSON(RegisterTableRequest)
ICEBERG_DEFINE_FROM_JSON(RenameTableRequest)
ICEBERG_DEFINE_FROM_JSON(CreateTableRequest)
ICEBERG_DEFINE_FROM_JSON(CommitTableRequest)
ICEBERG_DEFINE_FROM_JSON(CommitTableResponse)
ICEBERG_DEFINE_FROM_JSON(OAuthTokenResponse)
ICEBERG_DEFINE_FROM_JSON(PlanTableScanRequest)
ICEBERG_DEFINE_FROM_JSON(FetchScanTasksRequest)

}  // namespace iceberg::rest
