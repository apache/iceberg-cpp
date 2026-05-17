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

#include <map>
#include <memory>
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
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"
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

}  // namespace

Result<DataFile> DataFileFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& partition_spec_by_id,
    const Schema& schema) {
  if (!json.is_object()) {
    return JsonParseError("DataFile must be a JSON object: {}", SafeDumpJson(json));
  }
  DataFile df;

  ICEBERG_ASSIGN_OR_RAISE(auto content_str, GetJsonValue<std::string>(json, kContent));
  if (content_str == ToString(DataFile::Content::kData)) {
    df.content = DataFile::Content::kData;
  } else if (content_str == ToString(DataFile::Content::kPositionDeletes)) {
    df.content = DataFile::Content::kPositionDeletes;
  } else if (content_str == ToString(DataFile::Content::kEqualityDeletes)) {
    df.content = DataFile::Content::kEqualityDeletes;
  } else {
    return JsonParseError("Unknown data file content: {}", content_str);
  }

  ICEBERG_ASSIGN_OR_RAISE(df.file_path, GetJsonValue<std::string>(json, kFilePath));
  ICEBERG_ASSIGN_OR_RAISE(auto format_str, GetJsonValue<std::string>(json, kFileFormat));
  ICEBERG_ASSIGN_OR_RAISE(df.file_format, FileFormatTypeFromString(format_str));

  if (json.contains(kSpecId) && !json.at(kSpecId).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(auto spec_id, GetJsonValue<int32_t>(json, kSpecId));
    df.partition_spec_id = spec_id;
  }

  if (json.contains(kPartition)) {
    ICEBERG_ASSIGN_OR_RAISE(auto partition_vals,
                            GetJsonValue<nlohmann::json>(json, kPartition));
    if (!partition_vals.is_array()) {
      return JsonParseError("PartitionValues must be a JSON array: {}",
                            SafeDumpJson(partition_vals));
    }
    std::vector<Literal> literals;
    auto it = partition_spec_by_id.find(df.partition_spec_id.value_or(-1));
    if (it == partition_spec_by_id.end()) {
      return JsonParseError("Invalid partition spec id: {}",
                            df.partition_spec_id.value_or(-1));
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
    df.partition = PartitionValues(std::move(literals));
  }

  ICEBERG_ASSIGN_OR_RAISE(df.record_count, GetJsonValue<int64_t>(json, kRecordCount));
  ICEBERG_ASSIGN_OR_RAISE(df.file_size_in_bytes,
                          GetJsonValue<int64_t>(json, kFileSizeInBytes));

  auto parse_int_map = [&](std::string_view key,
                           std::map<int32_t, int64_t>& target) -> Status {
    if (!json.contains(key) || json.at(key).is_null()) {
      return {};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto map_json, GetJsonValue<nlohmann::json>(json, key));
    ICEBERG_ASSIGN_OR_RAISE(auto keys,
                            GetTypedJsonValue<std::vector<int32_t>>(map_json.at("keys")));
    ICEBERG_ASSIGN_OR_RAISE(
        auto values, GetTypedJsonValue<std::vector<int64_t>>(map_json.at("values")));
    if (keys.size() != values.size()) {
      return JsonParseError("'{}' map keys and values have different lengths", key);
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      target[keys[i]] = values[i];
    }
    return {};
  };

  ICEBERG_RETURN_UNEXPECTED(parse_int_map(kColumnSizes, df.column_sizes));
  ICEBERG_RETURN_UNEXPECTED(parse_int_map(kValueCounts, df.value_counts));
  ICEBERG_RETURN_UNEXPECTED(parse_int_map(kNullValueCounts, df.null_value_counts));
  ICEBERG_RETURN_UNEXPECTED(parse_int_map(kNanValueCounts, df.nan_value_counts));

  auto parse_binary_map = [&](std::string_view key,
                              std::map<int32_t, std::vector<uint8_t>>& target) -> Status {
    if (!json.contains(key) || json.at(key).is_null()) {
      return {};
    }
    ICEBERG_ASSIGN_OR_RAISE(auto map_json, GetJsonValue<nlohmann::json>(json, key));
    ICEBERG_ASSIGN_OR_RAISE(auto keys,
                            GetJsonValue<std::vector<int32_t>>(map_json, "keys"));
    ICEBERG_ASSIGN_OR_RAISE(
        auto values, GetJsonValue<std::vector<std::vector<uint8_t>>>(map_json, "values"));
    if (keys.size() != values.size()) {
      return JsonParseError("'{}' binary map keys and values have different lengths", key);
    }
    for (size_t i = 0; i < keys.size(); ++i) {
      target[keys[i]] = values[i];
    }
    return {};
  };

  ICEBERG_RETURN_UNEXPECTED(parse_binary_map(kLowerBounds, df.lower_bounds));
  ICEBERG_RETURN_UNEXPECTED(parse_binary_map(kUpperBounds, df.upper_bounds));

  if (json.contains(kKeyMetadata) && !json.at(kKeyMetadata).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.key_metadata,
                            GetJsonValue<std::vector<uint8_t>>(json, kKeyMetadata));
  }
  if (json.contains(kSplitOffsets) && !json.at(kSplitOffsets).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.split_offsets,
                            GetJsonValue<std::vector<int64_t>>(json, kSplitOffsets));
  }
  if (json.contains(kEqualityIds) && !json.at(kEqualityIds).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.equality_ids,
                            GetJsonValue<std::vector<int32_t>>(json, kEqualityIds));
  }
  if (json.contains(kSortOrderId) && !json.at(kSortOrderId).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.sort_order_id, GetJsonValue<int32_t>(json, kSortOrderId));
  }
  if (json.contains(kFirstRowId) && !json.at(kFirstRowId).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.first_row_id, GetJsonValue<int64_t>(json, kFirstRowId));
  }
  if (json.contains(kReferencedDataFile) && !json.at(kReferencedDataFile).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.referenced_data_file,
                            GetJsonValue<std::string>(json, kReferencedDataFile));
  }
  if (json.contains(kContentOffset) && !json.at(kContentOffset).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.content_offset,
                            GetJsonValue<int64_t>(json, kContentOffset));
  }
  if (json.contains(kContentSizeInBytes) && !json.at(kContentSizeInBytes).is_null()) {
    ICEBERG_ASSIGN_OR_RAISE(df.content_size_in_bytes,
                            GetJsonValue<int64_t>(json, kContentSizeInBytes));
  }

  return df;
}

Result<std::vector<std::shared_ptr<FileScanTask>>> FileScanTasksFromJson(
    const nlohmann::json& json,
    const std::vector<std::shared_ptr<DataFile>>& delete_files,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>& partition_spec_by_id,
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
    ICEBERG_ASSIGN_OR_RAISE(auto data_file,
                            DataFileFromJson(data_file_json, partition_spec_by_id, schema));

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

    file_scan_tasks.push_back(
        std::make_shared<FileScanTask>(std::make_shared<DataFile>(std::move(data_file)),
                                      std::move(task_delete_files),
                                      std::move(residual_filter)));
  }
  return file_scan_tasks;
}

nlohmann::json ToJson(const DataFile& df) {
  nlohmann::json json;
  json[kContent] = ToString(df.content);
  json[kFilePath] = df.file_path;
  json[kFileFormat] = ToString(df.file_format);

  if (df.partition_spec_id.has_value()) {
    json[kSpecId] = df.partition_spec_id.value();
  }

  json[kRecordCount] = df.record_count;
  json[kFileSizeInBytes] = df.file_size_in_bytes;

  auto write_int_map = [&](std::string_view key,
                           const std::map<int32_t, int64_t>& m) {
    if (!m.empty()) {
      std::vector<int32_t> keys;
      std::vector<int64_t> values;
      for (const auto& [k, v] : m) {
        keys.push_back(k);
        values.push_back(v);
      }
      json[key] = {{"keys", std::move(keys)}, {"values", std::move(values)}};
    }
  };

  write_int_map(kColumnSizes, df.column_sizes);
  write_int_map(kValueCounts, df.value_counts);
  write_int_map(kNullValueCounts, df.null_value_counts);
  write_int_map(kNanValueCounts, df.nan_value_counts);

  auto write_binary_map = [&](std::string_view key,
                              const std::map<int32_t, std::vector<uint8_t>>& m) {
    if (!m.empty()) {
      std::vector<int32_t> keys;
      std::vector<std::vector<uint8_t>> values;
      for (const auto& [k, v] : m) {
        keys.push_back(k);
        values.push_back(v);
      }
      json[key] = {{"keys", std::move(keys)}, {"values", std::move(values)}};
    }
  };

  write_binary_map(kLowerBounds, df.lower_bounds);
  write_binary_map(kUpperBounds, df.upper_bounds);

  if (!df.key_metadata.empty()) {
    json[kKeyMetadata] = df.key_metadata;
  }
  if (!df.split_offsets.empty()) {
    json[kSplitOffsets] = df.split_offsets;
  }
  if (!df.equality_ids.empty()) {
    json[kEqualityIds] = df.equality_ids;
  }
  if (df.sort_order_id.has_value()) {
    json[kSortOrderId] = df.sort_order_id.value();
  }
  if (df.first_row_id.has_value()) {
    json[kFirstRowId] = df.first_row_id.value();
  }
  if (df.referenced_data_file.has_value()) {
    json[kReferencedDataFile] = df.referenced_data_file.value();
  }
  if (df.content_offset.has_value()) {
    json[kContentOffset] = df.content_offset.value();
  }
  if (df.content_size_in_bytes.has_value()) {
    json[kContentSizeInBytes] = df.content_size_in_bytes.value();
  }

  return json;
}

namespace {

nlohmann::json BaseScanTaskResponseToJson(const BaseScanTaskResponse& response) {
  nlohmann::json json;

  SetContainerField(json, kPlanTasks, response.plan_tasks);

  // Build delete_files array and a pointer-to-index map for reference lookup.
  std::unordered_map<const DataFile*, int32_t> delete_file_index;
  nlohmann::json delete_files_json = nlohmann::json::array();
  for (size_t i = 0; i < response.delete_files.size(); ++i) {
    if (response.delete_files[i]) {
      delete_files_json.push_back(ToJson(*response.delete_files[i]));
      delete_file_index[response.delete_files[i].get()] = static_cast<int32_t>(i);
    }
  }
  if (!delete_files_json.empty()) {
    json[kDeleteFiles] = std::move(delete_files_json);
  }

  nlohmann::json tasks_json = nlohmann::json::array();
  for (const auto& task : response.file_scan_tasks) {
    if (!task) continue;
    nlohmann::json task_json;
    if (task->data_file()) {
      task_json[kDataFile] = ToJson(*task->data_file());
    }
    if (!task->delete_files().empty()) {
      std::vector<int32_t> refs;
      for (const auto& df : task->delete_files()) {
        auto it = delete_file_index.find(df.get());
        if (it != delete_file_index.end()) {
          refs.push_back(it->second);
        }
      }
      if (!refs.empty()) {
        task_json[kDeleteFileReferences] = std::move(refs);
      }
    }
    tasks_json.push_back(std::move(task_json));
  }
  if (!tasks_json.empty()) {
    json[kFileScanTasks] = std::move(tasks_json);
  }

  return json;
}

Status BaseScanTaskResponseFromJson(
    const nlohmann::json& json, BaseScanTaskResponse* response,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  // 1. plan_tasks
  ICEBERG_ASSIGN_OR_RAISE(
      response->plan_tasks,
      GetJsonValueOrDefault<std::vector<std::string>>(json, kPlanTasks));

  // 2. delete_files
  ICEBERG_ASSIGN_OR_RAISE(
      auto delete_files_json,
      GetJsonValueOrDefault<nlohmann::json>(json, kDeleteFiles, nlohmann::json::array()));
  for (const auto& entry_json : delete_files_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto delete_file,
                            DataFileFromJson(entry_json, partition_specs_by_id, schema));
    response->delete_files.push_back(std::make_shared<DataFile>(std::move(delete_file)));
  }

  // 3. file_scan_tasks
  ICEBERG_ASSIGN_OR_RAISE(auto file_scan_tasks_json,
                          GetJsonValueOrDefault<nlohmann::json>(json, kFileScanTasks,
                                                                nlohmann::json::array()));
  ICEBERG_ASSIGN_OR_RAISE(
      response->file_scan_tasks,
      FileScanTasksFromJson(file_scan_tasks_json, response->delete_files,
                            partition_specs_by_id, schema));
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
nlohmann::json ToJson(const LoadTableResult& result) {
  nlohmann::json json;
  SetOptionalStringField(json, kMetadataLocation, result.metadata_location);
  json[kMetadata] = ToJson(*result.metadata);
  SetContainerField(json, kConfig, result.config);
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

nlohmann::json ToJson(const CreateTableRequest& request) {
  nlohmann::json json;
  json[kName] = request.name;
  SetOptionalStringField(json, kLocation, request.location);
  if (request.schema) {
    json[kSchema] = ToJson(*request.schema);
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
nlohmann::json ToJson(const CommitTableRequest& request) {
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
    updates_json.push_back(ToJson(*update));
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
                          GetJsonValue<nlohmann::json>(json, kRequirements));
  for (const auto& req_json : requirements_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto requirement, TableRequirementFromJson(req_json));
    request.requirements.push_back(std::move(requirement));
  }

  ICEBERG_ASSIGN_OR_RAISE(auto updates_json,
                          GetJsonValue<nlohmann::json>(json, kUpdates));
  for (const auto& update_json : updates_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto update, TableUpdateFromJson(update_json));
    request.updates.push_back(std::move(update));
  }

  ICEBERG_RETURN_UNEXPECTED(request.Validate());
  return request;
}

// CommitTableResponse serialization
nlohmann::json ToJson(const CommitTableResponse& response) {
  nlohmann::json json;
  json[kMetadataLocation] = response.metadata_location;
  if (response.metadata) {
    json[kMetadata] = ToJson(*response.metadata);
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
  ICEBERG_RETURN_UNEXPECTED(
      BaseScanTaskResponseFromJson(json, &response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
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
  ICEBERG_RETURN_UNEXPECTED(
      BaseScanTaskResponseFromJson(json, &response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

Result<FetchScanTasksResponse> FetchScanTasksResponseFromJson(
    const nlohmann::json& json,
    const std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>>&
        partition_specs_by_id,
    const Schema& schema) {
  FetchScanTasksResponse response;
  ICEBERG_RETURN_UNEXPECTED(
      BaseScanTaskResponseFromJson(json, &response, partition_specs_by_id, schema));
  ICEBERG_RETURN_UNEXPECTED(response.Validate());
  return response;
}

nlohmann::json ToJson(const PlanTableScanResponse& response) {
  nlohmann::json json = BaseScanTaskResponseToJson(response);
  json[kPlanStatus] = ToString(response.plan_status);
  if (!response.plan_id.empty()) {
    json[kPlanId] = response.plan_id;
  }
  return json;
}

nlohmann::json ToJson(const FetchPlanningResultResponse& response) {
  nlohmann::json json = BaseScanTaskResponseToJson(response);
  json[kPlanStatus] = ToString(response.plan_status);
  return json;
}

nlohmann::json ToJson(const FetchScanTasksResponse& response) {
  return BaseScanTaskResponseToJson(response);
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

}  // namespace iceberg::rest
