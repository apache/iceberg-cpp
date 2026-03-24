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

#include <nlohmann/json.hpp>

#include "iceberg/puffin/file_metadata.h"
#include "iceberg/puffin/json_serde_internal.h"
#include "iceberg/util/json_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::puffin {

namespace {
constexpr std::string_view kBlobs = "blobs";
constexpr std::string_view kProperties = "properties";
constexpr std::string_view kType = "type";
constexpr std::string_view kFields = "fields";
constexpr std::string_view kSnapshotId = "snapshot-id";
constexpr std::string_view kSequenceNumber = "sequence-number";
constexpr std::string_view kOffset = "offset";
constexpr std::string_view kLength = "length";
constexpr std::string_view kCompressionCodec = "compression-codec";
}  // namespace

nlohmann::json ToJson(const BlobMetadata& blob_metadata) {
  nlohmann::json json;
  json[kType] = blob_metadata.type;
  json[kFields] = blob_metadata.input_fields;
  json[kSnapshotId] = blob_metadata.snapshot_id;
  json[kSequenceNumber] = blob_metadata.sequence_number;
  json[kOffset] = blob_metadata.offset;
  json[kLength] = blob_metadata.length;

  SetOptionalStringField(json, kCompressionCodec, blob_metadata.compression_codec);
  SetContainerField(json, kProperties, blob_metadata.properties);

  return json;
}

Result<BlobMetadata> BlobMetadataFromJson(const nlohmann::json& json) {
  BlobMetadata blob_metadata;

  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.type, GetJsonValue<std::string>(json, kType));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.input_fields,
                          GetJsonValue<std::vector<int32_t>>(json, kFields));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.snapshot_id,
                          GetJsonValue<int64_t>(json, kSnapshotId));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.sequence_number,
                          GetJsonValue<int64_t>(json, kSequenceNumber));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.offset, GetJsonValue<int64_t>(json, kOffset));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.length, GetJsonValue<int64_t>(json, kLength));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.compression_codec,
                          GetJsonValueOrDefault<std::string>(json, kCompressionCodec));
  ICEBERG_ASSIGN_OR_RAISE(blob_metadata.properties,
                          FromJsonMap<std::string>(json, kProperties));

  return blob_metadata;
}

nlohmann::json ToJson(const FileMetadata& file_metadata) {
  nlohmann::json json;

  nlohmann::json blobs_json = nlohmann::json::array();
  for (const auto& blob : file_metadata.blobs) {
    blobs_json.push_back(ToJson(blob));
  }
  json[kBlobs] = std::move(blobs_json);

  SetContainerField(json, kProperties, file_metadata.properties);

  return json;
}

Result<FileMetadata> FileMetadataFromJson(const nlohmann::json& json) {
  FileMetadata file_metadata;

  ICEBERG_ASSIGN_OR_RAISE(auto blobs_json, GetJsonValue<nlohmann::json>(json, kBlobs));
  if (!blobs_json.is_array()) {
    return JsonParseError("Cannot parse blobs from non-array: {}",
                          SafeDumpJson(blobs_json));
  }

  for (const auto& blob_json : blobs_json) {
    ICEBERG_ASSIGN_OR_RAISE(auto blob, BlobMetadataFromJson(blob_json));
    file_metadata.blobs.push_back(std::move(blob));
  }

  ICEBERG_ASSIGN_OR_RAISE(file_metadata.properties,
                          FromJsonMap<std::string>(json, kProperties));

  return file_metadata;
}

std::string ToJsonString(const FileMetadata& file_metadata, bool pretty) {
  auto json = ToJson(file_metadata);
  return pretty ? json.dump(2) : json.dump();
}

Result<FileMetadata> FileMetadataFromJsonString(std::string_view json_string) {
  if (json_string.empty()) {
    return JsonParseError("Cannot parse empty JSON string");
  }
  try {
    auto json = nlohmann::json::parse(json_string);
    return FileMetadataFromJson(json);
  } catch (const nlohmann::json::parse_error& e) {
    return JsonParseError("Failed to parse JSON: {}", e.what());
  }
}

}  // namespace iceberg::puffin
