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

#include <nlohmann/json_fwd.hpp>

#include "iceberg/result.h"
#include "iceberg/statistics_file.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Serializes a `SortField` object to JSON.
///
/// This function converts a `SortField` object into a JSON representation.
/// The resulting JSON object includes the transform type, source ID, sort direction, and
/// null ordering.
///
/// \param sort_field The `SortField` object to be serialized.
/// \return A JSON object representing the `SortField` in the form of key-value pairs.
nlohmann::json ToJson(const SortField& sort_field);

/// \brief Deserializes a JSON object into a `SortField` object.
///
/// This function parses the provided JSON and creates a `SortField` object.
/// It expects the JSON object to contain keys for the transform, source ID, direction,
/// and null order.
///
/// \param json The JSON object representing a `SortField`.
/// \return An `expected` value containing either a `SortField` object or an error. If the
/// JSON is malformed or missing expected fields, an error will be returned.
Result<std::unique_ptr<SortField>> SortFieldFromJson(const nlohmann::json& json);

/// \brief Serializes a `SortOrder` object to JSON.
///
/// This function converts a `SortOrder` object into a JSON representation.
/// The resulting JSON includes the order ID and a list of `SortField` objects.
/// Each `SortField` is serialized as described in the `ToJson(SortField)` function.
///
/// \param sort_order The `SortOrder` object to be serialized.
/// \return A JSON object representing the `SortOrder` with its order ID and fields array.
nlohmann::json ToJson(const SortOrder& sort_order);

/// \brief Deserializes a JSON object into a `SortOrder` object.
///
/// This function parses the provided JSON and creates a `SortOrder` object.
/// It expects the JSON object to contain the order ID and a list of `SortField` objects.
/// Each `SortField` will be parsed using the `SortFieldFromJson` function.
///
/// \param json The JSON object representing a `SortOrder`.
/// \return An `expected` value containing either a `SortOrder` object or an error. If the
/// JSON is malformed or missing expected fields, an error will be returned.
Result<std::unique_ptr<SortOrder>> SortOrderFromJson(const nlohmann::json& json);

/// \brief Convert an Iceberg Schema to JSON.
///
/// \param[in] schema The Iceberg schema to convert.
/// \return The JSON representation of the schema.
nlohmann::json ToJson(const Schema& schema);

/// \brief Convert JSON to an Iceberg Schema.
///
/// \param[in] json The JSON representation of the schema.
/// \return The Iceberg schema or an error if the conversion fails.
Result<std::unique_ptr<Schema>> SchemaFromJson(const nlohmann::json& json);

/// \brief Convert an Iceberg Type to JSON.
///
/// \param[in] type The Iceberg type to convert.
/// \return The JSON representation of the type.
nlohmann::json ToJson(const Type& type);

/// \brief Convert JSON to an Iceberg Type.
///
/// \param[in] json The JSON representation of the type.
/// \return The Iceberg type or an error if the conversion fails.
Result<std::unique_ptr<Type>> TypeFromJson(const nlohmann::json& json);

/// \brief Convert an Iceberg SchemaField to JSON.
///
/// \param[in] field The Iceberg field to convert.
/// \return The JSON representation of the field.
nlohmann::json ToJson(const SchemaField& field);

/// \brief Convert JSON to an Iceberg SchemaField.
///
/// \param[in] json The JSON representation of the field.
/// \return The Iceberg field or an error if the conversion fails.
Result<std::unique_ptr<SchemaField>> FieldFromJson(const nlohmann::json& json);

/// \brief Serializes a `PartitionField` object to JSON.
///
/// This function converts a `PartitionField` object into a JSON representation.
/// The resulting JSON object includes the transform type, source ID, field ID, and
/// name.
///
/// \param partition_field The `PartitionField` object to be serialized.
/// \return A JSON object representing the `PartitionField` in the form of key-value
/// pairs.
nlohmann::json ToJson(const PartitionField& partition_field);

/// \brief Deserializes a JSON object into a `PartitionField` object.
///
/// This function parses the provided JSON and creates a `PartitionField` object.
/// It expects the JSON object to contain keys for the transform, source ID, field ID,
/// and name.
///
/// \param json The JSON object representing a `PartitionField`.
/// \param allow_field_id_missing Whether the field ID is allowed to be missing. This can
/// happen when deserializing partition fields from V1 metadata files.
/// \return An `expected` value containing either a `PartitionField` object or an error.
/// If the JSON is malformed or missing expected fields, an error will be returned.
Result<std::unique_ptr<PartitionField>> PartitionFieldFromJson(
    const nlohmann::json& json, bool allow_field_id_missing = false);

/// \brief Serializes a `PartitionSpec` object to JSON.
///
/// This function converts a `PartitionSpec` object into a JSON representation.
/// The resulting JSON includes the spec ID and a list of `PartitionField` objects.
/// Each `PartitionField` is serialized as described in the `ToJson(PartitionField)`
/// function.
///
/// \param partition_spec The `PartitionSpec` object to be serialized.
/// \return A JSON object representing the `PartitionSpec` with its order ID and fields
/// array.
nlohmann::json ToJson(const PartitionSpec& partition_spec);

/// \brief Deserializes a JSON object into a `PartitionSpec` object.
///
/// This function parses the provided JSON and creates a `PartitionSpec` object.
/// It expects the JSON object to contain the spec ID and a list of `PartitionField`
/// objects. Each `PartitionField` will be parsed using the `PartitionFieldFromJson`
/// function.
///
/// \param json The JSON object representing a `PartitionSpec`.
/// \return An `expected` value containing either a `PartitionSpec` object or an error. If
/// the JSON is malformed or missing expected fields, an error will be returned.
Result<std::unique_ptr<PartitionSpec>> PartitionSpecFromJson(
    const std::shared_ptr<Schema>& schema, const nlohmann::json& json);

/// \brief Serializes a `SnapshotRef` object to JSON.
///
/// \param[in] snapshot_ref The `SnapshotRef` object to be serialized.
/// \return A JSON object representing the `SnapshotRef`.
nlohmann::json ToJson(const SnapshotRef& snapshot_ref);

/// \brief Deserializes a JSON object into a `SnapshotRef` object.
///
/// \param[in] json The JSON object representing a `SnapshotRef`.
/// \return A `SnapshotRef` object or an error if the conversion fails.
Result<std::unique_ptr<SnapshotRef>> SnapshotRefFromJson(const nlohmann::json& json);

/// \brief Serializes a `Snapshot` object to JSON.
///
/// \param[in] snapshot The `Snapshot` object to be serialized.
/// \return A JSON object representing the `snapshot`.
nlohmann::json ToJson(const Snapshot& snapshot);

/// \brief Deserializes a JSON object into a `Snapshot` object.
///
/// \param[in] json The JSON representation of the snapshot.
/// \return A `Snapshot` object or an error if the conversion fails.
Result<std::unique_ptr<Snapshot>> SnapshotFromJson(const nlohmann::json& json);

/// \brief Serializes a `StatisticsFile` object to JSON.
///
/// \param statistics_file The `StatisticsFile` object to be serialized.
/// \return A JSON object representing the `StatisticsFile`.
nlohmann::json ToJson(const StatisticsFile& statistics_file);

/// \brief Deserializes a JSON object into a `StatisticsFile` object.
///
/// \param json The JSON object representing a `StatisticsFile`.
/// \return A `StatisticsFile` object or an error if the conversion fails.
Result<std::unique_ptr<StatisticsFile>> StatisticsFileFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `PartitionStatisticsFile` object to JSON.
///
/// \param partition_statistics_file The `PartitionStatisticsFile` object to be
/// serialized. \return A JSON object representing the `PartitionStatisticsFile`.
nlohmann::json ToJson(const PartitionStatisticsFile& partition_statistics_file);

/// \brief Deserializes a JSON object into a `PartitionStatisticsFile` object.
///
/// \param json The JSON object representing a `PartitionStatisticsFile`.
/// \return A `PartitionStatisticsFile` object or an error if the conversion fails.
Result<std::unique_ptr<PartitionStatisticsFile>> PartitionStatisticsFileFromJson(
    const nlohmann::json& json);

/// \brief Serializes a `SnapshotLogEntry` object to JSON.
///
/// \param snapshot_log_entry The `SnapshotLogEntry` object to be serialized.
/// \return A JSON object representing the `SnapshotLogEntry`.
nlohmann::json ToJson(const SnapshotLogEntry& snapshot_log_entry);

/// \brief Deserializes a JSON object into a `SnapshotLogEntry` object.
///
/// \param json The JSON object representing a `SnapshotLogEntry`.
/// \return A `SnapshotLogEntry` object or an error if the conversion fails.
Result<SnapshotLogEntry> SnapshotLogEntryFromJson(const nlohmann::json& json);

/// \brief Serializes a `MetadataLogEntry` object to JSON.
///
/// \param metadata_log_entry The `MetadataLogEntry` object to be serialized.
/// \return A JSON object representing the `MetadataLogEntry`.
nlohmann::json ToJson(const MetadataLogEntry& metadata_log_entry);

/// \brief Deserializes a JSON object into a `MetadataLogEntry` object.
///
/// \param json The JSON object representing a `MetadataLogEntry`.
/// \return A `MetadataLogEntry` object or an error if the conversion fails.
Result<MetadataLogEntry> MetadataLogEntryFromJson(const nlohmann::json& json);

/// \brief Serializes a `TableMetadata` object to JSON.
///
/// \param table_metadata The `TableMetadata` object to be serialized.
/// \return A JSON object representing the `TableMetadata`.
nlohmann::json ToJson(const TableMetadata& table_metadata);

/// \brief Deserializes a JSON object into a `TableMetadata` object.
///
/// \param json The JSON object representing a `TableMetadata`.
/// \return A `TableMetadata` object or an error if the conversion fails.
Result<std::unique_ptr<TableMetadata>> TableMetadataFromJson(const nlohmann::json& json);

/// \brief Deserialize a JSON string into a `nlohmann::json` object.
///
/// \param json_string The JSON string to deserialize.
/// \return A `nlohmann::json` object or an error if the deserialization fails.
Result<nlohmann::json> FromJsonString(const std::string& json_string);

/// \brief Serialize a `nlohmann::json` object into a JSON string.
///
/// \param json The `nlohmann::json` object to serialize.
/// \return A JSON string or an error if the serialization fails.
Result<std::string> ToJsonString(const nlohmann::json& json);

/// \brief Serializes a `MappedField` object to JSON.
///
/// \param[in] field The `MappedField` object to be serialized.
/// \return A JSON object representing the `MappedField`.
nlohmann::json ToJson(const MappedField& field);

/// \brief Deserializes a JSON object into a `MappedField` object.
///
/// \param[in] json The JSON object representing a `MappedField`.
/// \return A `MappedField` object or an error if the conversion fails.
Result<MappedField> MappedFieldFromJson(const nlohmann::json& json);

/// \brief Serializes a `MappedFields` object to JSON.
///
/// \param[in] mapped_fields The `MappedFields` object to be serialized.
/// \return A JSON object representing the `MappedFields`.
nlohmann::json ToJson(const MappedFields& mapped_fields);

/// \brief Deserializes a JSON object into a `MappedFields` object.
///
/// \param[in] json The JSON object representing a `MappedFields`.
/// \return A `MappedFields` object or an error if the conversion fails.
Result<std::unique_ptr<MappedFields>> MappedFieldsFromJson(const nlohmann::json& json);

/// \brief Serializes a `NameMapping` object to JSON.
///
/// \param[in] name_mapping The `NameMapping` object to be serialized.
/// \return A JSON object representing the `NameMapping`.
nlohmann::json ToJson(const NameMapping& name_mapping);

/// \brief Deserializes a JSON object into a `NameMapping` object.
///
/// \param[in] json The JSON object representing a `NameMapping`.
/// \return A `NameMapping` object or an error if the conversion fails.
Result<std::unique_ptr<NameMapping>> NameMappingFromJson(const nlohmann::json& json);

}  // namespace iceberg
