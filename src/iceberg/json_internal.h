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

/// \brief Serializes a `SortOrder` object to JSON.
///
/// This function converts a `SortOrder` object into a JSON representation.
/// The resulting JSON includes the order ID and a list of `SortField` objects.
/// Each `SortField` is serialized as described in the `ToJson(SortField)` function.
///
/// \param sort_order The `SortOrder` object to be serialized.
/// \return A JSON object representing the `SortOrder` with its order ID and fields array.
nlohmann::json ToJson(const SortOrder& sort_order);

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
nlohmann::json SchemaToJson(const Schema& schema);

/// \brief Convert an Iceberg Type to JSON.
///
/// \param[in] type The Iceberg type to convert.
/// \return The JSON representation of the type.
nlohmann::json TypeToJson(const Type& type);

/// \brief Convert an Iceberg SchemaField to JSON.
///
/// \param[in] field The Iceberg field to convert.
/// \return The JSON representation of the field.
nlohmann::json FieldToJson(const SchemaField& field);

/// \brief Serializes a `SnapshotRef` object to JSON.
///
/// \param[in] snapshot_ref The `SnapshotRef` object to be serialized.
/// \return A JSON object representing the `SnapshotRef`.
nlohmann::json ToJson(const SnapshotRef& snapshot_ref);

/// \brief Serializes a `Snapshot` object to JSON.
///
/// \param[in] snapshot The `Snapshot` object to be serialized.
/// \return A JSON object representing the `snapshot`.
nlohmann::json ToJson(const Snapshot& snapshot);

/// \brief Convert JSON to an Iceberg Schema.
///
/// \param[in] json The JSON representation of the schema.
/// \return The Iceberg schema or an error if the conversion fails.
Result<std::unique_ptr<Schema>> SchemaFromJson(const nlohmann::json& json);

/// \brief Convert JSON to an Iceberg Type.
///
/// \param[in] json The JSON representation of the type.
/// \return The Iceberg type or an error if the conversion fails.
Result<std::unique_ptr<Type>> TypeFromJson(const nlohmann::json& json);

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

/// \brief Deserializes a JSON object into a `PartitionField` object.
///
/// This function parses the provided JSON and creates a `PartitionField` object.
/// It expects the JSON object to contain keys for the transform, source ID, field ID,
/// and name.
///
/// \param json The JSON object representing a `PartitionField`.
/// \return An `expected` value containing either a `PartitionField` object or an error.
/// If the JSON is malformed or missing expected fields, an error will be returned.
Result<std::unique_ptr<PartitionField>> PartitionFieldFromJson(
    const nlohmann::json& json);

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

/// \brief Deserializes a JSON object into a `SnapshotRef` object.
///
/// \param[in] json The JSON object representing a `SnapshotRef`.
/// \return A `SnapshotRef` object or an error if the conversion fails.
Result<std::unique_ptr<SnapshotRef>> SnapshotRefFromJson(const nlohmann::json& json);

/// \brief Deserializes a JSON object into a `Snapshot` object.
///
/// \param[in] json The JSON representation of the snapshot.
/// \return A `Snapshot` object or an error if the conversion fails.
Result<std::unique_ptr<Snapshot>> SnapshotFromJson(const nlohmann::json& json);

}  // namespace iceberg
