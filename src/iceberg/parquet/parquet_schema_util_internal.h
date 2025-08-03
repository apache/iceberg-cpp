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

#include <parquet/arrow/schema.h>

#include "iceberg/schema.h"
#include "iceberg/schema_util.h"

namespace iceberg::parquet {

/// \brief Project an Iceberg Schema onto a Parquet Schema.
///
/// This function creates a projection from an Iceberg Schema to a Parquet schema.
/// The projection determines how to read data from the Parquet schema into the expected
/// Iceberg Schema.
///
/// \param expected_schema The Iceberg Schema that defines the expected structure.
/// \param parquet_schema The Parquet schema to read data from.
/// \return The schema projection result with column indices of projected Parquet columns.
Result<SchemaProjection> Project(const Schema& expected_schema,
                                 const ::parquet::arrow::SchemaManifest& parquet_schema);

/// \brief Get the selected column indices by walking through the projection result.
///
/// \param projection The schema projection result.
/// \return The selected column indices.
Result<std::vector<int>> SelectedColumnIndices(const SchemaProjection& projection);

/// \brief Check whether the Parquet schema has field IDs.
///
/// \param root_node The root node of the Parquet schema.
/// \return True if the Parquet schema has field IDs, false otherwise.
bool HasFieldIds(const ::parquet::schema::NodePtr& root_node);

}  // namespace iceberg::parquet
