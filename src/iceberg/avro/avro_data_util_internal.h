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

#include <arrow/array/builder_base.h>
#include <arrow/scalar.h>
#include <avro/GenericDatum.hh>

#include "iceberg/arrow/metadata_column_util_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/schema_util.h"

namespace iceberg::avro {

/// \brief Cached Arrow scalar for a `kDefault` field projection.
///
/// Materialized once (see `PrepareDefaultScalars`) so row-by-row Avro decode only
/// needs `AppendScalar` instead of repeating `ToArrowScalar` / `CastTo` per row.
struct AvroDefaultAttributes : FieldProjection::ExtraAttributes {
  std::shared_ptr<::arrow::Scalar> scalar;
};

/// \brief Precompute cast Arrow scalars for every `kDefault` field under `projection`.
///
/// Walks `root_builder` in lockstep with the projection so each default is cast to the
/// builder's Arrow type once per scan. Safe to call repeatedly; existing
/// `AvroDefaultAttributes` entries are left unchanged.
Status PrepareDefaultScalars(SchemaProjection& projection,
                             ::arrow::ArrayBuilder* root_builder);

/// \brief Append a literal once to `builder` while decoding Avro row-by-row.
///
/// Used to materialize `FieldProjection::Kind::kDefault`. Shares `ToArrowScalar` with
/// Parquet's batch path (`MakeDefaultArray`); the append shape stays Avro-local because
/// Avro builds Arrow arrays via per-row `ArrayBuilder`s rather than whole-column arrays.
/// Prefer the `FieldProjection` overload after `PrepareDefaultScalars` so the scalar is
/// reused across rows.
Status AppendDefaultToBuilder(const Literal& literal, ::arrow::ArrayBuilder* builder);

/// \brief Append a `kDefault` projection, reusing a scalar cached on
/// `projection.attributes` when present.
Status AppendDefaultToBuilder(const FieldProjection& projection,
                              ::arrow::ArrayBuilder* builder);

/// \brief Append an Avro datum to an Arrow array builder.
///
/// This function handles schema evolution by using the provided projection to map
/// fields from the Avro data to the expected Arrow schema.
///
/// \param avro_node The Avro schema node (must be a record at root level)
/// \param avro_datum The Avro data to append
/// \param projection Schema projection from `projected_schema` to `avro_node`
/// \param projected_schema The projected schema
/// \param metadata_context Context for populating metadata columns
/// \param array_builder The Arrow array builder to append to (must be a struct builder)
/// \return Status indicating success or failure
Status AppendDatumToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const SchemaProjection& projection,
                            const Schema& projected_schema,
                            const arrow::MetadataColumnContext& metadata_context,
                            ::arrow::ArrayBuilder* array_builder);

/// \brief Extract an Avro datum from an Arrow array.
///
/// \param array The Arrow array to extract from.
/// \param index The index of the element to extract.
/// \param datum The Avro datum to extract to. Its Avro type should be consistent with the
/// Arrow type.
Status ExtractDatumFromArray(const ::arrow::Array& array, int64_t index,
                             ::avro::GenericDatum* datum);

}  // namespace iceberg::avro
