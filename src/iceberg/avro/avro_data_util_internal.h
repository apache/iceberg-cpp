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

#include <cstddef>
#include <cstdint>
#include <vector>

#include <avro/GenericDatum.hh>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data.h"
#include "iceberg/metadata_column_context_internal.h"
#include "iceberg/result.h"
#include "iceberg/schema_util.h"
#include "iceberg/util/decimal.h"

namespace iceberg::avro {

/// \brief Append the default value of a `kDefault` projection to a nanoarrow array
/// builder.
Status AppendDefaultToBuilder(const FieldProjection& projection, ArrowArray* builder);

/// \brief Append the value of a metadata column (_file, _pos, _row_id or
/// _last_updated_sequence_number) for the current row to a nanoarrow array builder.
Status AppendMetadataToBuilder(int32_t field_id,
                               const MetadataColumnContext& metadata_context,
                               ArrowArray* builder);

/// \brief Append an Avro datum to a nanoarrow array builder.
///
/// This function handles schema evolution by using the provided projection to map
/// fields from the Avro data to the expected Arrow schema.
///
/// \param avro_node The Avro schema node (must be a record at root level)
/// \param avro_datum The Avro data to append
/// \param projection Schema projection from `projected_schema` to `avro_node`
/// \param projected_schema The projected schema
/// \param metadata_context Context for populating metadata columns
/// \param array_builder The nanoarrow struct array to append one row to
/// \return Status indicating success or failure
Status AppendDatumToBuilder(const ::avro::NodePtr& avro_node,
                            const ::avro::GenericDatum& avro_datum,
                            const SchemaProjection& projection,
                            const Schema& projected_schema,
                            const MetadataColumnContext& metadata_context,
                            ArrowArray* array_builder);

/// \brief Encode a decimal as exactly `fixed_size` big-endian signed bytes.
/// Sign-extends smaller values and rejects values that cannot fit the fixed width.
Status EncodeDecimalToFixed(const Decimal& value, size_t fixed_size,
                            std::vector<uint8_t>* bytes);

/// \brief Read the decimal128 value at `index` of a nanoarrow array view.
Decimal GetDecimal(const ArrowArrayView& array_view, int64_t index);

/// \brief Read a list or map offset at a logical index in a possibly sliced array.
inline int64_t GetCollectionChildOffset(const ArrowArrayView& array_view, int64_t index) {
  // Nanoarrow indexes the raw offset buffer without applying the array offset.
  return ArrowArrayViewListChildOffset(&array_view, array_view.offset + index);
}

/// \brief Extract an Avro datum from a nanoarrow array view.
///
/// \param array_view The view of the array to extract from.
/// \param index The index of the element to extract.
/// \param datum The Avro datum to extract to. Its Avro type should be consistent with the
/// Arrow type.
Status ExtractDatumFromArray(const ArrowArrayView& array_view, int64_t index,
                             ::avro::GenericDatum* datum);

}  // namespace iceberg::avro
