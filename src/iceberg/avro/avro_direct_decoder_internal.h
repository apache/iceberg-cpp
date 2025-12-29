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
#include <span>

#include <arrow/array/builder_base.h>
#include <avro/Decoder.hh>
#include <avro/Node.hh>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/schema_util.h"

namespace iceberg::avro {

/// \brief Context for reusing scratch buffers during Avro decoding
///
/// Avoids frequent small allocations by reusing temporary buffers across
/// multiple decode operations. This is particularly important for string,
/// binary, and fixed-size data types.
struct DecodeContext {
  // Scratch buffer for string decoding (reused across rows)
  std::string string_scratch;
  // Scratch buffer for binary/fixed/uuid/decimal data (reused across rows)
  std::vector<uint8_t> bytes_scratch;
  // Cache for avro field index to projection index mapping
  // Key: pointer to projections array (identifies struct schema)
  // Value: vector mapping avro field index -> projection index (-1 if not projected)
  std::unordered_map<const FieldProjection*, std::vector<int>> avro_to_projection_cache;
};

/// \brief Directly decode Avro data to Arrow array builders without GenericDatum
///
/// Eliminates the GenericDatum intermediate layer by directly calling Avro decoder
/// methods and immediately appending to Arrow builders. Matches Java Iceberg's
/// ValueReader approach for better performance.
///
/// Features:
/// - All primitive, temporal, and logical types
/// - Nested types (struct, list, map)
/// - Union types with bounds checking
/// - Field skipping for schema projection
///
/// Schema Evolution:
/// Schema resolution is handled via SchemaProjection (from Project() function).
/// Supports field reordering and missing fields (set to NULL). Default values
/// are NOT currently supported.
///
/// Error Handling:
/// - Type mismatches → InvalidArgument
/// - Union branch out of range → InvalidArgument
/// - Decimal precision insufficient → InvalidArgument
/// - Missing logical type → InvalidArgument
///
/// \param avro_node The Avro schema node for the data being decoded
/// \param decoder The Avro decoder positioned at the data to read
/// \param projection The field projections (from Project() function)
/// \param projected_schema The target Iceberg schema after projection
/// \param array_builder The Arrow array builder to append decoded data to
/// \param ctx Decode context for reusing scratch buffers
/// \return Status::OK if successful, or an error status
Status DecodeAvroToBuilder(const ::avro::NodePtr& avro_node, ::avro::Decoder& decoder,
                           const SchemaProjection& projection,
                           const Schema& projected_schema,
                           ::arrow::ArrayBuilder* array_builder, DecodeContext& ctx);

}  // namespace iceberg::avro
