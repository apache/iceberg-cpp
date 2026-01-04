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

#include <vector>

#include <arrow/record_batch.h>
#include <avro/Encoder.hh>
#include <avro/Node.hh>

#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg::avro {

/// \brief Context for reusing scratch buffers during Avro encoding
///
/// Avoids frequent small allocations by reusing temporary buffers across
/// multiple encode operations. This is particularly important for string,
/// binary, and fixed-size data types.
struct EncodeContext {
  // Scratch buffer for binary/fixed/uuid/decimal data (reused across rows)
  std::vector<uint8_t> bytes_scratch;
};

/// \brief Directly encode Arrow data to Avro without GenericDatum
///
/// Eliminates the GenericDatum intermediate layer by directly calling Avro encoder
/// methods from Arrow arrays.
///
/// \param avro_node The Avro schema node for the data being encoded
/// \param encoder The Avro encoder to write data to
/// \param type The Iceberg type for the data
/// \param array The Arrow array containing the data to encode
/// \param row_index The index of the row to encode within the array
/// \param ctx Encode context for reusing scratch buffers
/// \return Status indicating success, or an error status
Status EncodeArrowToAvro(const ::avro::NodePtr& avro_node, ::avro::Encoder& encoder,
                         const Type& type, const ::arrow::Array& array, int64_t row_index,
                         EncodeContext& ctx);

}  // namespace iceberg::avro
