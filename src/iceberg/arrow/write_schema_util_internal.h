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

#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace arrow {
class MemoryPool;
}

namespace iceberg::arrow {

/// \brief Align an Arrow batch to an Iceberg write schema.
///
/// Fields are matched by Iceberg field id. Missing fields are materialized from their
/// `write-default`, or as null when optional. Missing required fields without a
/// `write-default` are rejected. Existing values, including explicit nulls, are
/// preserved. Nested structs are aligned recursively.
///
/// `input_batch` is consumed even when alignment fails.
Result<ArrowArray> AlignBatchForWrite(ArrowArray* input_batch, const Schema& input_schema,
                                      const Schema& write_schema,
                                      ::arrow::MemoryPool* pool);

}  // namespace iceberg::arrow
