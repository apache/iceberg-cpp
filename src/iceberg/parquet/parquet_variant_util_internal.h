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
#include <unordered_map>

#include <arrow/type_fwd.h>

#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

// Arrow's variant utilities are not available in Arrow 25. Keep ordinary Parquet
// I/O available with that release while enabling variant I/O with the new APIs.
#if __has_include(<parquet/variant/shred.h>) && \
    __has_include(<parquet/variant/unshred.h>) && \
    __has_include(<parquet/variant/validate.h>) && \
    __has_include(<parquet/variant/builder.h>) && \
    __has_include(<parquet/variant/decoding.h>)
#  define ICEBERG_HAS_ARROW_VARIANT 1
#else
#  define ICEBERG_HAS_ARROW_VARIANT 0
#endif

namespace iceberg::parquet {

Status CheckVariantSupport(const Type& type);

// Propagate ancestor nulls to variant arrays before validating complete batches.
Result<std::shared_ptr<::arrow::RecordBatch>> NormalizeVariantReadBatch(
    const std::shared_ptr<::arrow::RecordBatch>& batch, ::arrow::MemoryPool* pool);

Result<std::shared_ptr<::arrow::Array>> ProjectVariantArray(
    const std::shared_ptr<::arrow::Array>& array,
    const std::shared_ptr<::arrow::DataType>& output_type, ::arrow::MemoryPool* pool);

Result<std::shared_ptr<::arrow::Schema>> MakeVariantWriteSchema(
    const Schema& schema, const std::shared_ptr<::arrow::Schema>& input_schema,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    ::arrow::MemoryPool* pool);

Result<std::shared_ptr<::arrow::RecordBatch>> ShredVariantBatch(
    const Schema& schema, const std::shared_ptr<::arrow::RecordBatch>& batch,
    const std::shared_ptr<::arrow::Schema>& output_schema,
    const std::unordered_map<int32_t, std::shared_ptr<Type>>& targets,
    ::arrow::MemoryPool* pool);

}  // namespace iceberg::parquet
