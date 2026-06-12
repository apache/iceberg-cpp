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

#include <cstdint>
#include <memory>

#include <arrow/type_fwd.h>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"

namespace iceberg::arrow {

/// \brief Convert a primitive literal to an Arrow scalar of its canonical Arrow type.
///
/// A null literal converts to a null scalar of the corresponding Arrow type.
Result<std::shared_ptr<::arrow::Scalar>> ToArrowScalar(const Literal& literal);

/// \brief Create an Arrow array of `num_rows` rows where every row holds the literal
/// value, e.g. to materialize a missing column with a default value.
///
/// The array is cast to `type` when the literal's canonical Arrow type differs.
Result<std::shared_ptr<::arrow::Array>> MakeDefaultArray(
    const Literal& literal, const std::shared_ptr<::arrow::DataType>& type,
    int64_t num_rows, ::arrow::MemoryPool* pool);

/// \brief Append the literal value once to `builder`, e.g. to materialize a missing
/// field with a default value while building rows.
Status AppendDefaultToBuilder(const Literal& literal, ::arrow::ArrayBuilder* builder);

}  // namespace iceberg::arrow
