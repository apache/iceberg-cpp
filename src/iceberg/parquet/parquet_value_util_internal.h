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

#include <parquet/types.h>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"

namespace iceberg::parquet {

// Convert physical Parquet values to owned Iceberg literals after validating the
// file type against the expected Iceberg type. Shared by statistics and dictionaries.
Result<Literal> ParquetValueToLiteral(bool value, const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(int32_t value, const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(int64_t value, const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(float value, const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(double value, const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(const ::parquet::ByteArray& value,
                                      const PrimitiveType& iceberg_type);
Result<Literal> ParquetValueToLiteral(const ::parquet::FixedLenByteArray& value,
                                      const PrimitiveType& iceberg_type, int32_t length);

}  // namespace iceberg::parquet
