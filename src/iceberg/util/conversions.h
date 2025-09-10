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

#include <span>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/util/conversions.h
/// \brief Conversion utilities for primitive types

namespace iceberg {

/// \brief Conversion utilities for primitive types
class ICEBERG_EXPORT Conversions {
 public:
  /// \brief Convert a literal value to bytes
  static Result<std::vector<uint8_t>> ToBytes(const PrimitiveType& type,
                                              const Literal::Value& value);

  static Result<std::vector<uint8_t>> ToBytes(const Literal& literal);

  /// \brief Convert bytes to a literal value
  static Result<Literal::Value> FromBytes(const PrimitiveType& type,
                                          std::span<const uint8_t> data);

  static Result<Literal> FromBytes(std::shared_ptr<PrimitiveType> type,
                                   std::span<const uint8_t> data);
};

}  // namespace iceberg
