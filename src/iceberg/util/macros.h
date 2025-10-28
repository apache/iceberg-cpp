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

#include <cassert>

#include "iceberg/exception.h"
#include "iceberg/result.h"

#define ICEBERG_RETURN_UNEXPECTED(result)                       \
  if (auto&& result_name = result; !result_name) [[unlikely]] { \
    return std::unexpected<Error>(result_name.error());         \
  }

#define ICEBERG_ASSIGN_OR_RAISE_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                               \
  ICEBERG_RETURN_UNEXPECTED(result_name)                      \
  lhs = std::move(result_name.value());

#define ICEBERG_CONCAT(x, y) x##y

#define ICEBERG_ASSIGN_OR_RAISE_NAME(x, y) ICEBERG_CONCAT(x, y)

#define ICEBERG_ASSIGN_OR_RAISE(lhs, rexpr)                                             \
  ICEBERG_ASSIGN_OR_RAISE_IMPL(ICEBERG_ASSIGN_OR_RAISE_NAME(result_, __COUNTER__), lhs, \
                               rexpr)

#define ICEBERG_DCHECK(expr, message) assert((expr) && (message))

#define ICEBERG_THROW_NOT_OK(result)                            \
  if (auto&& result_name = result; !result_name) [[unlikely]] { \
    throw iceberg::IcebergError(result_name.error().message);   \
  }

#define ICEBERG_ASSIGN_OR_THROW_IMPL(result_name, lhs, rexpr) \
  auto&& result_name = (rexpr);                               \
  ICEBERG_THROW_NOT_OK(result_name);                          \
  lhs = std::move(result_name.value());

#define ICEBERG_ASSIGN_OR_THROW(lhs, rexpr) \
  ICEBERG_ASSIGN_OR_THROW_IMPL(             \
      ICEBERG_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr);
