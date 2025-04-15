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

#include <arrow/result.h>
#include <arrow/status.h>

#include "iceberg/result.h"

namespace iceberg::arrow {

inline ErrorKind ToErrorKind(const ::arrow::Status& status) {
  switch (status.code()) {
    case ::arrow::StatusCode::IOError:
      return ErrorKind::kIOError;
    default:
      return ErrorKind::kUnknownError;
  }
}

#define ICEBERG_ARROW_ASSIGN_OR_RETURN_IMPL(result_name, lhs, rexpr, error_transform) \
  auto&& result_name = (rexpr);                                                       \
  if (!result_name.ok()) {                                                            \
    return unexpected<Error>{{.kind = error_transform(result_name.status()),          \
                              .message = result_name.status().ToString()}};           \
  }                                                                                   \
  lhs = std::move(result_name).ValueOrDie();

#define ICEBERG_ARROW_ASSIGN_OR_RETURN(lhs, rexpr) \
  ICEBERG_ARROW_ASSIGN_OR_RETURN_IMPL(             \
      ARROW_ASSIGN_OR_RAISE_NAME(_error_or_value, __COUNTER__), lhs, rexpr, ToErrorKind)

#define ICEBERG_ARROW_RETURN_NOT_OK(expr)                                 \
  do {                                                                    \
    auto&& _status = (expr);                                              \
    if (!_status.ok()) {                                                  \
      return unexpected<Error>{                                           \
          {.kind = ToErrorKind(_status), .message = _status.ToString()}}; \
    }                                                                     \
  } while (0)

}  // namespace iceberg::arrow
