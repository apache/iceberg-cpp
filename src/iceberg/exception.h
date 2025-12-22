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

/// \file iceberg/exception.h
/// Common exception types for Iceberg.  Note that this library primarily uses
/// return values for error handling, not exceptions.  Some operations,
/// however, will throw exceptions in contexts where no other option is
/// available (e.g. a constructor).  In those cases, an exception type from
/// here will be used.

#include <stdexcept>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Base exception class for exceptions thrown by the Iceberg library.
class ICEBERG_EXPORT IcebergError : public std::runtime_error {
 public:
  explicit IcebergError(const std::string& what) : std::runtime_error(what) {}
};

/// \brief Exception thrown when expression construction fails.
class ICEBERG_EXPORT ExpressionError : public IcebergError {
 public:
  explicit ExpressionError(const std::string& what) : IcebergError(what) {}
};

#define ICEBERG_CHECK_OR_DIE(condition, ...)                 \
  do {                                                       \
    if (!(condition)) [[unlikely]] {                         \
      throw iceberg::IcebergError(std::format(__VA_ARGS__)); \
    }                                                        \
  } while (0)

}  // namespace iceberg
