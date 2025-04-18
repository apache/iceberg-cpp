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

#include <format>
#include <string>

#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Error types for iceberg.
enum class ErrorKind {
  kAlreadyExists,
  kCommitStateUnknown,
  kInvalidArgument,
  kInvalidExpression,
  kInvalidSchema,
  kIOError,
  kJsonParseError,
  kNoSuchNamespace,
  kNoSuchTable,
  kNotFound,
  kNotImplemented,
  kNotSupported,
  kUnknownError,
};

/// \brief Error with a kind and a message.
struct ICEBERG_EXPORT [[nodiscard]] Error {
  ErrorKind kind;
  std::string message;
};

/// /brief Default error trait
template <typename T>
struct DefaultError {
  using type = Error;
};

/// \brief Result alias
template <typename T, typename E = typename DefaultError<T>::type>
using Result = expected<T, E>;

using Status = Result<void>;

/// \brief Create an unexpected error with kAlreadyExists
template <typename... Args>
auto AlreadyExistsError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kAlreadyExists,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kCommitStateUnknown
template <typename... Args>
auto CommitStateUnknownError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kCommitStateUnknown,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kInvalidArgument
template <typename... Args>
auto InvalidArgumentError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kInvalidArgument,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kInvalidExpression
template <typename... Args>
auto InvalidExpressionError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kInvalidExpression,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kInvalidSchema
template <typename... Args>
auto InvalidSchemaError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kInvalidSchema,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kIOError
template <typename... Args>
auto IOError(const std::format_string<Args...> fmt, Args&&... args) -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kIOError,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kJsonParseError
template <typename... Args>
auto JsonParseError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kJsonParseError,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kNoSuchNamespace
template <typename... Args>
auto NoSuchNamespaceError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kNoSuchNamespace,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kNoSuchTable
template <typename... Args>
auto NoSuchTableError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kNoSuchTable,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kNotImplemented
template <typename... Args>
auto NotImplementedError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kNotImplemented,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kNotSupported
template <typename... Args>
auto NotSupportedError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kNotSupported,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

/// \brief Create an unexpected error with kUnknownError
template <typename... Args>
auto UnknownError(const std::format_string<Args...> fmt, Args&&... args)
    -> unexpected<Error> {
  return unexpected<Error>({.kind = ErrorKind::kUnknownError,
                            .message = std::format(fmt, std::forward<Args>(args)...)});
}

}  // namespace iceberg
