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

#include <expected>
#include <format>
#include <string>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Error types for iceberg.
enum class ErrorKind {
  kAlreadyExists,
  kCommitStateUnknown,
  kDecompressError,
  kInvalidArgument,
  kInvalidArrowData,
  kInvalidExpression,
  kInvalidSchema,
  kInvalidManifest,
  kInvalidManifestList,
  kIOError,
  kJsonParseError,
  kNoSuchNamespace,
  kNoSuchTable,
  kNotAllowed,
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
using Result = std::expected<T, E>;

using Status = Result<void>;

/// \brief Macro to define error creation functions
#define DEFINE_ERROR_FUNCTION(name)                                           \
  template <typename... Args>                                                 \
  inline auto name(const std::format_string<Args...> fmt, Args&&... args)     \
      -> std::unexpected<Error> {                                             \
    return std::unexpected<Error>(                                            \
        {ErrorKind::k##name, std::format(fmt, std::forward<Args>(args)...)}); \
  }

DEFINE_ERROR_FUNCTION(AlreadyExists)
DEFINE_ERROR_FUNCTION(CommitStateUnknown)
DEFINE_ERROR_FUNCTION(DecompressError)
DEFINE_ERROR_FUNCTION(InvalidArgument)
DEFINE_ERROR_FUNCTION(InvalidArrowData)
DEFINE_ERROR_FUNCTION(InvalidExpression)
DEFINE_ERROR_FUNCTION(InvalidSchema)
DEFINE_ERROR_FUNCTION(InvalidManifest)
DEFINE_ERROR_FUNCTION(InvalidManifestList)
DEFINE_ERROR_FUNCTION(IOError)
DEFINE_ERROR_FUNCTION(JsonParseError)
DEFINE_ERROR_FUNCTION(NoSuchNamespace)
DEFINE_ERROR_FUNCTION(NoSuchTable)
DEFINE_ERROR_FUNCTION(NotAllowed)
DEFINE_ERROR_FUNCTION(NotFound)
DEFINE_ERROR_FUNCTION(NotImplemented)
DEFINE_ERROR_FUNCTION(NotSupported)
DEFINE_ERROR_FUNCTION(UnknownError)

#undef DEFINE_ERROR_FUNCTION

}  // namespace iceberg
