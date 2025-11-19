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

#include "iceberg/catalog/rest/iceberg_rest_export.h"
#include "iceberg/catalog/rest/types.h"
#include "iceberg/result.h"

/// \file iceberg/catalog/rest/error_handlers.h
/// Error handlers for different HTTP error types in Iceberg REST API.

namespace iceberg::rest {

/// \brief Error handler interface for processing REST API error responses. Maps HTTP
/// status codes to appropriate ErrorKind values following the Iceberg REST specification.
class ICEBERG_REST_EXPORT ErrorHandler {
 public:
  virtual ~ErrorHandler() = default;

  /// \brief Process an error response and return an appropriate Error.
  ///
  /// \param error The error model parsed from the HTTP response body
  /// \return An Error object with appropriate ErrorKind and message
  virtual Status Accept(const ErrorModel& error) const = 0;
};

/// \brief Default error handler for REST API responses.
class ICEBERG_REST_EXPORT DefaultErrorHandler : public ErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 400:
        if (error.type == "IllegalArgumentException") {
          return InvalidArgument("{}", error.message);
        }
        return BadRequest("Malformed request: {}", error.message);

      case 401:
        return NotAuthorized("Not authorized: {}", error.message);

      case 403:
        return Forbidden("Forbidden: {}", error.message);

      case 405:
      case 406:
        break;

      case 500:
        return ServiceFailure("Server error: {}: {}", error.type, error.message);

      case 501:
        return NotSupported("{}", error.message);

      case 503:
        return ServiceUnavailable("Service unavailable: {}", error.message);
    }
    return RESTError("Unable to process: {}", error.message);
  }
};

/// \brief Namespace-specific error handler for create/read/update operations.
class ICEBERG_REST_EXPORT NamespaceErrorHandler : public DefaultErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 400:
        if (error.type == "NamespaceNotEmptyException") {
          return NamespaceNotEmpty("{}", error.message);
        }
        return BadRequest("Malformed request: {}", error.message);

      case 404:
        return NoSuchNamespace("{}", error.message);

      case 409:
        return AlreadyExists("{}", error.message);

      case 422:
        return RESTError("Unable to process: {}", error.message);
    }

    return DefaultErrorHandler::Accept(error);
  }
};

/// \brief Error handler for drop namespace operations.
class ICEBERG_REST_EXPORT DropNamespaceErrorHandler : public NamespaceErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    if (error.code == 409) {
      return NamespaceNotEmpty("{}", error.message);
    }

    // Delegate to parent handler
    return NamespaceErrorHandler::Accept(error);
  }
};

/// \brief Table-level error handler.
class ICEBERG_REST_EXPORT TableErrorHandler : public DefaultErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 404:
        if (error.type == "NoSuchNamespaceException") {
          return NoSuchNamespace("{}", error.message);
        }
        return NoSuchTable("{}", error.message);

      case 409:
        return AlreadyExists("{}", error.message);
    }

    return DefaultErrorHandler::Accept(error);
  }
};

/// \brief View-level error handler.
///
/// Handles view-specific errors including NoSuchView, NoSuchNamespace,
/// and AlreadyExists scenarios.
class ICEBERG_REST_EXPORT ViewErrorHandler : public DefaultErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 404:
        if (error.type == "NoSuchNamespaceException") {
          return NoSuchNamespace("{}", error.message);
        }
        return NoSuchView("{}", error.message);

      case 409:
        return AlreadyExists("{}", error.message);
    }

    return DefaultErrorHandler::Accept(error);
  }
};

/// \brief Table commit operation error handler.
class ICEBERG_REST_EXPORT TableCommitErrorHandler : public DefaultErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 404:
        return NoSuchTable("{}", error.message);

      case 409:
        return CommitFailed("Commit failed: {}", error.message);

      case 500:
      case 502:
      case 503:
      case 504:
        return CommitStateUnknown("Service failed: {}: {}", error.code, error.message);
    }

    return DefaultErrorHandler::Accept(error);
  }
};

/// \brief View commit operation error handler.
class ICEBERG_REST_EXPORT ViewCommitErrorHandler : public DefaultErrorHandler {
 public:
  Status Accept(const ErrorModel& error) const override {
    switch (error.code) {
      case 404:
        return NoSuchView("{}", error.message);

      case 409:
        return CommitFailed("Commit failed: {}", error.message);

      case 500:
      case 502:
      case 503:
      case 504:
        return CommitStateUnknown("Service failed: {}: {}", error.code, error.message);
    }

    return DefaultErrorHandler::Accept(error);
  }
};

}  // namespace iceberg::rest
