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
  /// \param error The error response parsed from the HTTP response body
  /// \return An Error object with appropriate ErrorKind and message
  virtual Status Accept(const ErrorResponse& error) const = 0;

  /// \brief Parse an HTTP error response body.
  ///
  /// \param code The HTTP status code from the failed response
  /// \param text The HTTP response body
  /// \return The parsed error response
  virtual Result<ErrorResponse> ParseResponse(uint32_t code,
                                              const std::string& text) const = 0;
};

/// \brief Default error handler for REST API responses.
class ICEBERG_REST_EXPORT DefaultErrorHandler : public ErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<DefaultErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;
  Result<ErrorResponse> ParseResponse(uint32_t code,
                                      const std::string& text) const override;

 protected:
  constexpr DefaultErrorHandler() = default;
};

/// \brief Namespace-specific error handler for create/read/update operations.
class ICEBERG_REST_EXPORT NamespaceErrorHandler : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<NamespaceErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 protected:
  constexpr NamespaceErrorHandler() = default;
};

/// \brief Error handler for drop namespace operations.
class ICEBERG_REST_EXPORT DropNamespaceErrorHandler final : public NamespaceErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<DropNamespaceErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr DropNamespaceErrorHandler() = default;
};

/// \brief Error handler for the catalog config endpoint.
class ICEBERG_REST_EXPORT ConfigErrorHandler final : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<ConfigErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr ConfigErrorHandler() = default;
};

/// \brief Table-level error handler.
class ICEBERG_REST_EXPORT TableErrorHandler final : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<TableErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr TableErrorHandler() = default;
};

/// \brief Table commit operation error handler.
class ICEBERG_REST_EXPORT TableCommitErrorHandler : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<TableCommitErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 protected:
  constexpr TableCommitErrorHandler() = default;
};

/// \brief Table create commit operation error handler.
class ICEBERG_REST_EXPORT CreateTableErrorHandler final : public TableCommitErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<CreateTableErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr CreateTableErrorHandler() = default;
};

/// \brief View-level error handler.
class ICEBERG_REST_EXPORT ViewErrorHandler final : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<ViewErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr ViewErrorHandler() = default;
};

/// \brief View commit operation error handler.
class ICEBERG_REST_EXPORT ViewCommitErrorHandler final : public DefaultErrorHandler {
 public:
  /// \brief Returns the singleton instance
  static const std::shared_ptr<ViewCommitErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr ViewCommitErrorHandler() = default;
};

/// \brief Plan operation error handler.
class ICEBERG_REST_EXPORT PlanErrorHandler final : public DefaultErrorHandler {
 public:
  static const std::shared_ptr<PlanErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr PlanErrorHandler() = default;
};

/// \brief Fetch scan tasks operation error handler.
class ICEBERG_REST_EXPORT PlanTaskErrorHandler final : public DefaultErrorHandler {
 public:
  static const std::shared_ptr<PlanTaskErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;

 private:
  constexpr PlanTaskErrorHandler() = default;
};

/// \brief OAuth token endpoint error handler.
class ICEBERG_REST_EXPORT OAuthErrorHandler final : public ErrorHandler {
 public:
  static const std::shared_ptr<OAuthErrorHandler>& Instance();

  Status Accept(const ErrorResponse& error) const override;
  Result<ErrorResponse> ParseResponse(uint32_t code,
                                      const std::string& text) const override;

 private:
  constexpr OAuthErrorHandler() = default;
};

}  // namespace iceberg::rest
