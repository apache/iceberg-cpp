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

#include "iceberg/catalog/rest/error_handlers.h"

#include <string_view>

#include "iceberg/catalog/rest/types.h"

namespace iceberg::rest {

namespace {

constexpr std::string_view kIllegalArgumentException = "IllegalArgumentException";
constexpr std::string_view kNoSuchNamespaceException = "NoSuchNamespaceException";
constexpr std::string_view kNamespaceNotEmptyException = "NamespaceNotEmptyException";
constexpr std::string_view kNoSuchTableException = "NoSuchTableException";
constexpr std::string_view kNotFoundException = "NotFoundException";
constexpr std::string_view kRestException = "RESTException";
constexpr std::string_view kInvalidClient = "invalid_client";
constexpr std::string_view kInvalidRequest = "invalid_request";
constexpr std::string_view kInvalidGrant = "invalid_grant";
constexpr std::string_view kUnauthorizedClient = "unauthorized_client";
constexpr std::string_view kUnsupportedGrantType = "unsupported_grant_type";
constexpr std::string_view kInvalidScope = "invalid_scope";
constexpr std::string_view kNull = "null";

std::string_view NullIfEmpty(const std::string& value) {
  if (value.empty()) {
    return kNull;
  }
  return value;
}

Status CreateRestError(const ErrorResponse& error) {
  return RestError("Unable to process (code: {}, type: {}): {}", error.code,
                   NullIfEmpty(error.type), NullIfEmpty(error.message));
}

}  // namespace

const std::shared_ptr<DefaultErrorHandler>& DefaultErrorHandler::Instance() {
  static const std::shared_ptr<DefaultErrorHandler> instance{new DefaultErrorHandler()};
  return instance;
}

Status DefaultErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 400:
      if (error.type == kIllegalArgumentException) {
        return InvalidArgument(error.message);
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
      return InternalServerError("Server error: {}: {}", error.type, error.message);
    case 501:
      return NotSupported(error.message);
    case 503:
      return ServiceUnavailable("Service unavailable: {}", error.message);
  }

  return CreateRestError(error);
}

const std::shared_ptr<NamespaceErrorHandler>& NamespaceErrorHandler::Instance() {
  static const std::shared_ptr<NamespaceErrorHandler> instance{
      new NamespaceErrorHandler()};
  return instance;
}

Status NamespaceErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 400:
      if (error.type == kNamespaceNotEmptyException) {
        return NamespaceNotEmpty(error.message);
      }
      return BadRequest("Malformed request: {}", error.message);
    case 404:
      return NoSuchNamespace(error.message);
    case 409:
      return AlreadyExists(error.message);
    case 422:
      return CreateRestError(error);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<DropNamespaceErrorHandler>& DropNamespaceErrorHandler::Instance() {
  static const std::shared_ptr<DropNamespaceErrorHandler> instance{
      new DropNamespaceErrorHandler()};
  return instance;
}

Status DropNamespaceErrorHandler::Accept(const ErrorResponse& error) const {
  if (error.code == 409) {
    return NamespaceNotEmpty(error.message);
  }

  return NamespaceErrorHandler::Accept(error);
}

const std::shared_ptr<ConfigErrorHandler>& ConfigErrorHandler::Instance() {
  static const std::shared_ptr<ConfigErrorHandler> instance{new ConfigErrorHandler()};
  return instance;
}

Status ConfigErrorHandler::Accept(const ErrorResponse& error) const {
  if (error.code == 404 && !error.type.empty() && error.type != kRestException) {
    return NoSuchWarehouse(error.message);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<TableErrorHandler>& TableErrorHandler::Instance() {
  static const std::shared_ptr<TableErrorHandler> instance{new TableErrorHandler()};
  return instance;
}

Status TableErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      if (error.type == kNoSuchNamespaceException) {
        return NoSuchNamespace(error.message);
      }
      if (error.type == kNotFoundException) {
        return NotFound(error.message);
      }
      return NoSuchTable(error.message);
    case 409:
      return AlreadyExists(error.message);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<TableCommitErrorHandler>& TableCommitErrorHandler::Instance() {
  static const std::shared_ptr<TableCommitErrorHandler> instance{
      new TableCommitErrorHandler()};
  return instance;
}

Status TableCommitErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      return NoSuchTable(error.message);
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

const std::shared_ptr<CreateTableErrorHandler>& CreateTableErrorHandler::Instance() {
  static const std::shared_ptr<CreateTableErrorHandler> instance{
      new CreateTableErrorHandler()};
  return instance;
}

Status CreateTableErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      return NoSuchNamespace(error.message);
    case 409:
      return AlreadyExists(error.message);
  }

  return TableCommitErrorHandler::Accept(error);
}

const std::shared_ptr<ViewCommitErrorHandler>& ViewCommitErrorHandler::Instance() {
  static const std::shared_ptr<ViewCommitErrorHandler> instance{
      new ViewCommitErrorHandler()};
  return instance;
}

Status ViewCommitErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      return NoSuchView(error.message);
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

const std::shared_ptr<ViewErrorHandler>& ViewErrorHandler::Instance() {
  static const std::shared_ptr<ViewErrorHandler> instance{new ViewErrorHandler()};
  return instance;
}

Status ViewErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      if (error.type == kNoSuchNamespaceException) {
        return NoSuchNamespace(error.message);
      }
      return NoSuchView(error.message);
    case 409:
      return AlreadyExists(error.message);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<PlanErrorHandler>& PlanErrorHandler::Instance() {
  static const std::shared_ptr<PlanErrorHandler> instance{new PlanErrorHandler()};
  return instance;
}

Status PlanErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      if (error.type == kNoSuchNamespaceException) {
        return NoSuchNamespace(error.message);
      }
      if (error.type == kNoSuchTableException) {
        return NoSuchTable(error.message);
      }
      return NoSuchPlanId(error.message);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<PlanTaskErrorHandler>& PlanTaskErrorHandler::Instance() {
  static const std::shared_ptr<PlanTaskErrorHandler> instance{new PlanTaskErrorHandler()};
  return instance;
}

Status PlanTaskErrorHandler::Accept(const ErrorResponse& error) const {
  switch (error.code) {
    case 404:
      if (error.type == kNoSuchNamespaceException) {
        return NoSuchNamespace(error.message);
      }
      if (error.type == kNoSuchTableException) {
        return NoSuchTable(error.message);
      }
      return NoSuchPlanTask(error.message);
  }

  return DefaultErrorHandler::Accept(error);
}

const std::shared_ptr<OAuthErrorHandler>& OAuthErrorHandler::Instance() {
  static const std::shared_ptr<OAuthErrorHandler> instance{new OAuthErrorHandler()};
  return instance;
}

Status OAuthErrorHandler::Accept(const ErrorResponse& error) const {
  if (!error.type.empty()) {
    if (error.type == kInvalidClient) {
      return NotAuthorized("Not authorized: {}: {}", error.type,
                           NullIfEmpty(error.message));
    }
    if (error.type == kInvalidRequest || error.type == kInvalidGrant ||
        error.type == kUnauthorizedClient || error.type == kUnsupportedGrantType ||
        error.type == kInvalidScope) {
      return BadRequest("Malformed request: {}: {}", error.type,
                        NullIfEmpty(error.message));
    }
  }

  return CreateRestError(error);
}

}  // namespace iceberg::rest
