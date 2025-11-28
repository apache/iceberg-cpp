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

  return RestError("Code: {}, message: {}", error.code, error.message);
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
      return RestError("Unable to process: {}", error.message);
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
      return NoSuchTable(error.message);
    case 409:
      return AlreadyExists(error.message);
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

}  // namespace iceberg::rest
