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

#include <gtest/gtest.h>

#include "iceberg/catalog/rest/types.h"
#include "iceberg/test/matchers.h"

namespace iceberg::rest {

namespace {

void ExpectErrorWithMessage(const Status& status, ErrorKind kind,
                            std::string_view message) {
  ASSERT_FALSE(status.has_value());
  EXPECT_EQ(status.error().kind, kind);
  EXPECT_EQ(status.error().message, message);
}

}  // namespace

TEST(ErrorHandlersTest, DefaultErrorHandlerIncludesCodeAndType) {
  ErrorResponse error{
      .code = 422,
      .type = "ValidationException",
      .message = "Invalid input",
  };

  ExpectErrorWithMessage(
      DefaultErrorHandler::Instance()->Accept(error), ErrorKind::kRestError,
      "Unable to process (code: 422, type: ValidationException): Invalid input");
}

TEST(ErrorHandlersTest, DefaultErrorHandlerWithCodeOnly) {
  ErrorResponse error{
      .code = 422,
      .type = "",
      .message = "",
  };

  ExpectErrorWithMessage(DefaultErrorHandler::Instance()->Accept(error),
                         ErrorKind::kRestError,
                         "Unable to process (code: 422, type: null): null");
}

TEST(ErrorHandlersTest, DefaultErrorHandlerWithCodeAndMessageOnly) {
  ErrorResponse error{
      .code = 422,
      .type = "",
      .message = "Invalid input",
  };

  ExpectErrorWithMessage(DefaultErrorHandler::Instance()->Accept(error),
                         ErrorKind::kRestError,
                         "Unable to process (code: 422, type: null): Invalid input");
}

TEST(ErrorHandlersTest, DefaultErrorHandlerWithCodeAndTypeOnly) {
  ErrorResponse error{
      .code = 422,
      .type = "ValidationException",
      .message = "",
  };

  ExpectErrorWithMessage(
      DefaultErrorHandler::Instance()->Accept(error), ErrorKind::kRestError,
      "Unable to process (code: 422, type: ValidationException): null");
}

TEST(ErrorHandlersTest, NamespaceErrorHandlerFormats422AsRestError) {
  ErrorResponse error{
      .code = 422,
      .type = "ValidationException",
      .message = "Invalid namespace",
  };

  ExpectErrorWithMessage(
      NamespaceErrorHandler::Instance()->Accept(error), ErrorKind::kRestError,
      "Unable to process (code: 422, type: ValidationException): Invalid namespace");
}

TEST(ErrorHandlersTest, TableErrorHandlerMaps404NotFoundToNotFound) {
  ErrorResponse error{
      .code = 404,
      .type = "NotFoundException",
      .message = "Failed to open input stream for file: metadata.json",
  };

  EXPECT_THAT(TableErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNotFound));
  EXPECT_THAT(TableErrorHandler::Instance()->Accept(error),
              HasErrorMessage("metadata.json"));
}

TEST(ErrorHandlersTest, TableErrorHandlerMaps404ToNoSuchTableByDefault) {
  ErrorResponse error{
      .code = 404,
      .type = "NoSuchTableException",
      .message = "Table does not exist",
  };

  EXPECT_THAT(TableErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNoSuchTable));
}

TEST(ErrorHandlersTest, CreateTableErrorHandlerMaps404ToNoSuchNamespace) {
  ErrorResponse error{
      .code = 404,
      .type = "NoSuchNamespaceException",
      .message = "Namespace does not exist",
  };

  EXPECT_THAT(CreateTableErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNoSuchNamespace));
}

TEST(ErrorHandlersTest, CreateTableErrorHandlerMaps409ToAlreadyExists) {
  ErrorResponse error{
      .code = 409,
      .type = "AlreadyExistsException",
      .message = "Table already exists",
  };

  EXPECT_THAT(CreateTableErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kAlreadyExists));
}

TEST(ErrorHandlersTest, CreateTableErrorHandlerMapsServiceFailureToCommitStateUnknown) {
  ErrorResponse error{
      .code = 503,
      .type = "ServiceFailureException",
      .message = "Service unavailable",
  };

  EXPECT_THAT(CreateTableErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_THAT(CreateTableErrorHandler::Instance()->Accept(error),
              HasErrorMessage("Service failed: 503: Service unavailable"));
}

TEST(ErrorHandlersTest, PlanErrorHandlerMapsUnknown404ToNoSuchPlanId) {
  ErrorResponse error{
      .code = 404,
      .type = "UnknownException",
      .message = "Plan does not exist",
  };

  EXPECT_THAT(PlanErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNoSuchPlanId));
}

TEST(ErrorHandlersTest, PlanErrorHandlerDelegates406ToDefaultHandler) {
  ErrorResponse error{
      .code = 406,
      .type = "NotAcceptableException",
      .message = "Not acceptable",
  };

  ExpectErrorWithMessage(
      PlanErrorHandler::Instance()->Accept(error), ErrorKind::kRestError,
      "Unable to process (code: 406, type: NotAcceptableException): Not acceptable");
}

TEST(ErrorHandlersTest, PlanTaskErrorHandlerMapsUnknown404ToNoSuchPlanTask) {
  ErrorResponse error{
      .code = 404,
      .type = "UnknownException",
      .message = "Plan task does not exist",
  };

  EXPECT_THAT(PlanTaskErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNoSuchPlanTask));
}

TEST(ErrorHandlersTest, OAuthErrorHandlerMapsInvalidClientToNotAuthorized) {
  ErrorResponse error{
      .code = 400,
      .type = "invalid_client",
      .message = "Credentials given were invalid",
  };

  EXPECT_THAT(OAuthErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNotAuthorized));
  EXPECT_THAT(
      OAuthErrorHandler::Instance()->Accept(error),
      HasErrorMessage("Not authorized: invalid_client: Credentials given were invalid"));
}

TEST(ErrorHandlersTest, OAuthErrorHandlerMapsClientErrorsToBadRequest) {
  ErrorResponse error{
      .code = 400,
      .type = "invalid_grant",
      .message = "Grant is invalid",
  };

  EXPECT_THAT(OAuthErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kBadRequest));
  EXPECT_THAT(OAuthErrorHandler::Instance()->Accept(error),
              HasErrorMessage("Malformed request: invalid_grant: Grant is invalid"));
}

TEST(ErrorHandlersTest, ConfigErrorHandlerMapsTyped404ToNoSuchWarehouse) {
  ErrorResponse error{
      .code = 404,
      .type = "NoSuchWarehouseException",
      .message = "Warehouse does not exist",
  };

  EXPECT_THAT(ConfigErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kNoSuchWarehouse));
}

TEST(ErrorHandlersTest, ConfigErrorHandlerDelegatesUntyped404ToDefaultHandler) {
  ErrorResponse error{
      .code = 404,
      .type = "",
      .message = "Not Found",
  };

  EXPECT_THAT(ConfigErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kRestError));
  EXPECT_THAT(ConfigErrorHandler::Instance()->Accept(error),
              HasErrorMessage("Not Found"));
}

TEST(ErrorHandlersTest, ConfigErrorHandlerDelegatesNon404ToDefaultHandler) {
  ErrorResponse error{
      .code = 500,
      .type = "",
      .message = "Internal server error",
  };

  EXPECT_THAT(ConfigErrorHandler::Instance()->Accept(error),
              IsError(ErrorKind::kInternalServerError));
  EXPECT_THAT(ConfigErrorHandler::Instance()->Accept(error),
              HasErrorMessage("Internal server error"));
}

}  // namespace iceberg::rest
