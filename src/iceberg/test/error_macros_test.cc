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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/exception.h"
#include "iceberg/result.h"
#include "iceberg/test/matchers.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace {

using ::iceberg::HasErrorMessage;
using ::iceberg::HasValue;
using ::iceberg::IsError;
using ::iceberg::IsOk;

// The macros expand here, in a namespace with its own nested iceberg namespace, so a
// macro that names iceberg::X instead of ::iceberg::X fails to compile.
namespace caller {

namespace iceberg {}

::iceberg::Result<int> Parse(bool valid) {
  if (!valid) {
    return ::iceberg::InvalidArgument("Invalid input");
  }
  return 42;
}

::iceberg::Result<int> AddOne(bool valid) {
  ICEBERG_ASSIGN_OR_RAISE(auto value, Parse(valid));
  return value + 1;
}

::iceberg::Status Validate(bool valid) {
  ICEBERG_RETURN_UNEXPECTED(Parse(valid));
  return {};
}

::iceberg::Status CheckRange(int value) {
  ICEBERG_PRECHECK(value > 0, "Value must be positive, got {}", value);
  ICEBERG_CHECK(value < 100, "Value must be below 100, got {}", value);
  return {};
}

int AddOneOrThrow(bool valid) {
  ICEBERG_ASSIGN_OR_THROW(auto value, Parse(valid));
  return value + 1;
}

int CheckPositiveOrDie(int value) {
  ICEBERG_CHECK_OR_DIE(value > 0, "Value must be positive, got {}", value);
  return value;
}

class TestBuilder : public ::iceberg::ErrorCollector {
 public:
  TestBuilder& Set(int value) {
    ICEBERG_BUILDER_CHECK(value > 0, "Value must be positive, got {}", value);
    ICEBERG_BUILDER_ASSIGN_OR_RETURN(value_, Parse(value < 100));
    ICEBERG_BUILDER_ASSIGN_OR_RETURN_WITH_ERROR(value_, Parse(value < 50),
                                                "Value {} is too large", value);
    return *this;
  }

 private:
  int value_ = 0;
};

}  // namespace caller

using caller::AddOne;
using caller::AddOneOrThrow;
using caller::CheckPositiveOrDie;
using caller::CheckRange;
using caller::TestBuilder;
using caller::Validate;

}  // namespace

TEST(ErrorMacrosTest, AssignOrRaise) {
  EXPECT_THAT(AddOne(true), HasValue(::testing::Eq(43)));
  EXPECT_THAT(AddOne(false), IsError(iceberg::ErrorKind::kInvalidArgument));
}

TEST(ErrorMacrosTest, ReturnUnexpected) {
  EXPECT_THAT(Validate(true), IsOk());
  EXPECT_THAT(Validate(false), HasErrorMessage("Invalid input"));
}

TEST(ErrorMacrosTest, PrecheckAndCheck) {
  EXPECT_THAT(CheckRange(1), IsOk());
  EXPECT_THAT(CheckRange(0), IsError(iceberg::ErrorKind::kInvalidArgument));
  EXPECT_THAT(CheckRange(100), IsError(iceberg::ErrorKind::kValidationFailed));
}

TEST(ErrorMacrosTest, AssignOrThrow) {
  EXPECT_EQ(AddOneOrThrow(true), 43);
  EXPECT_THROW(AddOneOrThrow(false), iceberg::IcebergError);
}

TEST(ErrorMacrosTest, CheckOrDie) {
  EXPECT_EQ(CheckPositiveOrDie(1), 1);
  EXPECT_THROW(CheckPositiveOrDie(0), iceberg::IcebergError);
}

TEST(ErrorMacrosTest, BuilderMacros) {
  EXPECT_THAT(TestBuilder().Set(1).CheckErrors(), IsOk());
  EXPECT_THAT(TestBuilder().Set(0).CheckErrors(),
              HasErrorMessage("Value must be positive, got 0"));
  EXPECT_THAT(TestBuilder().Set(150).CheckErrors(), HasErrorMessage("Invalid input"));
  EXPECT_THAT(TestBuilder().Set(70).CheckErrors(),
              HasErrorMessage("Value 70 is too large"));
}
