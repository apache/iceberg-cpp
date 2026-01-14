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

#include <memory>
#include <string>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"

namespace iceberg {

struct HumanStringTestParam {
  std::string test_name;
  std::shared_ptr<Type> source_type;
  Literal literal;
  std::vector<std::string> expecteds;
};

class IdentityHumanStringTest : public ::testing::TestWithParam<HumanStringTestParam> {
 protected:
  std::vector<std::shared_ptr<Transform>> transforms_{{Transform::Identity()}};
};

TEST_P(IdentityHumanStringTest, ToHumanString) {
  const auto& param = GetParam();
  for (int32_t i = 0; i < transforms_.size(); ++i) {
    EXPECT_THAT(transforms_[i]->ToHumanString(param.literal),
                HasValue(::testing::Eq(param.expecteds[i])));
  }
}

INSTANTIATE_TEST_SUITE_P(
    IdentityHumanStringTestCases, IdentityHumanStringTest,
    ::testing::Values(
        HumanStringTestParam{.test_name = "Null",
                             .literal = Literal::Null(std::make_shared<IntType>()),
                             .expecteds{"null"}},
        HumanStringTestParam{.test_name = "Binary",
                             .literal = Literal::Binary(std::vector<uint8_t>{1, 2, 3}),
                             .expecteds{"AQID"}},
        HumanStringTestParam{.test_name = "Fixed",
                             .literal = Literal::Fixed(std::vector<uint8_t>{1, 2, 3}),
                             .expecteds{"AQID"}},
        HumanStringTestParam{.test_name = "Date",
                             .literal = Literal::Date(17501),
                             .expecteds{"2017-12-01"}},
        HumanStringTestParam{.test_name = "Time",
                             .literal = Literal::Time(36775038194),
                             .expecteds{"10:12:55.038194"}},
        HumanStringTestParam{.test_name = "TimestampWithZone",
                             .literal = Literal::TimestampTz(1512151975038194),
                             .expecteds{"2017-12-01T18:12:55.038194+00:00"}},
        HumanStringTestParam{.test_name = "TimestampWithoutZone",
                             .literal = Literal::Timestamp(1512123175038194),
                             .expecteds{"2017-12-01T10:12:55.038194"}},
        HumanStringTestParam{.test_name = "Long",
                             .literal = Literal::Long(-1234567890000L),
                             .expecteds{"-1234567890000"}},
        HumanStringTestParam{.test_name = "String",
                             .literal = Literal::String("a/b/c=d"),
                             .expecteds{"a/b/c=d"}}),
    [](const ::testing::TestParamInfo<HumanStringTestParam>& info) {
      return info.param.test_name;
    });

class DateHumanStringTest : public ::testing::TestWithParam<HumanStringTestParam> {
 protected:
  std::vector<std::shared_ptr<Transform>> transforms_{
      Transform::Year(), Transform::Month(), Transform::Day()};
};

TEST_P(DateHumanStringTest, ToHumanString) {
  const auto& param = GetParam();

  for (uint32_t i = 0; i < transforms_.size(); i++) {
    ICEBERG_UNWRAP_OR_FAIL(auto trans_func,
                           transforms_[i]->Bind(std::make_shared<DateType>()));
    ICEBERG_UNWRAP_OR_FAIL(auto literal, trans_func->Transform(param.literal));
    EXPECT_THAT(transforms_[i]->ToHumanString(literal),
                HasValue(::testing::Eq(param.expecteds[i])));
  }
}

INSTANTIATE_TEST_SUITE_P(
    DateHumanStringTestCases, DateHumanStringTest,
    ::testing::Values(
        HumanStringTestParam{.test_name = "Date",
                             .literal = Literal::Date(17501),
                             .expecteds = {"2017", "2017-12", "2017-12-01"}},
        HumanStringTestParam{.test_name = "NegativeDate",
                             .literal = Literal::Date(-2),
                             .expecteds = {"1969", "1969-12", "1969-12-30"}},
        HumanStringTestParam{.test_name = "DateLowerBound",
                             .literal = Literal::Date(0),
                             .expecteds = {"1970", "1970-01", "1970-01-01"}},
        HumanStringTestParam{.test_name = "NegativeDateLowerBound",
                             .literal = Literal::Date(-365),
                             .expecteds = {"1969", "1969-01", "1969-01-01"}},
        HumanStringTestParam{.test_name = "NegativeDateUpperBound",
                             .literal = Literal::Date(-1),
                             .expecteds = {"1969", "1969-12", "1969-12-31"}},
        HumanStringTestParam{.test_name = "Null",
                             .literal = Literal::Null(std::make_shared<DateType>()),
                             .expecteds = {"null", "null", "null"}}),
    [](const ::testing::TestParamInfo<HumanStringTestParam>& info) {
      return info.param.test_name;
    });

class TimestampHumanStringTest : public ::testing::TestWithParam<HumanStringTestParam> {
 protected:
  std::vector<std::shared_ptr<Transform>> transforms_{
      Transform::Year(), Transform::Month(), Transform::Day(), Transform::Hour()};
};

TEST_F(TimestampHumanStringTest, InvalidType) {
  ICEBERG_UNWRAP_OR_FAIL(auto above_max,
                         Literal::Long(std::numeric_limits<int64_t>::max())
                             .CastTo(std::make_shared<IntType>()));
  ICEBERG_UNWRAP_OR_FAIL(auto below_min,
                         Literal::Long(std::numeric_limits<int64_t>::min())
                             .CastTo(std::make_shared<IntType>()));

  auto unmatch_type_literal = Literal::Long(std::numeric_limits<int64_t>::max());

  for (const auto& transform : transforms_) {
    auto result = transform->ToHumanString(above_max);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result,
                HasErrorMessage("Cannot transfrom human string for value: aboveMax"));

    result = transform->ToHumanString(below_min);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result,
                HasErrorMessage("Cannot transfrom human string for value: belowMin"));

    result = transform->ToHumanString(unmatch_type_literal);
    EXPECT_THAT(result, IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(result, HasErrorMessage(std::format(
                            "Transfrom human {} from type {} is not supported",
                            TransformTypeToString(transform->transform_type()),
                            unmatch_type_literal.type()->ToString())));
  }
}

TEST_P(TimestampHumanStringTest, ToHumanString) {
  const auto& param = GetParam();
  for (uint32_t i = 0; i < transforms_.size(); i++) {
    ICEBERG_UNWRAP_OR_FAIL(auto trans_func, transforms_[i]->Bind(param.source_type));
    ICEBERG_UNWRAP_OR_FAIL(auto literal, trans_func->Transform(param.literal));
    EXPECT_THAT(transforms_[i]->ToHumanString(literal),
                HasValue(::testing::Eq(param.expecteds[i])));
  }
}

INSTANTIATE_TEST_SUITE_P(
    TimestampHumanStringTestCases, TimestampHumanStringTest,
    ::testing::Values(
        HumanStringTestParam{
            .test_name = "Timestamp",
            .source_type = std::make_shared<TimestampType>(),
            .literal = Literal::Timestamp(1512123175038194),
            .expecteds = {"2017", "2017-12", "2017-12-01", "2017-12-01-10"}},
        HumanStringTestParam{
            .test_name = "NegativeTimestamp",
            .source_type = std::make_shared<TimestampType>(),
            .literal = Literal::Timestamp(-136024961806),
            .expecteds = {"1969", "1969-12", "1969-12-30", "1969-12-30-10"}},
        HumanStringTestParam{
            .test_name = "TimestampLowerBound",
            .source_type = std::make_shared<TimestampType>(),
            .literal = Literal::Timestamp(0),
            .expecteds = {"1970", "1970-01", "1970-01-01", "1970-01-01-00"}},
        HumanStringTestParam{
            .test_name = "NegativeTimestampLowerBound",
            .source_type = std::make_shared<TimestampType>(),
            .literal = Literal::Timestamp(-172800000000),
            .expecteds = {"1969", "1969-12", "1969-12-30", "1969-12-30-00"},
        },
        HumanStringTestParam{
            .test_name = "NegativeTimestampUpperBound",
            .source_type = std::make_shared<TimestampType>(),
            .literal = Literal::Timestamp(-1),
            .expecteds = {"1969", "1969-12", "1969-12-31", "1969-12-31-23"}},
        HumanStringTestParam{
            .test_name = "TimestampTz",
            .source_type = std::make_shared<TimestampTzType>(),
            .literal = Literal::TimestampTz(1512151975038194),
            .expecteds = {"2017", "2017-12", "2017-12-01", "2017-12-01-18"}},
        HumanStringTestParam{.test_name = "Null",
                             .source_type = std::make_shared<TimestampType>(),
                             .literal = Literal::Null(std::make_shared<TimestampType>()),
                             .expecteds = {"null", "null", "null", "null"}}),
    [](const ::testing::TestParamInfo<HumanStringTestParam>& info) {
      return info.param.test_name;
    });

}  // namespace iceberg
