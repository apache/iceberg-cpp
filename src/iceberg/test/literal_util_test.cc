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

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/buffer.h>
#include <arrow/scalar.h>
#include <arrow/type.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/literal_util_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

namespace iceberg::arrow {

namespace {

struct ToArrowScalarParam {
  std::string test_name;
  Literal literal;
  std::shared_ptr<::arrow::Scalar> expected;
};

class ToArrowScalarTest : public ::testing::TestWithParam<ToArrowScalarParam> {};

TEST_P(ToArrowScalarTest, ConvertsTypeAndValue) {
  const auto& param = GetParam();

  ICEBERG_UNWRAP_OR_FAIL(auto scalar, ToArrowScalar(param.literal));
  ASSERT_TRUE(scalar->type->Equals(*param.expected->type))
      << "actual type: " << scalar->type->ToString();
  ASSERT_TRUE(scalar->Equals(*param.expected)) << "actual value: " << scalar->ToString();
}

std::shared_ptr<::arrow::Buffer> BufferOf(const std::vector<uint8_t>& bytes) {
  return ::arrow::Buffer::FromString(std::string(bytes.begin(), bytes.end()));
}

const std::vector<uint8_t> kUuidBytes = {0xF7, 0x9C, 0x3E, 0x09, 0x67, 0x7C, 0x4B, 0xBD,
                                         0xA4, 0x79, 0x3F, 0x34, 0x9C, 0xB7, 0x85, 0xE7};

INSTANTIATE_TEST_SUITE_P(
    AllPrimitiveTypes, ToArrowScalarTest,
    ::testing::Values(
        ToArrowScalarParam{"Boolean", Literal::Boolean(true),
                           std::make_shared<::arrow::BooleanScalar>(true)},
        ToArrowScalarParam{"Int", Literal::Int(42),
                           std::make_shared<::arrow::Int32Scalar>(42)},
        ToArrowScalarParam{"Long", Literal::Long(42),
                           std::make_shared<::arrow::Int64Scalar>(42)},
        ToArrowScalarParam{"Float", Literal::Float(1.5F),
                           std::make_shared<::arrow::FloatScalar>(1.5F)},
        ToArrowScalarParam{"Double", Literal::Double(2.5),
                           std::make_shared<::arrow::DoubleScalar>(2.5)},
        ToArrowScalarParam{"String", Literal::String("iceberg"),
                           std::make_shared<::arrow::StringScalar>("iceberg")},
        ToArrowScalarParam{
            "Binary", Literal::Binary({0x01, 0x02}),
            std::make_shared<::arrow::BinaryScalar>(BufferOf({0x01, 0x02}))},
        ToArrowScalarParam{"Fixed", Literal::Fixed({0xAB, 0xCD}),
                           std::make_shared<::arrow::FixedSizeBinaryScalar>(
                               BufferOf({0xAB, 0xCD}), ::arrow::fixed_size_binary(2))},
        ToArrowScalarParam{"Uuid", Literal::UUID(Uuid::FromBytes(kUuidBytes).value()),
                           std::make_shared<::arrow::FixedSizeBinaryScalar>(
                               BufferOf(kUuidBytes), ::arrow::fixed_size_binary(16))},
        ToArrowScalarParam{
            "Decimal", Literal::Decimal(12345, /*precision=*/9, /*scale=*/2),
            std::make_shared<::arrow::Decimal128Scalar>(::arrow::Decimal128(12345),
                                                        ::arrow::decimal128(9, 2))},
        ToArrowScalarParam{
            "NegativeDecimal", Literal::Decimal(-12345, /*precision=*/9, /*scale=*/2),
            std::make_shared<::arrow::Decimal128Scalar>(::arrow::Decimal128(-12345),
                                                        ::arrow::decimal128(9, 2))},
        ToArrowScalarParam{"Date", Literal::Date(19000),
                           std::make_shared<::arrow::Date32Scalar>(19000)},
        ToArrowScalarParam{"Time", Literal::Time(3600000000),
                           std::make_shared<::arrow::Time64Scalar>(
                               3600000000, ::arrow::time64(::arrow::TimeUnit::MICRO))},
        ToArrowScalarParam{
            "Timestamp", Literal::Timestamp(1672531200000000),
            std::make_shared<::arrow::TimestampScalar>(
                1672531200000000, ::arrow::timestamp(::arrow::TimeUnit::MICRO))},
        ToArrowScalarParam{"TimestampTz", Literal::TimestampTz(1672531200000000),
                           std::make_shared<::arrow::TimestampScalar>(
                               1672531200000000,
                               ::arrow::timestamp(::arrow::TimeUnit::MICRO, "UTC"))},
        ToArrowScalarParam{"TimestampNs", Literal::TimestampNs(1672531200000000000),
                           std::make_shared<::arrow::TimestampScalar>(
                               1672531200000000000,
                               ::arrow::timestamp(::arrow::TimeUnit::NANO))},
        ToArrowScalarParam{"TimestampTzNs", Literal::TimestampTzNs(1672531200000000000),
                           std::make_shared<::arrow::TimestampScalar>(
                               1672531200000000000,
                               ::arrow::timestamp(::arrow::TimeUnit::NANO, "UTC"))}),
    [](const ::testing::TestParamInfo<ToArrowScalarParam>& info) {
      return info.param.test_name;
    });

TEST(LiteralUtilTest, NullLiteralBecomesNullScalar) {
  ICEBERG_UNWRAP_OR_FAIL(auto scalar, ToArrowScalar(Literal::Null(iceberg::int32())));
  ASSERT_TRUE(scalar->type->Equals(*::arrow::int32()));
  ASSERT_FALSE(scalar->is_valid);
}

TEST(LiteralUtilTest, SentinelLiteralsAreRejected) {
  // Casting to a narrower type may produce AboveMax/BelowMin sentinels; they have
  // no value to materialize.
  ICEBERG_UNWRAP_OR_FAIL(
      auto above_max,
      Literal::Long(std::numeric_limits<int64_t>::max()).CastTo(iceberg::int32()));
  ASSERT_TRUE(above_max.IsAboveMax());
  ASSERT_THAT(ToArrowScalar(above_max), IsError(ErrorKind::kNotSupported));
}

TEST(LiteralUtilTest, MakeDefaultArrayFillsAllRows) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto array, MakeDefaultArray(Literal::Int(7), ::arrow::int32(), /*num_rows=*/3,
                                   ::arrow::default_memory_pool()));
  ASSERT_EQ(array->length(), 3);
  ASSERT_EQ(array->null_count(), 0);
  const auto& int_array = static_cast<const ::arrow::Int32Array&>(*array);
  for (int64_t i = 0; i < 3; i++) {
    ASSERT_EQ(int_array.Value(i), 7);
  }
}

TEST(LiteralUtilTest, MakeDefaultArrayCastsToTargetType) {
  // The target Arrow type prevails when it differs from the literal's natural type.
  ICEBERG_UNWRAP_OR_FAIL(
      auto array, MakeDefaultArray(Literal::Int(7), ::arrow::int64(), /*num_rows=*/2,
                                   ::arrow::default_memory_pool()));
  ASSERT_TRUE(array->type()->Equals(*::arrow::int64()));
  const auto& long_array = static_cast<const ::arrow::Int64Array&>(*array);
  ASSERT_EQ(long_array.Value(0), 7);
}

TEST(LiteralUtilTest, AppendDefaultToBuilderAppendsOneValue) {
  ::arrow::Int64Builder builder;
  ASSERT_THAT(AppendDefaultToBuilder(Literal::Long(42), &builder), IsOk());
  ASSERT_THAT(AppendDefaultToBuilder(Literal::Long(42), &builder), IsOk());

  std::shared_ptr<::arrow::Array> array;
  ASSERT_TRUE(builder.Finish(&array).ok());
  ASSERT_EQ(array->length(), 2);
  const auto& long_array = static_cast<const ::arrow::Int64Array&>(*array);
  ASSERT_EQ(long_array.Value(0), 42);
  ASSERT_EQ(long_array.Value(1), 42);
}

}  // namespace

}  // namespace iceberg::arrow
