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
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/expression/inclusive_metrics_evaluator.h"
#include "iceberg/expression/term.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {
constexpr bool kRowsMightMatch = true;
constexpr bool kRowCannotMatch = false;
constexpr int64_t kIntMinValue = 30;
constexpr int64_t kIntMaxValue = 79;
constexpr int64_t kMicrosPerDay = 86'400'000'000LL;
constexpr int64_t kTsMinValue = 30 * kMicrosPerDay;
constexpr int64_t kTsMaxValue = 79 * kMicrosPerDay;

std::shared_ptr<UnboundTerm<BoundTransform>> ToBoundTransform(
    const std::shared_ptr<UnboundTransform>& transform) {
  return std::static_pointer_cast<UnboundTerm<BoundTransform>>(transform);
}
}  // namespace

class InclusiveMetricsEvaluatorWithTransformTest : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int64()),
            SchemaField::MakeRequired(2, "ts", timestamp_tz()),
            SchemaField::MakeOptional(3, "all_nulls", int64()),
            SchemaField::MakeOptional(4, "all_nulls_str", string()),
            SchemaField::MakeOptional(5, "no_stats", int64()),
            SchemaField::MakeOptional(6, "str", string()),
        },
        /*schema_id=*/0);

    data_file_ = std::make_shared<DataFile>();
    data_file_->file_path = "file.avro";
    data_file_->file_format = FileFormatType::kAvro;
    data_file_->record_count = 50;
    data_file_->value_counts = {
        {1, 50L},
        {2, 50L},
        {3, 50L},
        {4, 50L},
    };
    data_file_->null_value_counts = {
        {1, 0L},
        {2, 0L},
        {3, 50L},
        {4, 50L},
    };
    data_file_->nan_value_counts.clear();
    data_file_->lower_bounds = {
        {2, Literal::TimestampTz(kTsMinValue).Serialize().value()},
        {6, Literal::String("abc").Serialize().value()},
    };
    data_file_->upper_bounds = {
        {2, Literal::TimestampTz(kTsMaxValue).Serialize().value()},
        {6, Literal::String("abe").Serialize().value()},
    };
  }

  void ExpectShouldRead(const std::shared_ptr<Expression>& expr, bool expected_result,
                        std::shared_ptr<DataFile> file = nullptr,
                        bool case_sensitive = true) {
    auto target_file = file ? file : data_file_;
    ICEBERG_UNWRAP_OR_FAIL(
        auto evaluator, InclusiveMetricsEvaluator::Make(expr, *schema_, case_sensitive));
    auto eval_result = evaluator->Evaluate(*target_file);
    ASSERT_TRUE(eval_result.has_value());
    ASSERT_EQ(eval_result.value(), expected_result) << expr->ToString();
  }

  std::vector<std::shared_ptr<Expression>> MissingStatsExpressions() const {
    auto truncate_no_stats = ToBoundTransform(Expressions::Truncate("no_stats", 10));
    return {
        Expressions::LessThan(truncate_no_stats, Literal::Long(5)),
        Expressions::LessThanOrEqual(truncate_no_stats, Literal::Long(30)),
        Expressions::Equal(truncate_no_stats, Literal::Long(70)),
        Expressions::GreaterThan(truncate_no_stats, Literal::Long(78)),
        Expressions::GreaterThanOrEqual(truncate_no_stats, Literal::Long(90)),
        Expressions::NotEqual(truncate_no_stats, Literal::Long(101)),
        Expressions::IsNull(truncate_no_stats),
        Expressions::NotNull(truncate_no_stats),
    };
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> data_file_;
};

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, AllNullsWithNonOrderPreserving) {
  auto bucket_all_nulls = ToBoundTransform(Expressions::Bucket("all_nulls", 100));
  ExpectShouldRead(Expressions::IsNull(bucket_all_nulls), kRowsMightMatch);
  ExpectShouldRead(Expressions::NotNull(bucket_all_nulls), kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThan(bucket_all_nulls, Literal::Int(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(bucket_all_nulls, Literal::Int(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThan(bucket_all_nulls, Literal::Int(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThanOrEqual(bucket_all_nulls, Literal::Int(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::Equal(bucket_all_nulls, Literal::Int(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::NotEqual(bucket_all_nulls, Literal::Int(30)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::In(bucket_all_nulls, {Literal::Int(1), Literal::Int(2)}),
                   kRowCannotMatch);
  ExpectShouldRead(
      Expressions::NotIn(bucket_all_nulls, {Literal::Int(1), Literal::Int(2)}),
      kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, RequiredWithNonOrderPreserving) {
  auto bucket_ts = ToBoundTransform(Expressions::Bucket("ts", 100));
  ExpectShouldRead(Expressions::IsNull(bucket_ts), kRowsMightMatch);
  ExpectShouldRead(Expressions::NotNull(bucket_ts), kRowsMightMatch);
  ExpectShouldRead(Expressions::LessThan(bucket_ts, Literal::Int(30)), kRowsMightMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(bucket_ts, Literal::Int(30)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::GreaterThan(bucket_ts, Literal::Int(30)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::GreaterThanOrEqual(bucket_ts, Literal::Int(30)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::Equal(bucket_ts, Literal::Int(30)), kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(bucket_ts, Literal::Int(30)), kRowsMightMatch);
  ExpectShouldRead(Expressions::In(bucket_ts, {Literal::Int(1), Literal::Int(2)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(bucket_ts, {Literal::Int(1), Literal::Int(2)}),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, AllNulls) {
  auto truncate_all_nulls = ToBoundTransform(Expressions::Truncate("all_nulls", 10));
  ExpectShouldRead(Expressions::IsNull(truncate_all_nulls), kRowsMightMatch);
  ExpectShouldRead(Expressions::NotNull(truncate_all_nulls), kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThan(truncate_all_nulls, Literal::Long(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(truncate_all_nulls, Literal::Long(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThan(truncate_all_nulls, Literal::Long(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThanOrEqual(truncate_all_nulls, Literal::Long(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::Equal(truncate_all_nulls, Literal::Long(30)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::NotEqual(truncate_all_nulls, Literal::Long(30)),
                   kRowsMightMatch);
  ExpectShouldRead(
      Expressions::In(truncate_all_nulls, {Literal::Long(10), Literal::Long(20)}),
      kRowCannotMatch);
  ExpectShouldRead(
      Expressions::NotIn(truncate_all_nulls, {Literal::Long(10), Literal::Long(20)}),
      kRowsMightMatch);

  auto truncate_all_nulls_str =
      ToBoundTransform(Expressions::Truncate("all_nulls_str", 10));
  ExpectShouldRead(Expressions::StartsWith(truncate_all_nulls_str, "a"), kRowsMightMatch);
  ExpectShouldRead(Expressions::NotStartsWith(truncate_all_nulls_str, "a"),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, MissingColumn) {
  auto expr = Expressions::LessThan(
      ToBoundTransform(Expressions::Truncate("missing", 10)), Literal::Long(20));
  auto result = InclusiveMetricsEvaluator::Make(expr, *schema_, true);
  ASSERT_FALSE(result.has_value()) << result.error().message;
  ASSERT_TRUE(result.error().message.contains("Cannot find field 'missing'"))
      << result.error().message;
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, MissingStats) {
  for (const auto& expr : MissingStatsExpressions()) {
    ExpectShouldRead(expr, kRowsMightMatch);
  }
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, ZeroRecordFile) {
  auto zero_record_file = std::make_shared<DataFile>();
  zero_record_file->file_path = "file.parquet";
  zero_record_file->file_format = FileFormatType::kParquet;
  zero_record_file->record_count = 0;

  for (const auto& expr : MissingStatsExpressions()) {
    ExpectShouldRead(expr, kRowCannotMatch, zero_record_file);
  }
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, Not) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(
      Expressions::Not(Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25))),
      kRowsMightMatch);
  ExpectShouldRead(Expressions::Not(Expressions::GreaterThan(
                       day_ts, Literal::Long(kIntMinValue - 25))),
                   kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, And) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(
      Expressions::And(
          Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMinValue - 30))),
      kRowCannotMatch);
  ExpectShouldRead(
      Expressions::And(
          Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue + 1))),
      kRowCannotMatch);
  ExpectShouldRead(
      Expressions::And(Expressions::GreaterThan(day_ts, Literal::Long(kIntMinValue - 25)),
                       Expressions::LessThanOrEqual(day_ts, Literal::Long(kIntMinValue))),
      kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, Or) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(
      Expressions::Or(
          Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue + 1))),
      kRowCannotMatch);
  ExpectShouldRead(
      Expressions::Or(
          Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25)),
          Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue - 19))),
      kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerLt) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::LessThan(day_ts, Literal::Long(kIntMinValue - 25)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThan(day_ts, Literal::Long(kIntMinValue)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThan(day_ts, Literal::Long(kIntMinValue + 1)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::LessThan(day_ts, Literal::Long(kIntMaxValue)),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerLtEq) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::LessThanOrEqual(day_ts, Literal::Long(kIntMinValue - 25)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(day_ts, Literal::Long(kIntMinValue - 1)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(day_ts, Literal::Long(kIntMinValue)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::LessThanOrEqual(day_ts, Literal::Long(kIntMaxValue)),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerGt) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::GreaterThan(day_ts, Literal::Int(kIntMaxValue + 6)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThan(day_ts, Literal::Date(kIntMaxValue)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThan(day_ts, Literal::Date(kIntMaxValue - 1)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::GreaterThan(day_ts, Literal::Date(kIntMaxValue - 4)),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerGtEq) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(
      Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue + 6)),
      kRowCannotMatch);
  ExpectShouldRead(
      Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue + 1)),
      kRowCannotMatch);
  ExpectShouldRead(Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue)),
                   kRowsMightMatch);
  ExpectShouldRead(
      Expressions::GreaterThanOrEqual(day_ts, Literal::Long(kIntMaxValue - 4)),
      kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerEq) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 25)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 1)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMinValue)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue - 4)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 1)),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 6)),
                   kRowCannotMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerNotEq) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMinValue - 25)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMinValue - 1)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMinValue)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMaxValue - 4)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMaxValue)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMaxValue + 1)),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotEqual(day_ts, Literal::Long(kIntMaxValue + 6)),
                   kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerNotEqRewritten) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 25))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 1))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue - 4))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 1))),
      kRowsMightMatch);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 6))),
      kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, CaseInsensitiveIntegerNotEqRewritten) {
  auto day_ts = ToBoundTransform(Expressions::Day("TS"));
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 25))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue - 1))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMinValue))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue - 4))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 1))),
      kRowsMightMatch, nullptr, false);
  ExpectShouldRead(
      Expressions::Not(Expressions::Equal(day_ts, Literal::Long(kIntMaxValue + 6))),
      kRowsMightMatch, nullptr, false);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, CaseSensitiveIntegerNotEqRewritten) {
  auto day_ts = ToBoundTransform(Expressions::Day("TS"));
  auto expr = Expressions::Not(Expressions::Equal(day_ts, Literal::Long(5)));
  auto result = InclusiveMetricsEvaluator::Make(expr, *schema_, true);
  ASSERT_FALSE(result.has_value()) << result.error().message;
  ASSERT_TRUE(result.error().message.contains("Cannot find field 'TS'"))
      << result.error().message;
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, StringStartsWith) {
  auto truncate_str = ToBoundTransform(Expressions::Truncate("str", 10));
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "a"), kRowsMightMatch);
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "ab"), kRowsMightMatch);
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "b"), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, StringNotStartsWith) {
  auto truncate_str = ToBoundTransform(Expressions::Truncate("str", 10));
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "a"), kRowsMightMatch);
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "ab"), kRowsMightMatch);
  ExpectShouldRead(Expressions::StartsWith(truncate_str, "b"), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerIn) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMinValue - 25),
                                            Literal::Long(kIntMinValue - 24)}),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMinValue - 2),
                                            Literal::Long(kIntMinValue - 1)}),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMinValue - 1),
                                            Literal::Long(kIntMinValue)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMaxValue - 4),
                                            Literal::Long(kIntMaxValue - 3)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMaxValue),
                                            Literal::Long(kIntMaxValue + 1)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMaxValue + 1),
                                            Literal::Long(kIntMaxValue + 2)}),
                   kRowCannotMatch);
  ExpectShouldRead(Expressions::In(day_ts, {Literal::Long(kIntMaxValue + 6),
                                            Literal::Long(kIntMaxValue + 7)}),
                   kRowCannotMatch);

  std::vector<Literal> ids;
  ids.reserve(401);
  for (int i = -400; i <= 0; ++i) {
    ids.emplace_back(Literal::Long(i));
  }
  ExpectShouldRead(Expressions::In(day_ts, ids), kRowsMightMatch);
}

TEST_F(InclusiveMetricsEvaluatorWithTransformTest, IntegerNotIn) {
  auto day_ts = ToBoundTransform(Expressions::Day("ts"));
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMinValue - 25),
                                               Literal::Long(kIntMinValue - 24)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMinValue - 2),
                                               Literal::Long(kIntMinValue - 1)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMinValue - 1),
                                               Literal::Long(kIntMinValue)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMaxValue - 4),
                                               Literal::Long(kIntMaxValue - 3)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMaxValue),
                                               Literal::Long(kIntMaxValue + 1)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMaxValue + 1),
                                               Literal::Long(kIntMaxValue + 2)}),
                   kRowsMightMatch);
  ExpectShouldRead(Expressions::NotIn(day_ts, {Literal::Long(kIntMaxValue + 6),
                                               Literal::Long(kIntMaxValue + 7)}),
                   kRowsMightMatch);

  std::vector<Literal> ids;
  ids.reserve(401);
  for (int i = -400; i <= 0; ++i) {
    ids.emplace_back(Literal::Long(i));
  }
  ExpectShouldRead(Expressions::NotIn(day_ts, ids), kRowsMightMatch);
}

}  // namespace iceberg
