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

#include "iceberg/expression/manifest_evaluator.h"

#include <optional>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/type.h"

namespace iceberg {

class ManifestEvaluatorTest : public ::testing::Test {
 protected:
  static constexpr int32_t kIntMinValue = 30;
  static constexpr int32_t kIntMaxValue = 79;

  void SetUp() override {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int32()),
            SchemaField::MakeOptional(4, "all_nulls_missing_nan", string()),
            SchemaField::MakeOptional(5, "some_nulls", string()),
            SchemaField::MakeOptional(6, "no_nulls", string()),
            SchemaField::MakeOptional(7, "float", float32()),
            SchemaField::MakeOptional(8, "all_nulls_double", float64()),
            SchemaField::MakeOptional(9, "all_nulls_no_nans", float32()),
            SchemaField::MakeOptional(10, "all_nans", float64()),
            SchemaField::MakeOptional(11, "both_nan_and_null", float32()),
            SchemaField::MakeOptional(12, "no_nan_or_null", float64()),
            SchemaField::MakeOptional(13, "all_nulls_missing_nan_float", float32()),
            SchemaField::MakeOptional(14, "all_same_value_or_null", string()),
            SchemaField::MakeOptional(15, "no_nulls_same_value_a", string()),
        },
        0);

    ICEBERG_UNWRAP_OR_FAIL(auto spec_result,
                           PartitionSpec::Make(/*spec_id=*/0, BuildIdentityFields()));
    spec_ = std::shared_ptr<PartitionSpec>(std::move(spec_result));

    file_ = BuildManifestFile();
    no_stats_.manifest_path = "no-stats.avro";
    no_stats_.partition_spec_id = 0;
  }

  std::vector<PartitionField> BuildIdentityFields() {
    std::vector<PartitionField> fields;
    int32_t partition_field_id = PartitionSpec::kLegacyPartitionDataIdStart;
    auto add_field = [&](int32_t source_id, std::string name) {
      fields.emplace_back(source_id, partition_field_id++, std::move(name),
                          Transform::Identity());
    };
    add_field(1, "id");
    add_field(4, "all_nulls_missing_nan");
    add_field(5, "some_nulls");
    add_field(6, "no_nulls");
    add_field(7, "float");
    add_field(8, "all_nulls_double");
    add_field(9, "all_nulls_no_nans");
    add_field(10, "all_nans");
    add_field(11, "both_nan_and_null");
    add_field(12, "no_nan_or_null");
    add_field(13, "all_nulls_missing_nan_float");
    add_field(14, "all_same_value_or_null");
    add_field(15, "no_nulls_same_value_a");
    return fields;
  }

  PartitionFieldSummary MakeSummary(bool contains_null, std::optional<bool> contains_nan,
                                    std::optional<Literal> lower = std::nullopt,
                                    std::optional<Literal> upper = std::nullopt) {
    PartitionFieldSummary summary;
    summary.contains_null = contains_null;
    summary.contains_nan = contains_nan;
    if (lower.has_value()) {
      summary.lower_bound = lower->Serialize().value();
    }
    if (upper.has_value()) {
      summary.upper_bound = upper->Serialize().value();
    }
    return summary;
  }

  ManifestFile BuildManifestFile() {
    ManifestFile manifest;
    manifest.manifest_path = "manifest-list.avro";
    manifest.manifest_length = 1024;
    manifest.partition_spec_id = 0;
    manifest.partitions = {
        MakeSummary(/*contains_null=*/false, std::nullopt, Literal::Int(kIntMinValue),
                    Literal::Int(kIntMaxValue)),
        MakeSummary(/*contains_null=*/true, std::nullopt, std::nullopt, std::nullopt),
        MakeSummary(/*contains_null=*/true, std::nullopt, Literal::String("a"),
                    Literal::String("z")),
        MakeSummary(/*contains_null=*/false, std::nullopt, Literal::String("a"),
                    Literal::String("z")),
        MakeSummary(/*contains_null=*/false, std::nullopt, Literal::Float(0.0F),
                    Literal::Float(20.0F)),
        MakeSummary(/*contains_null=*/true, std::nullopt, std::nullopt, std::nullopt),
        MakeSummary(/*contains_null=*/true, /*contains_nan=*/false, std::nullopt,
                    std::nullopt),
        MakeSummary(/*contains_null=*/false, /*contains_nan=*/true, std::nullopt,
                    std::nullopt),
        MakeSummary(/*contains_null=*/true, /*contains_nan=*/true, std::nullopt,
                    std::nullopt),
        MakeSummary(/*contains_null=*/false, /*contains_nan=*/false, Literal::Double(0.0),
                    Literal::Double(20.0)),
        MakeSummary(/*contains_null=*/true, std::nullopt, std::nullopt, std::nullopt),
        MakeSummary(/*contains_null=*/true, std::nullopt, Literal::String("a"),
                    Literal::String("a")),
        MakeSummary(/*contains_null=*/false, std::nullopt, Literal::String("a"),
                    Literal::String("a")),
    };
    return manifest;
  }

  void ExpectEval(const std::shared_ptr<Expression>& expr, bool expected,
                  const ManifestFile& manifest, bool case_sensitive = true) {
    ICEBERG_UNWRAP_OR_FAIL(auto evaluator, ManifestEvaluator::MakePartitionFilter(
                                               expr, spec_, *schema_, case_sensitive));
    auto result = evaluator->Evaluate(manifest);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(expected, result.value()) << expr->ToString();
  }

  void ExpectEval(const std::shared_ptr<Expression>& expr, bool expected,
                  bool case_sensitive = true) {
    ExpectEval(expr, expected, file_, case_sensitive);
  }

  std::shared_ptr<Schema> schema_;
  std::shared_ptr<PartitionSpec> spec_;
  ManifestFile file_;
  ManifestFile no_stats_;
};

TEST_F(ManifestEvaluatorTest, AllNulls) {
  ExpectEval(Expressions::NotNull("all_nulls_missing_nan"), false);
  ExpectEval(Expressions::NotNull("all_nulls_missing_nan_float"), true);
  ExpectEval(Expressions::NotNull("some_nulls"), true);
  ExpectEval(Expressions::NotNull("no_nulls"), true);
  ExpectEval(Expressions::StartsWith("all_nulls_missing_nan", "asad"), false);
  ExpectEval(Expressions::NotStartsWith("all_nulls_missing_nan", "asad"), true);
}

TEST_F(ManifestEvaluatorTest, NoNulls) {
  ExpectEval(Expressions::IsNull("all_nulls_missing_nan"), true);
  ExpectEval(Expressions::IsNull("some_nulls"), true);
  ExpectEval(Expressions::IsNull("no_nulls"), false);
  ExpectEval(Expressions::IsNull("both_nan_and_null"), true);
}

TEST_F(ManifestEvaluatorTest, IsNaN) {
  ExpectEval(Expressions::IsNaN("float"), true);
  ExpectEval(Expressions::IsNaN("all_nulls_double"), true);
  ExpectEval(Expressions::IsNaN("all_nulls_missing_nan_float"), true);
  ExpectEval(Expressions::IsNaN("all_nulls_no_nans"), false);
  ExpectEval(Expressions::IsNaN("all_nans"), true);
  ExpectEval(Expressions::IsNaN("both_nan_and_null"), true);
  ExpectEval(Expressions::IsNaN("no_nan_or_null"), false);
}

TEST_F(ManifestEvaluatorTest, NotNaN) {
  ExpectEval(Expressions::NotNaN("float"), true);
  ExpectEval(Expressions::NotNaN("all_nulls_double"), true);
  ExpectEval(Expressions::NotNaN("all_nulls_no_nans"), true);
  ExpectEval(Expressions::NotNaN("all_nans"), false);
  ExpectEval(Expressions::NotNaN("both_nan_and_null"), true);
  ExpectEval(Expressions::NotNaN("no_nan_or_null"), true);
}

TEST_F(ManifestEvaluatorTest, MissingColumn) {
  auto expr = Expressions::LessThan("missing", Literal::Int(5));
  auto evaluator = ManifestEvaluator::MakePartitionFilter(expr, spec_, *schema_, true);
  ASSERT_FALSE(evaluator.has_value());
  ASSERT_TRUE(evaluator.error().message.contains("Cannot find field 'missing'"))
      << evaluator.error().message;
}

TEST_F(ManifestEvaluatorTest, MissingStats) {
  std::vector<std::shared_ptr<Expression>> expressions = {
      Expressions::LessThan("id", Literal::Int(5)),
      Expressions::LessThanOrEqual("id", Literal::Int(30)),
      Expressions::Equal("id", Literal::Int(70)),
      Expressions::GreaterThan("id", Literal::Int(78)),
      Expressions::GreaterThanOrEqual("id", Literal::Int(90)),
      Expressions::NotEqual("id", Literal::Int(101)),
      Expressions::IsNull("id"),
      Expressions::NotNull("id"),
      Expressions::StartsWith("all_nulls_missing_nan", "a"),
      Expressions::IsNaN("float"),
      Expressions::NotNaN("float"),
      Expressions::NotStartsWith("all_nulls_missing_nan", "a"),
  };

  for (const auto& expr : expressions) {
    ExpectEval(expr, true, no_stats_);
  }
}

TEST_F(ManifestEvaluatorTest, Not) {
  ExpectEval(
      Expressions::Not(Expressions::LessThan("id", Literal::Int(kIntMinValue - 25))),
      true);
  ExpectEval(
      Expressions::Not(Expressions::GreaterThan("id", Literal::Int(kIntMinValue - 25))),
      false);
}

TEST_F(ManifestEvaluatorTest, And) {
  ExpectEval(Expressions::And(
                 Expressions::LessThan("id", Literal::Int(kIntMinValue - 25)),
                 Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMinValue - 30))),
             false);
  ExpectEval(Expressions::And(
                 Expressions::LessThan("id", Literal::Int(kIntMinValue - 25)),
                 Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue + 1))),
             false);
  ExpectEval(
      Expressions::And(Expressions::GreaterThan("id", Literal::Int(kIntMinValue - 25)),
                       Expressions::LessThanOrEqual("id", Literal::Int(kIntMinValue))),
      true);
}

TEST_F(ManifestEvaluatorTest, Or) {
  ExpectEval(Expressions::Or(
                 Expressions::LessThan("id", Literal::Int(kIntMinValue - 25)),
                 Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue + 1))),
             false);
  ExpectEval(Expressions::Or(
                 Expressions::LessThan("id", Literal::Int(kIntMinValue - 25)),
                 Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue - 19))),
             true);
}

TEST_F(ManifestEvaluatorTest, IntegerLt) {
  ExpectEval(Expressions::LessThan("id", Literal::Int(kIntMinValue - 25)), false);
  ExpectEval(Expressions::LessThan("id", Literal::Int(kIntMinValue)), false);
  ExpectEval(Expressions::LessThan("id", Literal::Int(kIntMinValue + 1)), true);
  ExpectEval(Expressions::LessThan("id", Literal::Int(kIntMaxValue)), true);
}

TEST_F(ManifestEvaluatorTest, IntegerLtEq) {
  ExpectEval(Expressions::LessThanOrEqual("id", Literal::Int(kIntMinValue - 25)), false);
  ExpectEval(Expressions::LessThanOrEqual("id", Literal::Int(kIntMinValue - 1)), false);
  ExpectEval(Expressions::LessThanOrEqual("id", Literal::Int(kIntMinValue)), true);
  ExpectEval(Expressions::LessThanOrEqual("id", Literal::Int(kIntMaxValue)), true);
}

TEST_F(ManifestEvaluatorTest, IntegerGt) {
  ExpectEval(Expressions::GreaterThan("id", Literal::Int(kIntMaxValue + 6)), false);
  ExpectEval(Expressions::GreaterThan("id", Literal::Int(kIntMaxValue)), false);
  ExpectEval(Expressions::GreaterThan("id", Literal::Int(kIntMaxValue - 1)), true);
  ExpectEval(Expressions::GreaterThan("id", Literal::Int(kIntMaxValue - 4)), true);
}

TEST_F(ManifestEvaluatorTest, IntegerGtEq) {
  ExpectEval(Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue + 6)),
             false);
  ExpectEval(Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue + 1)),
             false);
  ExpectEval(Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue)), true);
  ExpectEval(Expressions::GreaterThanOrEqual("id", Literal::Int(kIntMaxValue - 4)), true);
}

TEST_F(ManifestEvaluatorTest, IntegerEq) {
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMinValue - 25)), false);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMinValue - 1)), false);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMinValue)), true);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMaxValue - 4)), true);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMaxValue)), true);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMaxValue + 1)), false);
  ExpectEval(Expressions::Equal("id", Literal::Int(kIntMaxValue + 6)), false);
}

TEST_F(ManifestEvaluatorTest, IntegerNotEq) {
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMinValue - 25)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMinValue - 1)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMinValue)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMaxValue - 4)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMaxValue)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMaxValue + 1)), true);
  ExpectEval(Expressions::NotEqual("id", Literal::Int(kIntMaxValue + 6)), true);
}

TEST_F(ManifestEvaluatorTest, IntegerNotEqRewritten) {
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMinValue - 25))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMinValue - 1))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMinValue))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMaxValue - 4))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMaxValue))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMaxValue + 1))),
             true);
  ExpectEval(Expressions::Not(Expressions::Equal("id", Literal::Int(kIntMaxValue + 6))),
             true);
}

TEST_F(ManifestEvaluatorTest, CaseInsensitiveIntegerNotEqRewritten) {
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMinValue - 25))),
             true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMinValue - 1))),
             true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMinValue))), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMaxValue - 4))),
             true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMaxValue))), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMaxValue + 1))),
             true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMaxValue + 6))),
             true,
             /*case_sensitive=*/false);
}

TEST_F(ManifestEvaluatorTest, CaseSensitiveIntegerNotEqRewritten) {
  auto expr = Expressions::Not(Expressions::Equal("ID", Literal::Int(kIntMinValue - 25)));
  auto evaluator = ManifestEvaluator::MakePartitionFilter(expr, spec_, *schema_, true);
  ASSERT_FALSE(evaluator.has_value());
  ASSERT_TRUE(evaluator.error().message.contains("Cannot find field 'ID'"))
      << evaluator.error().message;
}

TEST_F(ManifestEvaluatorTest, StringStartsWith) {
  ExpectEval(Expressions::StartsWith("some_nulls", "a"), true, /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("some_nulls", "aa"), true, /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("some_nulls", "dddd"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("some_nulls", "z"), true, /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("no_nulls", "a"), true, /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("some_nulls", "zzzz"), false,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::StartsWith("some_nulls", "1"), false, /*case_sensitive=*/false);
}

TEST_F(ManifestEvaluatorTest, StringNotStartsWith) {
  ExpectEval(Expressions::NotStartsWith("some_nulls", "a"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("some_nulls", "aa"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("some_nulls", "dddd"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("some_nulls", "z"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("no_nulls", "a"), true, /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("some_nulls", "zzzz"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("some_nulls", "1"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("all_same_value_or_null", "a"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("all_same_value_or_null", "aa"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("all_same_value_or_null", "A"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("all_nulls_missing_nan", "A"), true,
             /*case_sensitive=*/false);
  ExpectEval(Expressions::NotStartsWith("no_nulls_same_value_a", "a"), false,
             /*case_sensitive=*/false);
}

TEST_F(ManifestEvaluatorTest, IntegerIn) {
  ExpectEval(Expressions::In("id", {Literal::Int(kIntMinValue - 25),
                                    Literal::Int(kIntMinValue - 24)}),
             false);
  ExpectEval(Expressions::In(
                 "id", {Literal::Int(kIntMinValue - 2), Literal::Int(kIntMinValue - 1)}),
             false);
  ExpectEval(
      Expressions::In("id", {Literal::Int(kIntMinValue - 1), Literal::Int(kIntMinValue)}),
      true);
  ExpectEval(Expressions::In(
                 "id", {Literal::Int(kIntMaxValue - 4), Literal::Int(kIntMaxValue - 3)}),
             true);
  ExpectEval(
      Expressions::In("id", {Literal::Int(kIntMaxValue), Literal::Int(kIntMaxValue + 1)}),
      true);
  ExpectEval(Expressions::In(
                 "id", {Literal::Int(kIntMaxValue + 1), Literal::Int(kIntMaxValue + 2)}),
             false);
  ExpectEval(Expressions::In(
                 "id", {Literal::Int(kIntMaxValue + 6), Literal::Int(kIntMaxValue + 7)}),
             false);
  ExpectEval(Expressions::In("all_nulls_missing_nan",
                             {Literal::String("abc"), Literal::String("def")}),
             false);
  ExpectEval(
      Expressions::In("some_nulls", {Literal::String("abc"), Literal::String("def")}),
      true);
  ExpectEval(
      Expressions::In("no_nulls", {Literal::String("abc"), Literal::String("def")}),
      true);
}

TEST_F(ManifestEvaluatorTest, IntegerNotIn) {
  ExpectEval(Expressions::NotIn("id", {Literal::Int(kIntMinValue - 25),
                                       Literal::Int(kIntMinValue - 24)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMinValue - 2), Literal::Int(kIntMinValue - 1)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMinValue - 1), Literal::Int(kIntMinValue)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMaxValue - 4), Literal::Int(kIntMaxValue - 3)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMaxValue), Literal::Int(kIntMaxValue + 1)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMaxValue + 1), Literal::Int(kIntMaxValue + 2)}),
             true);
  ExpectEval(Expressions::NotIn(
                 "id", {Literal::Int(kIntMaxValue + 6), Literal::Int(kIntMaxValue + 7)}),
             true);
  ExpectEval(Expressions::NotIn("all_nulls_missing_nan",
                                {Literal::String("abc"), Literal::String("def")}),
             true);
  ExpectEval(
      Expressions::NotIn("some_nulls", {Literal::String("abc"), Literal::String("def")}),
      true);
  ExpectEval(
      Expressions::NotIn("no_nulls", {Literal::String("abc"), Literal::String("def")}),
      true);
}

}  // namespace iceberg
