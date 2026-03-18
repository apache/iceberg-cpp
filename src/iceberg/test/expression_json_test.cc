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
#include <optional>
#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

struct ExpressionJsonRoundTripParam {
  std::string name;
  nlohmann::json json;
  Expression::Operation expected_op;
};

class ExpressionJsonRoundTripTest
    : public ::testing::TestWithParam<ExpressionJsonRoundTripParam> {};

TEST_P(ExpressionJsonRoundTripTest, RoundTrip) {
  const auto& param = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(param.json));
  EXPECT_EQ(expr->op(), param.expected_op);
  ICEBERG_UNWRAP_OR_FAIL(auto round_trip, ToJson(*expr));
  EXPECT_EQ(round_trip, param.json);
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionJsonTest, ExpressionJsonRoundTripTest,
    ::testing::Values(
        ExpressionJsonRoundTripParam{"BooleanTrue", true, Expression::Operation::kTrue},
        ExpressionJsonRoundTripParam{"BooleanFalse", false,
                                     Expression::Operation::kFalse},
        ExpressionJsonRoundTripParam{"UnaryIsNull",
                                     {{"type", "is-null"}, {"term", "col"}},
                                     Expression::Operation::kIsNull},
        ExpressionJsonRoundTripParam{"LiteralGt",
                                     {{"type", "gt"}, {"term", "age"}, {"value", 21}},
                                     Expression::Operation::kGt},
        ExpressionJsonRoundTripParam{
            "SetIn",
            {{"type", "in"},
             {"term", "status"},
             {"values", nlohmann::json::array({"active", "pending"})}},
            Expression::Operation::kIn},
        ExpressionJsonRoundTripParam{
            "AndExpression",
            {{"type", "and"},
             {"left", {{"type", "gt"}, {"term", "age"}, {"value", 18}}},
             {"right", {{"type", "lt"}, {"term", "age"}, {"value", 65}}}},
            Expression::Operation::kAnd},
        ExpressionJsonRoundTripParam{
            "NotExpression",
            {{"type", "not"}, {"child", {{"type", "is-null"}, {"term", "name"}}}},
            Expression::Operation::kNot},
        ExpressionJsonRoundTripParam{
            "TransformDay",
            {{"type", "eq"},
             {"term", {{"type", "transform"}, {"transform", "day"}, {"term", "ts"}}},
             {"value", 19738}},
            Expression::Operation::kEq},
        ExpressionJsonRoundTripParam{
            "TransformYear",
            {{"type", "gt"},
             {"term",
              {{"type", "transform"}, {"transform", "year"}, {"term", "timestamp_col"}}},
             {"value", 2020}},
            Expression::Operation::kGt},
        ExpressionJsonRoundTripParam{
            "TransformTruncate",
            {{"type", "lt"},
             {"term",
              {{"type", "transform"}, {"transform", "truncate[4]"}, {"term", "col"}}},
             {"value", 100}},
            Expression::Operation::kLt},
        ExpressionJsonRoundTripParam{
            "LiteralNotEq",
            {{"type", "not-eq"}, {"term", "status"}, {"value", "closed"}},
            Expression::Operation::kNotEq},
        ExpressionJsonRoundTripParam{
            "LiteralLtEq",
            {{"type", "lt-eq"}, {"term", "price"}, {"value", 100}},
            Expression::Operation::kLtEq},
        ExpressionJsonRoundTripParam{
            "LiteralGtEq",
            {{"type", "gt-eq"}, {"term", "quantity"}, {"value", 1}},
            Expression::Operation::kGtEq},
        ExpressionJsonRoundTripParam{
            "SetNotIn",
            {{"type", "not-in"},
             {"term", "category"},
             {"values", nlohmann::json::array({"archived", "deleted"})}},
            Expression::Operation::kNotIn},
        ExpressionJsonRoundTripParam{"UnaryNotNan",
                                     {{"type", "not-nan"}, {"term", "score"}},
                                     Expression::Operation::kNotNan},
        ExpressionJsonRoundTripParam{
            "LiteralStartsWith",
            {{"type", "starts-with"}, {"term", "name"}, {"value", "prefix"}},
            Expression::Operation::kStartsWith},
        ExpressionJsonRoundTripParam{
            "LiteralNotStartsWith",
            {{"type", "not-starts-with"}, {"term", "name"}, {"value", "bad"}},
            Expression::Operation::kNotStartsWith},
        ExpressionJsonRoundTripParam{
            "OrExpression",
            {{"type", "or"},
             {"left", {{"type", "lt"}, {"term", "price"}, {"value", 50}}},
             {"right", {{"type", "not-null"}, {"term", "discount"}}}},
            Expression::Operation::kOr},
        ExpressionJsonRoundTripParam{
            "NestedWithDecimals",
            {{"type", "or"},
             {"left",
              {{"type", "and"},
               {"left",
                {{"type", "in"},
                 {"term", "price"},
                 {"values", nlohmann::json::array({3.14, 2.72})}}},
               {"right", {{"type", "eq"}, {"term", "currency"}, {"value", "USD"}}}}},
             {"right", {{"type", "is-nan"}, {"term", "discount"}}}},
            Expression::Operation::kOr},
        ExpressionJsonRoundTripParam{
            "FixedBinaryInPredicate",
            {{"type", "eq"}, {"term", "col"}, {"value", "010203"}},
            Expression::Operation::kEq},
        ExpressionJsonRoundTripParam{"ScaleDecimalInSet",
                                     {{"type", "in"},
                                      {"term", "amount"},
                                      {"values", nlohmann::json::array({"3.14E+4"})}},
                                     Expression::Operation::kIn}),
    [](const ::testing::TestParamInfo<ExpressionJsonRoundTripParam>& info) {
      return info.param.name;
    });

// -- Object wrapper normalization tests --

TEST(ExpressionJsonTest, PredicateWithObjectLiteral) {
  nlohmann::json input = {{"type", "lt-eq"},
                          {"term", "col"},
                          {"value", {{"type", "literal"}, {"value", 50}}}};
  nlohmann::json expected = {{"type", "lt-eq"}, {"term", "col"}, {"value", 50}};
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(input));
  ICEBERG_UNWRAP_OR_FAIL(auto result, ToJson(*expr));
  EXPECT_EQ(result, expected);
}

TEST(ExpressionJsonTest, LiteralBoolean) {
  nlohmann::json input = {{"type", "literal"}, {"value", true}};
  nlohmann::json expected = true;
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(input));
  ICEBERG_UNWRAP_OR_FAIL(auto result, ToJson(*expr));
  EXPECT_EQ(result, expected);
}

TEST(ExpressionJsonTest, PredicateWithObjectReference) {
  nlohmann::json input = {{"type", "lt-eq"},
                          {"term", {{"type", "reference"}, {"term", "col"}}},
                          {"value", 50}};
  nlohmann::json expected = {{"type", "lt-eq"}, {"term", "col"}, {"value", 50}};
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(input));
  ICEBERG_UNWRAP_OR_FAIL(auto result, ToJson(*expr));
  EXPECT_EQ(result, expected);
}

// -- Parameterized invalid expression tests --

struct InvalidExpressionParam {
  std::string name;
  nlohmann::json json;
  std::string expected_error_substr;
};

class InvalidExpressionTest : public ::testing::TestWithParam<InvalidExpressionParam> {};

TEST_P(InvalidExpressionTest, ReturnsError) {
  const auto& param = GetParam();
  auto result = ExpressionFromJson(param.json);
  EXPECT_THAT(result, HasErrorMessage(param.expected_error_substr));
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionJsonTest, InvalidExpressionTest,
    ::testing::Values(
        InvalidExpressionParam{"NotBooleanOrObject", 42, "an object with a 'type'"},
        InvalidExpressionParam{"UnknownOperationType",
                               {{"type", "illegal"}, {"term", "col"}},
                               "Unknown expression operation"},
        InvalidExpressionParam{
            "AndMissingLeft",
            {{"type", "and"}, {"right", {{"type", "is-null"}, {"term", "col"}}}},
            "missing 'left' or 'right'"},
        InvalidExpressionParam{
            "OrMissingRight",
            {{"type", "or"}, {"left", {{"type", "is-null"}, {"term", "col"}}}},
            "missing 'left' or 'right'"},
        InvalidExpressionParam{"NotMissingChild", {{"type", "not"}}, "missing 'child'"},
        InvalidExpressionParam{"UnaryWithSpuriousValue",
                               {{"type", "not-nan"}, {"term", "col"}, {"value", 42}},
                               "invalid 'value' field"},
        InvalidExpressionParam{"UnaryWithSpuriousValues",
                               {{"type", "is-nan"},
                                {"term", "col"},
                                {"values", nlohmann::json::array({1, 2})}},
                               "invalid 'values' field"},
        InvalidExpressionParam{"NumericTerm",
                               {{"type", "lt"}, {"term", 23}, {"value", 10}},
                               "Expected string for named reference"},
        InvalidExpressionParam{"SetMissingValues",
                               {{"type", "in"}, {"term", "col"}, {"value", 42}},
                               "values"},
        InvalidExpressionParam{
            "LiteralMissingValue", {{"type", "gt"}, {"term", "col"}}, "value"}),
    [](const ::testing::TestParamInfo<InvalidExpressionParam>& info) {
      return info.param.name;
    });

struct BooleanStringParam {
  std::string name;
  std::string json_value;
  Expression::Operation expected_op;
};

class BooleanStringDeserializationTest
    : public ::testing::TestWithParam<BooleanStringParam> {};

TEST_P(BooleanStringDeserializationTest, ParsesBooleanStrings) {
  const auto& param = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(nlohmann::json(param.json_value)));
  EXPECT_EQ(expr->op(), param.expected_op);
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionJsonTest, BooleanStringDeserializationTest,
    ::testing::Values(
        BooleanStringParam{"LowerTrue", "true", Expression::Operation::kTrue},
        BooleanStringParam{"LowerFalse", "false", Expression::Operation::kFalse},
        BooleanStringParam{"UpperTrue", "TRuE", Expression::Operation::kTrue}),
    [](const ::testing::TestParamInfo<BooleanStringParam>& info) {
      return info.param.name;
    });

// -- Bound predicate ToJson tests --

struct BoundPredicateToJsonParam {
  std::string name;
  std::shared_ptr<UnboundPredicate> pred;
  nlohmann::json expected_json;
};

class BoundPredicateToJsonTest
    : public ::testing::TestWithParam<BoundPredicateToJsonParam> {
 protected:
  static void SetUpTestSuite() {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeRequired(3, "age", int32()),
                                 SchemaField::MakeOptional(4, "salary", float64())},
        /*schema_id=*/0);
  }
  static std::shared_ptr<Schema> schema_;
};

std::shared_ptr<Schema> BoundPredicateToJsonTest::schema_;

TEST_P(BoundPredicateToJsonTest, ToJson) {
  const auto& param = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto bound, param.pred->Bind(*schema_, /*case_sensitive=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*bound));
  EXPECT_EQ(json, param.expected_json);
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionJsonTest, BoundPredicateToJsonTest,
    ::testing::Values(
        BoundPredicateToJsonParam{"UnaryIsNull",
                                  Expressions::IsNull("name"),
                                  {{"type", "is-null"}, {"term", "name"}}},
        BoundPredicateToJsonParam{"UnaryNotNull",
                                  Expressions::NotNull("name"),
                                  {{"type", "not-null"}, {"term", "name"}}},
        BoundPredicateToJsonParam{"UnaryIsNan",
                                  Expressions::IsNaN("salary"),
                                  {{"type", "is-nan"}, {"term", "salary"}}},
        BoundPredicateToJsonParam{"UnaryNotNan",
                                  Expressions::NotNaN("salary"),
                                  {{"type", "not-nan"}, {"term", "salary"}}},
        BoundPredicateToJsonParam{"LiteralEq",
                                  Expressions::Equal("age", Literal::Int(25)),
                                  {{"type", "eq"}, {"term", "age"}, {"value", 25}}},
        BoundPredicateToJsonParam{"LiteralLt",
                                  Expressions::LessThan("age", Literal::Int(18)),
                                  {{"type", "lt"}, {"term", "age"}, {"value", 18}}},
        BoundPredicateToJsonParam{
            "LiteralGtEq",
            Expressions::GreaterThanOrEqual("age", Literal::Int(21)),
            {{"type", "gt-eq"}, {"term", "age"}, {"value", 21}}},
        BoundPredicateToJsonParam{
            "LiteralStartsWith",
            Expressions::StartsWith("name", "prefix"),
            {{"type", "starts-with"}, {"term", "name"}, {"value", "prefix"}}},
        BoundPredicateToJsonParam{"LiteralNotEq",
                                  Expressions::NotEqual("age", Literal::Int(7)),
                                  {{"type", "not-eq"}, {"term", "age"}, {"value", 7}}}),
    [](const ::testing::TestParamInfo<BoundPredicateToJsonParam>& info) {
      return info.param.name;
    });

// -- Set operation round-trip tests --
// Tests the full cycle: bind UnboundPredicate → serialize BoundPredicate to JSON
// → deserialize to UnboundPredicate → compare op, term, and values.

struct SetOpRoundTripParam {
  std::string name;
  std::shared_ptr<UnboundPredicate> pred;
  Expression::Operation expected_op;
  std::string expected_term;
  std::vector<Literal> expected_values;
};

class SetOpRoundTripTest : public ::testing::TestWithParam<SetOpRoundTripParam> {
 protected:
  static void SetUpTestSuite() {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeRequired(3, "age", int32()),
                                 SchemaField::MakeOptional(4, "salary", float64())},
        /*schema_id=*/0);
  }
  static std::shared_ptr<Schema> schema_;
};

std::shared_ptr<Schema> SetOpRoundTripTest::schema_;

TEST_P(SetOpRoundTripTest, RoundTrip) {
  const auto& param = GetParam();

  ICEBERG_UNWRAP_OR_FAIL(auto bound, param.pred->Bind(*schema_, /*case_sensitive=*/true));
  ICEBERG_UNWRAP_OR_FAIL(auto json, ToJson(*bound));
  ICEBERG_UNWRAP_OR_FAIL(auto unbound, UnboundPredicateFromJson(json));

  EXPECT_EQ(unbound->op(), param.expected_op);
  EXPECT_EQ(unbound->reference()->name(), param.expected_term);
  std::vector<std::string> got;
  got.reserve(unbound->literals().size());
  for (const auto& lit : unbound->literals()) {
    got.push_back(lit.ToString());
  }

  std::vector<std::string> expected;
  expected.reserve(param.expected_values.size());
  for (const auto& lit : param.expected_values) {
    expected.push_back(lit.ToString());
  }

  EXPECT_THAT(got, ::testing::UnorderedElementsAreArray(expected));
}

INSTANTIATE_TEST_SUITE_P(
    ExpressionJsonTest, SetOpRoundTripTest,
    ::testing::Values(
        SetOpRoundTripParam{
            "In",
            Expressions::In("age", {Literal::Int(1), Literal::Int(2), Literal::Int(3)}),
            Expression::Operation::kIn,
            "age",
            {Literal::Int(1), Literal::Int(2), Literal::Int(3)}},
        SetOpRoundTripParam{
            "NotIn",
            Expressions::NotIn("age", {Literal::Int(5), Literal::Int(10)}),
            Expression::Operation::kNotIn,
            "age",
            {Literal::Int(5), Literal::Int(10)}}),
    [](const ::testing::TestParamInfo<SetOpRoundTripParam>& info) {
      return info.param.name;
    });

// --- LiteralFromJson(json, type) type-aware tests ---

struct LiteralFromJsonTypedParam {
  std::string name;
  nlohmann::json json;
  std::shared_ptr<Type> type;
  TypeId expected_type_id;
  std::optional<std::string> expected_str;
};

class LiteralFromJsonTypedTest
    : public ::testing::TestWithParam<LiteralFromJsonTypedParam> {};

TEST_P(LiteralFromJsonTypedTest, Parses) {
  const auto& p = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto lit, LiteralFromJson(p.json, p.type.get()));
  EXPECT_EQ(lit.type()->type_id(), p.expected_type_id);
  if (p.expected_str) {
    EXPECT_EQ(lit.ToString(), *p.expected_str);
  }
}

INSTANTIATE_TEST_SUITE_P(
    LiteralFromJsonTyped, LiteralFromJsonTypedTest,
    ::testing::Values(
        LiteralFromJsonTypedParam{"Boolean", nlohmann::json(true), boolean(),
                                  TypeId::kBoolean, "true"},
        LiteralFromJsonTypedParam{"Int", nlohmann::json(123), int32(), TypeId::kInt,
                                  "123"},
        LiteralFromJsonTypedParam{"Long", nlohmann::json(9876543210LL), int64(),
                                  TypeId::kLong, "9876543210"},
        LiteralFromJsonTypedParam{"Float", nlohmann::json(1.5), float32(), TypeId::kFloat,
                                  std::nullopt},
        LiteralFromJsonTypedParam{"Double", nlohmann::json(3.14), float64(),
                                  TypeId::kDouble, std::nullopt},
        LiteralFromJsonTypedParam{"String", nlohmann::json("hello"), string(),
                                  TypeId::kString, std::nullopt},
        LiteralFromJsonTypedParam{"DateString", nlohmann::json("2024-01-15"), date(),
                                  TypeId::kDate, std::nullopt},
        LiteralFromJsonTypedParam{"Uuid",
                                  nlohmann::json("f79c3e09-677c-4bbd-a479-3f349cb785e7"),
                                  uuid(), TypeId::kUuid, std::nullopt},
        LiteralFromJsonTypedParam{"Binary", nlohmann::json("deadbeef"), binary(),
                                  TypeId::kBinary, std::nullopt},
        LiteralFromJsonTypedParam{"Fixed", nlohmann::json("cafebabe"), fixed(4),
                                  TypeId::kFixed, std::nullopt},
        LiteralFromJsonTypedParam{"DecimalMatchingScale", nlohmann::json("123.4500"),
                                  decimal(9, 4), TypeId::kDecimal, "123.4500"},
        LiteralFromJsonTypedParam{"DecimalScaleZero", nlohmann::json("2"), decimal(9, 0),
                                  TypeId::kDecimal, "2"}),
    [](const ::testing::TestParamInfo<LiteralFromJsonTypedParam>& info) {
      return info.param.name;
    });

struct InvalidLiteralFromJsonTypedParam {
  std::string name;
  nlohmann::json json;
  std::shared_ptr<Type> type;
};

class InvalidLiteralFromJsonTypedTest
    : public ::testing::TestWithParam<InvalidLiteralFromJsonTypedParam> {};

TEST_P(InvalidLiteralFromJsonTypedTest, ReturnsError) {
  const auto& p = GetParam();
  EXPECT_FALSE(LiteralFromJson(p.json, p.type.get()).has_value());
}

INSTANTIATE_TEST_SUITE_P(
    LiteralFromJsonTyped, InvalidLiteralFromJsonTypedTest,
    ::testing::Values(
        InvalidLiteralFromJsonTypedParam{"BooleanTypeMismatch", nlohmann::json(42),
                                         boolean()},
        InvalidLiteralFromJsonTypedParam{"DateTypeMismatch", nlohmann::json(true),
                                         date()},
        InvalidLiteralFromJsonTypedParam{"UuidTypeMismatch", nlohmann::json(42), uuid()},
        InvalidLiteralFromJsonTypedParam{"BinaryInvalidHex", nlohmann::json("xyz"),
                                         binary()},
        InvalidLiteralFromJsonTypedParam{"FixedLengthMismatch", nlohmann::json("cafe12"),
                                         fixed(4)},
        InvalidLiteralFromJsonTypedParam{"DecimalScaleMismatch", nlohmann::json("123.45"),
                                         decimal(9, 4)},
        InvalidLiteralFromJsonTypedParam{"DecimalNotString", nlohmann::json(123.45),
                                         decimal(9, 2)}),
    [](const ::testing::TestParamInfo<InvalidLiteralFromJsonTypedParam>& info) {
      return info.param.name;
    });

struct SchemaAwarePredicateParam {
  std::string name;
  std::string field_name;
  std::shared_ptr<Type> field_type;
  nlohmann::json value;
};

class SchemaAwarePredicateRoundTripTest
    : public ::testing::TestWithParam<SchemaAwarePredicateParam> {};

TEST_P(SchemaAwarePredicateRoundTripTest, RoundTrip) {
  const auto& p = GetParam();
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, p.field_name, p.field_type)});
  nlohmann::json pred_json = {{"type", "eq"}, {"term", p.field_name}, {"value", p.value}};
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(pred_json, schema.get()));
  ASSERT_NE(expr, nullptr);
}

INSTANTIATE_TEST_SUITE_P(
    LiteralFromJsonTyped, SchemaAwarePredicateRoundTripTest,
    ::testing::Values(
        SchemaAwarePredicateParam{"Date", "event_date", date(), "2024-01-15"},
        SchemaAwarePredicateParam{"Time", "event_time", time(), "14:30:00"},
        SchemaAwarePredicateParam{"Timestamp", "created_at", timestamp(),
                                  "2026-01-01T00:00:01.500"},
        SchemaAwarePredicateParam{"TimestampTz", "updated_at", timestamp_tz(),
                                  "2026-01-01T00:00:01.500+00:00"},
        SchemaAwarePredicateParam{"Uuid", "trace_id", uuid(),
                                  "f79c3e09-677c-4bbd-a479-3f349cb785e7"},
        SchemaAwarePredicateParam{"Binary", "payload", binary(), "deadbeef"},
        SchemaAwarePredicateParam{"Fixed", "hash", fixed(4), "cafebabe"},
        SchemaAwarePredicateParam{"Decimal", "amount", decimal(9, 2), "123.45"}),
    [](const ::testing::TestParamInfo<SchemaAwarePredicateParam>& info) {
      return info.param.name;
    });

}  // namespace iceberg
