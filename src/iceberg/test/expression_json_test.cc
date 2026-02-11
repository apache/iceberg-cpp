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
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/json_serde_internal.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/term.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"
#include "iceberg/util/uuid.h"

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
  auto round_trip = ToJson(*expr);
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
  EXPECT_EQ(ToJson(*expr), expected);
}

TEST(ExpressionJsonTest, PredicateWithObjectReference) {
  nlohmann::json input = {{"type", "lt-eq"},
                          {"term", {{"type", "reference"}, {"term", "col"}}},
                          {"value", 50}};
  nlohmann::json expected = {{"type", "lt-eq"}, {"term", "col"}, {"value", 50}};
  ICEBERG_UNWRAP_OR_FAIL(auto expr, ExpressionFromJson(input));
  EXPECT_EQ(ToJson(*expr), expected);
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
        InvalidExpressionParam{"NotBooleanOrObject", 42, "boolean or object"},
        InvalidExpressionParam{"UnknownOperationType",
                               {{"type", "illegal"}, {"term", "col"}},
                               "Unknown expression type"},
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

}  // namespace iceberg
