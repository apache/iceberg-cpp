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

#include "iceberg/expression/json_internal.h"

#include <memory>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#include "iceberg/expression/expression.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/expression/predicate.h"
#include "iceberg/expression/term.h"
#include "iceberg/test/matchers.h"

namespace iceberg {

class ExpressionJsonTest : public ::testing::Test {
 protected:
  // Helper to test round-trip serialization
  // Uses string comparison since expressions may have different internal identity
  // but the same semantic meaning (i.e., ToString() output matches)
  void TestRoundTrip(const Expression& expr) {
    auto json = ToJson(expr);
    auto result = ExpressionFromJson(json);
    ASSERT_THAT(result, IsOk()) << "Failed to parse JSON: " << json.dump();
    EXPECT_EQ(expr.ToString(), result.value()->ToString())
        << "Round-trip failed.\nJSON: " << json.dump();
  }
};

// Test boolean constant expressions
TEST_F(ExpressionJsonTest, TrueExpression) {
  auto expr = True::Instance();
  auto json = ToJson(*expr);

  // True should serialize as JSON boolean true
  EXPECT_TRUE(json.is_boolean());
  EXPECT_TRUE(json.get<bool>());

  // Parse back
  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->op(), Expression::Operation::kTrue);
}

TEST_F(ExpressionJsonTest, FalseExpression) {
  auto expr = False::Instance();
  auto json = ToJson(*expr);

  // False should serialize as JSON boolean false
  EXPECT_TRUE(json.is_boolean());
  EXPECT_FALSE(json.get<bool>());

  // Parse back
  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(result.value()->op(), Expression::Operation::kFalse);
}

// Test And expression
TEST_F(ExpressionJsonTest, AndExpression) {
  auto left = Expressions::GreaterThanOrEqual("col1", Literal::Int(50));
  auto right = Expressions::LessThan("col2", Literal::Int(100));
  auto expr = Expressions::And(left, right);

  auto json = ToJson(*expr);

  // Verify JSON structure
  EXPECT_EQ(json["type"], "and");
  EXPECT_TRUE(json.contains("left"));
  EXPECT_TRUE(json.contains("right"));
  EXPECT_EQ(json["left"]["type"], "gt-eq");
  EXPECT_EQ(json["right"]["type"], "lt");

  // Round-trip test
  TestRoundTrip(*expr);
}

// Test Or expression
TEST_F(ExpressionJsonTest, OrExpression) {
  auto left = Expressions::Equal("status", Literal::String("active"));
  auto right = Expressions::Equal("status", Literal::String("pending"));
  auto expr = Expressions::Or(left, right);

  auto json = ToJson(*expr);

  // Verify JSON structure
  EXPECT_EQ(json["type"], "or");
  EXPECT_EQ(json["left"]["type"], "eq");
  EXPECT_EQ(json["right"]["type"], "eq");

  // Round-trip test
  TestRoundTrip(*expr);
}

// Test Not expression
TEST_F(ExpressionJsonTest, NotExpression) {
  auto child = Expressions::IsNull("col");
  auto expr = Expressions::Not(child);

  auto json = ToJson(*expr);

  // Verify JSON structure
  EXPECT_EQ(json["type"], "not");
  EXPECT_TRUE(json.contains("child"));
  EXPECT_EQ(json["child"]["type"], "is-null");

  // Round-trip test
  TestRoundTrip(*expr);
}

// Test unary predicates
TEST_F(ExpressionJsonTest, IsNullPredicate) {
  auto expr = Expressions::IsNull("column_name");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "is-null");
  EXPECT_EQ(json["term"], "column_name");
  EXPECT_FALSE(json.contains("value"));
  EXPECT_FALSE(json.contains("values"));

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, NotNullPredicate) {
  auto expr = Expressions::NotNull("column_name");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "not-null");
  EXPECT_EQ(json["term"], "column_name");

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, IsNanPredicate) {
  auto expr = Expressions::IsNaN("float_col");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "is-nan");
  EXPECT_EQ(json["term"], "float_col");

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, NotNanPredicate) {
  auto expr = Expressions::NotNaN("float_col");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "not-nan");
  EXPECT_EQ(json["term"], "float_col");

  TestRoundTrip(*expr);
}

// Test comparison predicates
TEST_F(ExpressionJsonTest, EqualPredicate) {
  auto expr = Expressions::Equal("name", Literal::String("test"));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "eq");
  EXPECT_EQ(json["term"], "name");
  EXPECT_EQ(json["value"], "test");

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, NotEqualPredicate) {
  auto expr = Expressions::NotEqual("count", Literal::Int(0));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "not-eq");
  EXPECT_EQ(json["term"], "count");
  EXPECT_EQ(json["value"], 0);

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, LessThanPredicate) {
  auto expr = Expressions::LessThan("age", Literal::Int(18));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "lt");
  EXPECT_EQ(json["term"], "age");
  EXPECT_EQ(json["value"], 18);

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, LessThanOrEqualPredicate) {
  auto expr = Expressions::LessThanOrEqual("score", Literal::Double(99.5));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "lt-eq");
  EXPECT_EQ(json["term"], "score");
  EXPECT_DOUBLE_EQ(json["value"].get<double>(), 99.5);

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, GreaterThanPredicate) {
  auto expr = Expressions::GreaterThan("price", Literal::Long(1000));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "gt");
  EXPECT_EQ(json["term"], "price");
  EXPECT_EQ(json["value"], 1000);

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, GreaterThanOrEqualPredicate) {
  auto expr = Expressions::GreaterThanOrEqual("quantity", Literal::Int(1));

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "gt-eq");
  EXPECT_EQ(json["term"], "quantity");
  EXPECT_EQ(json["value"], 1);

  TestRoundTrip(*expr);
}

// Test string predicates
TEST_F(ExpressionJsonTest, StartsWithPredicate) {
  auto expr = Expressions::StartsWith("path", "/home/user");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "starts-with");
  EXPECT_EQ(json["term"], "path");
  EXPECT_EQ(json["value"], "/home/user");

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, NotStartsWithPredicate) {
  auto expr = Expressions::NotStartsWith("path", "/tmp");

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "not-starts-with");
  EXPECT_EQ(json["term"], "path");
  EXPECT_EQ(json["value"], "/tmp");

  TestRoundTrip(*expr);
}

// Test set predicates
TEST_F(ExpressionJsonTest, InPredicate) {
  auto expr = Expressions::In("status",
                              {Literal::String("active"), Literal::String("pending"),
                               Literal::String("review")});

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "in");
  EXPECT_EQ(json["term"], "status");
  EXPECT_TRUE(json.contains("values"));
  EXPECT_TRUE(json["values"].is_array());
  EXPECT_EQ(json["values"].size(), 3);

  TestRoundTrip(*expr);
}

TEST_F(ExpressionJsonTest, NotInPredicate) {
  auto expr = Expressions::NotIn("id", {Literal::Int(1), Literal::Int(2), Literal::Int(3)});

  auto json = ToJson(*expr);

  EXPECT_EQ(json["type"], "not-in");
  EXPECT_EQ(json["term"], "id");
  EXPECT_TRUE(json["values"].is_array());
  EXPECT_EQ(json["values"].size(), 3);

  TestRoundTrip(*expr);
}

// Test nested expressions
TEST_F(ExpressionJsonTest, NestedAndOr) {
  auto cond1 = Expressions::Equal("a", Literal::Int(1));
  auto cond2 = Expressions::Equal("b", Literal::Int(2));
  auto cond3 = Expressions::Equal("c", Literal::Int(3));

  auto or_expr = Expressions::Or(cond1, cond2);
  auto and_expr = Expressions::And(or_expr, cond3);

  auto json = ToJson(*and_expr);

  EXPECT_EQ(json["type"], "and");
  EXPECT_EQ(json["left"]["type"], "or");
  EXPECT_EQ(json["right"]["type"], "eq");

  TestRoundTrip(*and_expr);
}

// Test deserialization from JSON strings (matching Java format)
TEST_F(ExpressionJsonTest, ParseAndExpression) {
  nlohmann::json json = R"({
    "type": "and",
    "left": {
      "type": "gt-eq",
      "term": "column-name-1",
      "value": 50
    },
    "right": {
      "type": "in",
      "term": "column-name-2",
      "values": ["one", "two"]
    }
  })"_json;

  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());

  auto expr = result.value();
  EXPECT_EQ(expr->op(), Expression::Operation::kAnd);

  const auto& and_expr = static_cast<const And&>(*expr);
  EXPECT_EQ(and_expr.left()->op(), Expression::Operation::kGtEq);
  EXPECT_EQ(and_expr.right()->op(), Expression::Operation::kIn);
}

TEST_F(ExpressionJsonTest, ParseOrExpression) {
  nlohmann::json json = R"({
    "type": "or",
    "left": {
      "type": "lt",
      "term": "column-name-1",
      "value": 50
    },
    "right": {
      "type": "not-null",
      "term": "column-name-2"
    }
  })"_json;

  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());

  auto expr = result.value();
  EXPECT_EQ(expr->op(), Expression::Operation::kOr);
}

TEST_F(ExpressionJsonTest, ParseNotExpression) {
  nlohmann::json json = R"({
    "type": "not",
    "child": {
      "type": "gt-eq",
      "term": "column-name-1",
      "value": 50
    }
  })"_json;

  auto result = ExpressionFromJson(json);
  ASSERT_THAT(result, IsOk());

  auto expr = result.value();
  EXPECT_EQ(expr->op(), Expression::Operation::kNot);
}

// Test literal serialization
TEST_F(ExpressionJsonTest, LiteralSerialization) {
  // Boolean
  auto bool_json = LiteralToJson(Literal::Boolean(true));
  EXPECT_TRUE(bool_json.is_boolean());
  EXPECT_TRUE(bool_json.get<bool>());

  // Integer
  auto int_json = LiteralToJson(Literal::Int(42));
  EXPECT_TRUE(int_json.is_number_integer());
  EXPECT_EQ(int_json.get<int32_t>(), 42);

  // Long
  auto long_json = LiteralToJson(Literal::Long(9876543210L));
  EXPECT_TRUE(long_json.is_number_integer());
  EXPECT_EQ(long_json.get<int64_t>(), 9876543210L);

  // Float
  auto float_json = LiteralToJson(Literal::Float(3.14f));
  EXPECT_TRUE(float_json.is_number_float());

  // Double
  auto double_json = LiteralToJson(Literal::Double(2.718281828));
  EXPECT_TRUE(double_json.is_number_float());

  // String
  auto string_json = LiteralToJson(Literal::String("hello"));
  EXPECT_TRUE(string_json.is_string());
  EXPECT_EQ(string_json.get<std::string>(), "hello");
}

// Test operation to JSON type conversion
TEST_F(ExpressionJsonTest, OperationToJsonType) {
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kTrue), "true");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kFalse), "false");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kAnd), "and");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kOr), "or");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNot), "not");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kEq), "eq");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNotEq), "not-eq");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kLt), "lt");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kLtEq), "lt-eq");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kGt), "gt");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kGtEq), "gt-eq");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kIn), "in");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNotIn), "not-in");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kIsNull), "is-null");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNotNull), "not-null");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kIsNan), "is-nan");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNotNan), "not-nan");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kStartsWith), "starts-with");
  EXPECT_EQ(OperationToJsonType(Expression::Operation::kNotStartsWith), "not-starts-with");
}

// Test JSON type to operation conversion
TEST_F(ExpressionJsonTest, OperationFromJsonType) {
  // Helper to test operation conversion
  auto test_op = [](std::string_view type_str, Expression::Operation expected) {
    auto result = OperationFromJsonType(type_str);
    ASSERT_THAT(result, IsOk()) << "Failed to parse: " << type_str;
    EXPECT_EQ(result.value(), expected) << "Mismatch for: " << type_str;
  };

  test_op("true", Expression::Operation::kTrue);
  test_op("false", Expression::Operation::kFalse);
  test_op("and", Expression::Operation::kAnd);
  test_op("or", Expression::Operation::kOr);
  test_op("not", Expression::Operation::kNot);
  test_op("eq", Expression::Operation::kEq);
  test_op("not-eq", Expression::Operation::kNotEq);
  test_op("lt", Expression::Operation::kLt);
  test_op("lt-eq", Expression::Operation::kLtEq);
  test_op("gt", Expression::Operation::kGt);
  test_op("gt-eq", Expression::Operation::kGtEq);
  test_op("in", Expression::Operation::kIn);
  test_op("not-in", Expression::Operation::kNotIn);
  test_op("is-null", Expression::Operation::kIsNull);
  test_op("not-null", Expression::Operation::kNotNull);
  test_op("is-nan", Expression::Operation::kIsNan);
  test_op("not-nan", Expression::Operation::kNotNan);
  test_op("starts-with", Expression::Operation::kStartsWith);
  test_op("not-starts-with", Expression::Operation::kNotStartsWith);

  // Unknown type should fail
  auto result = OperationFromJsonType("unknown-type");
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

// Test error cases
TEST_F(ExpressionJsonTest, InvalidJsonType) {
  nlohmann::json json = R"({"type": "invalid-op", "term": "col"})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingTypeField) {
  nlohmann::json json = R"({"term": "col", "value": 42})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingTermField) {
  nlohmann::json json = R"({"type": "eq", "value": 42})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingValueForLiteralPredicate) {
  nlohmann::json json = R"({"type": "eq", "term": "col"})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingValuesForSetPredicate) {
  nlohmann::json json = R"({"type": "in", "term": "col"})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingLeftForAnd) {
  nlohmann::json json = R"({"type": "and", "right": true})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

TEST_F(ExpressionJsonTest, MissingChildForNot) {
  nlohmann::json json = R"({"type": "not"})"_json;
  auto result = ExpressionFromJson(json);
  EXPECT_THAT(result, IsError(ErrorKind::kJsonParseError));
}

}  // namespace iceberg
