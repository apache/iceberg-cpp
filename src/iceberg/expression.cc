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

#include "iceberg/expression.h"

#include <unordered_map>

namespace iceberg {

expected<Expression::Operation, Error> Expression::FromString(
    const std::string& operation_type) {
  std::string lowercase_op = operation_type;
  std::transform(lowercase_op.begin(), lowercase_op.end(), lowercase_op.begin(),
                 [](unsigned char c) { return std::tolower(c); });

  static const std::unordered_map<std::string, Operation> op_map = {
      {"true", Operation::kTrue},
      {"false", Operation::kFalse},
      {"is_null", Operation::kIsNull},
      {"not_null", Operation::kNotNull},
      {"is_nan", Operation::kIsNan},
      {"not_nan", Operation::kNotNan},
      {"lt", Operation::kLt},
      {"lt_eq", Operation::kLtEq},
      {"gt", Operation::kGt},
      {"gt_eq", Operation::kGtEq},
      {"eq", Operation::kEq},
      {"not_eq", Operation::kNotEq},
      {"in", Operation::kIn},
      {"not_in", Operation::kNotIn},
      {"not", Operation::kNot},
      {"and", Operation::kAnd},
      {"or", Operation::kOr},
      {"starts_with", Operation::kStartsWith},
      {"not_starts_with", Operation::kNotStartsWith},
      {"count", Operation::kCount},
      {"count_star", Operation::kCountStar},
      {"max", Operation::kMax},
      {"min", Operation::kMin}};

  auto it = op_map.find(lowercase_op);
  if (it == op_map.end()) {
    return unexpected<Error>(
        {ErrorKind::kInvalidOperatorType, "Unknown operation type: " + operation_type});
  }
  return it->second;
}

expected<Expression::Operation, Error> Expression::Negate(Operation op) {
  switch (op) {
    case Operation::kTrue:
      return Operation::kFalse;
    case Operation::kFalse:
      return Operation::kTrue;
    case Operation::kIsNull:
      return Operation::kNotNull;
    case Operation::kNotNull:
      return Operation::kIsNull;
    case Operation::kIsNan:
      return Operation::kNotNan;
    case Operation::kNotNan:
      return Operation::kIsNan;
    case Operation::kLt:
      return Operation::kGtEq;
    case Operation::kLtEq:
      return Operation::kGt;
    case Operation::kGt:
      return Operation::kLtEq;
    case Operation::kGtEq:
      return Operation::kLt;
    case Operation::kEq:
      return Operation::kNotEq;
    case Operation::kNotEq:
      return Operation::kEq;
    case Operation::kIn:
      return Operation::kNotIn;
    case Operation::kNotIn:
      return Operation::kIn;
    case Operation::kStartsWith:
      return Operation::kNotStartsWith;
    case Operation::kNotStartsWith:
      return Operation::kStartsWith;
    default:
      return unexpected<Error>(
          {ErrorKind::kInvalidOperatorType, "No negation defined for operation"});
  }
}

expected<Expression::Operation, Error> Expression::FlipLR(Operation op) {
  switch (op) {
    case Operation::kLt:
      return Operation::kGt;
    case Operation::kLtEq:
      return Operation::kGtEq;
    case Operation::kGt:
      return Operation::kLt;
    case Operation::kGtEq:
      return Operation::kLtEq;
    case Operation::kEq:
      return Operation::kEq;
    case Operation::kNotEq:
      return Operation::kNotEq;
    case Operation::kAnd:
      return Operation::kAnd;
    case Operation::kOr:
      return Operation::kOr;
    default:
      return unexpected<Error>(
          {ErrorKind::kInvalidOperatorType, "No left-right flip for operation"});
  }
}

}  // namespace iceberg