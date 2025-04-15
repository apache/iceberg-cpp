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

#include "expression.h"

#include <format>

namespace iceberg {

// True implementation
const std::shared_ptr<True>& True::Instance() {
  static const std::shared_ptr<True> instance{new True()};
  return instance;
}

Result<std::shared_ptr<Expression>> True::Negate() const { return False::Instance(); }

// False implementation
const std::shared_ptr<False>& False::Instance() {
  static const std::shared_ptr<False> instance = std::shared_ptr<False>(new False());
  return instance;
}

Result<std::shared_ptr<Expression>> False::Negate() const { return True::Instance(); }

// And implementation
And::And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right)
    : left_(std::move(left)), right_(std::move(right)) {}

std::string And::ToString() const {
  return std::format("({} and {})", left_->ToString(), right_->ToString());
}

Result<std::shared_ptr<Expression>> And::Negate() const {
  // TODO(yingcai-cy): Implement Or expression
  return unexpected(
      Error(ErrorKind::kInvalidExpression, "And negation not yet implemented"));
}

bool And::Equals(const Expression& expr) const {
  if (expr.op() == Operation::kAnd) {
    const auto& other = static_cast<const And&>(expr);
    return (left_->Equals(*other.left()) && right_->Equals(*other.right())) ||
           (left_->Equals(*other.right()) && right_->Equals(*other.left()));
  }
  return false;
}

}  // namespace iceberg
