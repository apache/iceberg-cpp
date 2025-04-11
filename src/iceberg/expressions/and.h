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

#pragma once

#include <format>
#include <memory>
#include <string>

#include "iceberg/expression.h"

namespace iceberg {

/// \brief An Expression that represents a logical AND operation between two expressions.
///
/// This expression evaluates to true if and only if both of its child expressions
/// evaluate to true.
class ICEBERG_EXPORT And : public Expression {
 public:
  /// \brief Constructs an And expression from two sub-expressions.
  ///
  /// @param left The left operand of the AND expression
  /// @param right The right operand of the AND expression
  And(std::shared_ptr<Expression> left, std::shared_ptr<Expression> right);

  /// \brief Returns the left operand of the AND expression.
  ///
  /// @return The left operand of the AND expression
  const std::shared_ptr<Expression>& left() const { return left_; }

  /// \brief Returns the right operand of the AND expression.
  ///
  /// @return The right operand of the AND expression
  const std::shared_ptr<Expression>& right() const { return right_; }

  Operation Op() const override { return Operation::kAnd; }

  std::string ToString() const override {
    return std::format("({} and {})", left_->ToString(), right_->ToString());
  }

  // implement Negate later
  // expected<std::shared_ptr<Expression>, Error> Negate() const override;

  bool IsEquivalentTo(const Expression& other) const override;

 private:
  std::shared_ptr<Expression> left_;
  std::shared_ptr<Expression> right_;
};

}  // namespace iceberg
