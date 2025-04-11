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

#include <memory>
#include <string>

#include "iceberg/expression.h"

namespace iceberg {

/// \brief An Expression that is always true.
///
/// Represents a boolean predicate that always evaluates to true.
class ICEBERG_EXPORT True : public Expression {
 public:
  static const True& instance() {
    static True instance;
    return instance;
  }

  static const std::shared_ptr<Expression>& shared_instance() {
    static std::shared_ptr<Expression> instance = std::shared_ptr<Expression>(
        const_cast<True*>(&True::instance()), [](Expression*) {});
    return instance;
  }

  Operation Op() const override { return Operation::kTrue; }

  std::string ToString() const override { return "true"; }

  Result<std::shared_ptr<Expression>> Negate() const override;

  bool IsEquivalentTo(const Expression& other) const override {
    return other.Op() == Operation::kTrue;
  }

 private:
  constexpr True() = default;
};

}  // namespace iceberg
