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

#include "iceberg/expression/predicate.h"

#include "iceberg/exception.h"
#include "iceberg/expression/literal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// Predicate template implementations
template <typename TermType>
Predicate<TermType>::Predicate(Expression::Operation op, std::shared_ptr<TermType> term)
    : operation_(op), term_(std::move(term)) {}

// UnboundPredicate template implementations
template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term)
    : Base(op, std::move(term)) {}

template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term, Literal value)
    : Base(op, std::move(term)), values_{std::move(value)} {}

template <typename B>
UnboundPredicate<B>::UnboundPredicate(Expression::Operation op,
                                      std::shared_ptr<UnboundTerm<B>> term,
                                      std::vector<Literal> values)
    : Base(op, std::move(term)), values_(std::move(values)) {}

template <typename B>
std::string UnboundPredicate<B>::ToString() const {
  throw IcebergError("UnboundPredicate::ToString not implemented");
}

template <typename B>
Result<std::unique_ptr<Expression>> UnboundPredicate<B>::Bind(const Schema& schema,
                                                              bool case_sensitive) const {
  throw IcebergError("UnboundPredicate::Bind not implemented");
}

// BoundPredicate implementation
BoundPredicate::BoundPredicate(Expression::Operation op, std::shared_ptr<BoundTerm> term)
    : Predicate<BoundTerm>(op, std::move(term)) {}

Result<Literal::Value> BoundPredicate::Evaluate(const StructLike& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto eval_result, term_->Evaluate(data));
  ICEBERG_ASSIGN_OR_RAISE(auto test_result, Test(eval_result));
  return Literal::Value{test_result};
}

Result<std::vector<Literal::Value>> BoundPredicate::Evaluate(
    const ArrowArray& data) const {
  ICEBERG_ASSIGN_OR_RAISE(auto eval_result, term_->Evaluate(data));
  std::vector<Literal::Value> result;
  result.reserve(eval_result.size());
  for (const auto& value : eval_result) {
    ICEBERG_ASSIGN_OR_RAISE(auto test_result, Test(value));
    result.emplace_back(test_result);
  }
  return result;
}

std::string BoundPredicate::ToString() const {
  throw IcebergError("BoundPredicate::ToString not implemented");
}

// BoundUnaryPredicate implementation
BoundUnaryPredicate::BoundUnaryPredicate(Expression::Operation op,
                                         std::shared_ptr<BoundTerm> term)
    : BoundPredicate(op, std::move(term)) {}

Result<bool> BoundUnaryPredicate::Test(const Literal::Value& value) const {
  throw IcebergError("BoundUnaryPredicate::Test not implemented");
}

// BoundLiteralPredicate implementation
BoundLiteralPredicate::BoundLiteralPredicate(Expression::Operation op,
                                             std::shared_ptr<BoundTerm> term,
                                             Literal literal)
    : BoundPredicate(op, std::move(term)), literal_(std::move(literal)) {}

Result<bool> BoundLiteralPredicate::Test(const Literal::Value& value) const {
  throw IcebergError("BoundLiteralPredicate::Test not implemented");
}

std::string BoundLiteralPredicate::ToString() const {
  throw IcebergError("BoundLiteralPredicate::ToString not implemented");
}

// BoundSetPredicate implementation
BoundSetPredicate::BoundSetPredicate(Expression::Operation op,
                                     std::shared_ptr<BoundTerm> term,
                                     std::span<const Literal> literals)
    : BoundPredicate(op, std::move(term)) {
  for (const auto& literal : literals) {
    value_set_.push_back(literal.value());
  }
}

Result<bool> BoundSetPredicate::Test(const Literal::Value& value) const {
  throw IcebergError("BoundSetPredicate::Test not implemented");
}

std::string BoundSetPredicate::ToString() const {
  throw IcebergError("BoundSetPredicate::ToString not implemented");
}

// Explicit template instantiations
template class Predicate<UnboundTerm<BoundReference>>;
template class Predicate<UnboundTerm<BoundTransform>>;
template class Predicate<BoundTerm>;

template class UnboundPredicate<BoundReference>;
template class UnboundPredicate<BoundTransform>;

}  // namespace iceberg
