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

#include "iceberg/expression/binder.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/manifest_list.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

class ManifestEvalVisitor : public BoundVisitor<bool> {
 public:
  explicit ManifestEvalVisitor(const ManifestFile& manifest)
      : manifest_(manifest), stats_(manifest.partitions) {}

  Result<bool> AlwaysTrue() override { return true; }

  Result<bool> AlwaysFalse() override { return false; }

  Result<bool> Not(bool child_result) override { return !child_result; }

  Result<bool> And(bool left_result, bool right_result) override {
    return left_result && right_result;
  }

  Result<bool> Or(bool left_result, bool right_result) override {
    return left_result || right_result;
  }

  Result<bool> IsNull(const std::shared_ptr<BoundTerm>& term) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value.IsNull();
  }

  Result<bool> NotNull(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNull(term));
    return !value;
  }

  Result<bool> IsNaN(const std::shared_ptr<BoundTerm>& term) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value.IsNaN();
  }

  Result<bool> NotNaN(const std::shared_ptr<BoundTerm>& term) override {
    ICEBERG_ASSIGN_OR_RAISE(auto value, IsNaN(term));
    return !value;
  }

  Result<bool> Lt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value < lit;
  }

  Result<bool> LtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value <= lit;
  }

  Result<bool> Gt(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value > lit;
  }

  Result<bool> GtEq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value >= lit;
  }

  Result<bool> Eq(const std::shared_ptr<BoundTerm>& term, const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return value == lit;
  }

  Result<bool> NotEq(const std::shared_ptr<BoundTerm>& term,
                     const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto eq_result, Eq(term, lit));
    return !eq_result;
  }

  Result<bool> In(const std::shared_ptr<BoundTerm>& term,
                  const BoundSetPredicate::LiteralSet& literal_set) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //    return literal_set.contains(value);
  }

  Result<bool> NotIn(const std::shared_ptr<BoundTerm>& term,
                     const BoundSetPredicate::LiteralSet& literal_set) override {
    ICEBERG_ASSIGN_OR_RAISE(auto in_result, In(term, literal_set));
    return !in_result;
  }

  Result<bool> StartsWith(const std::shared_ptr<BoundTerm>& term,
                          const Literal& lit) override {
    // TODO(xiao.dong) need a wrapper like PartitionStructLike
    return NotImplemented("NotImplemented");
    //
    //    ICEBERG_ASSIGN_OR_RAISE(auto value, term->Evaluate(manifest_));
    //
    //    // Both value and literal should be strings
    //    if (!std::holds_alternative<std::string>(value.value()) ||
    //        !std::holds_alternative<std::string>(lit.value())) {
    //      return false;
    //    }
    //
    //    const auto& str_value = std::get<std::string>(value.value());
    //    const auto& str_prefix = std::get<std::string>(lit.value());
    //    return str_value.starts_with(str_prefix);
  }

  Result<bool> NotStartsWith(const std::shared_ptr<BoundTerm>& term,
                             const Literal& lit) override {
    ICEBERG_ASSIGN_OR_RAISE(auto starts_result, StartsWith(term, lit));
    return !starts_result;
  }

 private:
  const ManifestFile& manifest_;
  const std::vector<PartitionFieldSummary>& stats_;
};

ManifestEvaluator::ManifestEvaluator(const std::shared_ptr<Expression>& expr)
    : expr_(std::move(expr)) {}

ManifestEvaluator::~ManifestEvaluator() = default;

Result<std::unique_ptr<ManifestEvaluator>> ManifestEvaluator::MakeRowFilter(
    [[maybe_unused]] std::shared_ptr<Expression> expr,
    [[maybe_unused]] const std::shared_ptr<PartitionSpec> spec,
    [[maybe_unused]] const Schema& schema, [[maybe_unused]] bool case_sensitive) {
  // TODO(xiao.dong) we need a projection util to project row filter to the partition col
  return NotImplemented("ManifestEvaluator::MakeRowFilter");
}

Result<std::unique_ptr<ManifestEvaluator>> ManifestEvaluator::MakePartitionFilter(
    std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec> spec,
    const Schema& schema, bool case_sensitive) {
  ICEBERG_ASSIGN_OR_RAISE(auto partition_type, spec->PartitionType(schema));
  auto field_span = partition_type->fields();
  std::vector<SchemaField> fields(field_span.begin(), field_span.end());
  auto partition_schema = std::make_shared<Schema>(fields);
  ICEBERG_ASSIGN_OR_RAISE(auto rewrite_expr, RewriteNot::Visit(std::move(expr)));
  ICEBERG_ASSIGN_OR_RAISE(auto partition_expr,
                          Binder::Bind(*partition_schema, rewrite_expr, case_sensitive));
  return std::unique_ptr<ManifestEvaluator>(
      new ManifestEvaluator(std::move(partition_expr)));
}

Result<bool> ManifestEvaluator::Eval(const ManifestFile& manifest) const {
  ManifestEvalVisitor visitor(manifest);
  return Visit<bool, ManifestEvalVisitor>(expr_, visitor);
}

}  // namespace iceberg
