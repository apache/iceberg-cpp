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

#include "iceberg/expression/term.h"

#include <format>

#include "iceberg/exception.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/transform.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Bound::~Bound() = default;

BoundTerm::~BoundTerm() = default;

Reference::~Reference() = default;

template <typename B>
Result<std::shared_ptr<B>> Unbound<B>::Bind(const Schema& schema) const {
  return Bind(schema, /*case_sensitive=*/true);
}

// NamedReference implementation
Result<std::unique_ptr<NamedReference>> NamedReference::Make(std::string field_name) {
  if (field_name.empty()) [[unlikely]] {
    return InvalidExpression("NamedReference cannot have empty field name");
  }
  return std::unique_ptr<NamedReference>(new NamedReference(std::move(field_name)));
}

NamedReference::NamedReference(std::string field_name)
    : field_name_(std::move(field_name)) {
  ICEBERG_DCHECK(!field_name_.empty(), "NamedReference cannot have empty field name");
}

NamedReference::~NamedReference() = default;

Result<std::shared_ptr<BoundReference>> NamedReference::Bind(const Schema& schema,
                                                             bool case_sensitive) const {
  ICEBERG_ASSIGN_OR_RAISE(auto field_opt,
                          schema.GetFieldByName(field_name_, case_sensitive));
  if (!field_opt.has_value()) [[unlikely]] {
    return InvalidExpression("Cannot find field '{}' in struct: {}", field_name_,
                             schema.ToString());
  }
  return BoundReference::Make(field_opt.value().get());
}

std::string NamedReference::ToString() const {
  return std::format("ref(name=\"{}\")", field_name_);
}

// BoundReference implementation
Result<std::unique_ptr<BoundReference>> BoundReference::Make(SchemaField field) {
  if (auto status = field.Validate(); !status.has_value()) [[unlikely]] {
    return InvalidExpression("Cannot create BoundReference with invalid field: {}",
                             status.error().message);
  }
  return std::unique_ptr<BoundReference>(new BoundReference(std::move(field)));
}

BoundReference::BoundReference(SchemaField field) : field_(std::move(field)) {
  ICEBERG_DCHECK(field_.Validate().has_value(),
                 "Cannot create BoundReference with invalid field");
}

BoundReference::~BoundReference() = default;

std::string BoundReference::ToString() const {
  return std::format("ref(id={}, type={})", field_.field_id(), field_.type()->ToString());
}

Result<Literal> BoundReference::Evaluate(const StructLike& data) const {
  return NotImplemented("BoundReference::Evaluate(StructLike) not implemented");
}

bool BoundReference::Equals(const BoundTerm& other) const {
  if (other.kind() != Term::Kind::kReference) {
    return false;
  }

  const auto& other_ref = internal::checked_cast<const BoundReference&>(other);
  return field_.field_id() == other_ref.field_.field_id() &&
         field_.optional() == other_ref.field_.optional() &&
         *field_.type() == *other_ref.field_.type();
}

// UnboundTransform implementation
Result<std::unique_ptr<UnboundTransform>> UnboundTransform::Make(
    std::shared_ptr<NamedReference> ref, std::shared_ptr<Transform> transform) {
  if (!ref || !transform) [[unlikely]] {
    return InvalidExpression(
        "Cannot create UnboundTransform with null reference or transform");
  }
  return std::unique_ptr<UnboundTransform>(
      new UnboundTransform(std::move(ref), std::move(transform)));
}

UnboundTransform::UnboundTransform(std::shared_ptr<NamedReference> ref,
                                   std::shared_ptr<Transform> transform)
    : ref_(std::move(ref)), transform_(std::move(transform)) {
  ICEBERG_DCHECK(!ref || !transform,
                 "Cannot create UnboundTransform with null reference or transform");
}

UnboundTransform::~UnboundTransform() = default;

std::string UnboundTransform::ToString() const {
  return std::format("{}({})", transform_->ToString(), ref_->ToString());
}

Result<std::shared_ptr<BoundTransform>> UnboundTransform::Bind(
    const Schema& schema, bool case_sensitive) const {
  ICEBERG_ASSIGN_OR_RAISE(auto bound_ref, ref_->Bind(schema, case_sensitive));
  ICEBERG_ASSIGN_OR_RAISE(auto transform_func, transform_->Bind(bound_ref->type()));
  return BoundTransform::Make(std::move(bound_ref), transform_,
                              std::move(transform_func));
}

// BoundTransform implementation
Result<std::unique_ptr<BoundTransform>> BoundTransform::Make(
    std::shared_ptr<BoundReference> ref, std::shared_ptr<Transform> transform,
    std::shared_ptr<TransformFunction> transform_func) {
  if (!ref || !transform || !transform_func) [[unlikely]] {
    return InvalidExpression(
        "Cannot create BoundTransform with null reference or transform");
  }
  return std::unique_ptr<BoundTransform>(new BoundTransform(
      std::move(ref), std::move(transform), std::move(transform_func)));
}

BoundTransform::BoundTransform(std::shared_ptr<BoundReference> ref,
                               std::shared_ptr<Transform> transform,
                               std::shared_ptr<TransformFunction> transform_func)
    : ref_(std::move(ref)),
      transform_(std::move(transform)),
      transform_func_(std::move(transform_func)) {
  ICEBERG_DCHECK(ref_ && transform_ && transform_func_,
                 "Cannot create BoundTransform with null reference or transform");
}

BoundTransform::~BoundTransform() = default;

std::string BoundTransform::ToString() const {
  return std::format("{}({})", transform_->ToString(), ref_->ToString());
}

Result<Literal> BoundTransform::Evaluate(const StructLike& data) const {
  throw IcebergError("BoundTransform::Evaluate(StructLike) not implemented");
}

bool BoundTransform::MayProduceNull() const {
  // transforms must produce null for null input values
  // transforms may produce null for non-null inputs when not order-preserving
  // FIXME: add Transform::is_order_preserving()
  return ref_->MayProduceNull();  // || !transform_->is_order_preserving();
}

std::shared_ptr<Type> BoundTransform::type() const {
  return transform_func_->ResultType();
}

bool BoundTransform::Equals(const BoundTerm& other) const {
  if (other.kind() == Term::Kind::kTransform) {
    const auto& other_transform = internal::checked_cast<const BoundTransform&>(other);
    return *ref_ == *other_transform.ref_ && *transform_ == *other_transform.transform_;
  }

  if (transform_->transform_type() == TransformType::kIdentity &&
      other.kind() == Term::Kind::kReference) {
    return *ref_ == other;
  }

  return false;
}

// Explicit template instantiations
template Result<std::shared_ptr<BoundReference>> Unbound<BoundReference>::Bind(
    const Schema& schema) const;
template Result<std::shared_ptr<BoundTransform>> Unbound<BoundTransform>::Bind(
    const Schema& schema) const;

}  // namespace iceberg
