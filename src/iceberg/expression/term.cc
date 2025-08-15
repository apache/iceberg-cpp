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

#include "iceberg/exception.h"
#include "iceberg/transform.h"

namespace iceberg {

template <typename B>
Result<std::unique_ptr<B>> Unbound<B>::Bind(const Schema& schema) const {
  return Bind(schema, /*case_sensitive=*/true);
}

// NamedReference implementation
NamedReference::NamedReference(std::string field_name)
    : field_name_(std::move(field_name)) {}

Result<std::unique_ptr<BoundReference>> NamedReference::Bind(const Schema& schema,
                                                             bool case_sensitive) const {
  throw IcebergError("NamedReference::Bind not implemented");
}

std::string NamedReference::ToString() const {
  throw IcebergError("NamedReference::ToString not implemented");
}

// BoundReference implementation
BoundReference::BoundReference(std::shared_ptr<SchemaField> field)
    : field_(std::move(field)) {}

std::string BoundReference::ToString() const {
  throw IcebergError("BoundReference::ToString not implemented");
}

Result<Literal::Value> BoundReference::Evaluate(const StructLike& data) const {
  throw IcebergError("BoundReference::Evaluate(StructLike) not implemented");
}

Result<std::vector<Literal::Value>> BoundReference::Evaluate(
    const ArrowArray& data) const {
  throw IcebergError("BoundReference::Evaluate(ArrowArray) not implemented");
}

bool BoundReference::Equals(const BoundTerm& other) const {
  throw IcebergError("BoundReference::Equals not implemented");
}

// UnboundTransform implementation
UnboundTransform::UnboundTransform(std::shared_ptr<NamedReference> ref,
                                   std::shared_ptr<Transform> transform)
    : ref_(std::move(ref)), transform_(std::move(transform)) {}

std::string UnboundTransform::ToString() const {
  throw IcebergError("UnboundTransform::ToString not implemented");
}

Result<std::unique_ptr<BoundTransform>> UnboundTransform::Bind(
    const Schema& schema, bool case_sensitive) const {
  throw IcebergError("UnboundTransform::Bind not implemented");
}

// BoundTransform implementation
BoundTransform::BoundTransform(std::shared_ptr<BoundReference> ref,
                               std::shared_ptr<Transform> transform)
    : ref_(std::move(ref)), transform_(std::move(transform)) {}

std::string BoundTransform::ToString() const {
  throw IcebergError("BoundTransform::ToString not implemented");
}

Result<Literal::Value> BoundTransform::Evaluate(const StructLike& data) const {
  throw IcebergError("BoundTransform::Evaluate(StructLike) not implemented");
}

Result<std::vector<Literal::Value>> BoundTransform::Evaluate(
    const ArrowArray& data) const {
  throw IcebergError("BoundTransform::Evaluate(ArrowArray) not implemented");
}

bool BoundTransform::Equals(const BoundTerm& other) const {
  throw IcebergError("BoundTransform::Equals not implemented");
}

// Explicit template instantiations for the types used in the codebase
template Result<std::unique_ptr<BoundReference>> Unbound<BoundReference>::Bind(
    const Schema& schema) const;
template Result<std::unique_ptr<BoundTransform>> Unbound<BoundTransform>::Bind(
    const Schema& schema) const;

}  // namespace iceberg
