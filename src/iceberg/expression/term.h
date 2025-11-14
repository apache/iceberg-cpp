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

/// \file iceberg/expression/term.h
/// Term interface for Iceberg expressions - represents values that can be evaluated.

#include <memory>
#include <string>
#include <string_view>

#include "iceberg/expression/literal.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief A term is an expression node that produces a typed value when evaluated.
class ICEBERG_EXPORT Term : public util::Formattable {
 public:
  enum class Kind : uint8_t { kReference = 0, kTransform, kExtract };

  /// \brief Returns the kind of this term.
  virtual Kind kind() const = 0;
};

/// \brief Interface for unbound expressions that need schema binding.
///
/// Unbound expressions contain string-based references that must be resolved
/// against a concrete schema to produce bound expressions that can be evaluated.
///
/// \tparam B The bound type this term produces when binding is successful
template <typename B>
class ICEBERG_EXPORT Unbound {
 public:
  /// \brief Bind this expression to a concrete schema.
  ///
  /// \param schema The schema to bind against
  /// \param case_sensitive Whether field name matching should be case sensitive
  /// \return A bound expression or an error if binding fails
  virtual Result<std::shared_ptr<B>> Bind(const Schema& schema,
                                          bool case_sensitive) const = 0;

  /// \brief Overloaded Bind method that uses case-sensitive matching by default.
  Result<std::shared_ptr<B>> Bind(const Schema& schema) const;

  /// \brief Returns the underlying named reference for this unbound term.
  virtual std::shared_ptr<class NamedReference> reference() = 0;
};

/// \brief Interface for bound expressions that can be evaluated.
///
/// Bound expressions have been resolved against a concrete schema and contain
/// all necessary information to evaluate against data structures.
class ICEBERG_EXPORT Bound {
 public:
  virtual ~Bound();

  /// \brief Evaluate this expression against a row-based data.
  virtual Result<Literal> Evaluate(const StructLike& data) const = 0;

  /// \brief Returns the underlying bound reference for this term.
  virtual std::shared_ptr<class BoundReference> reference() = 0;
};

/// \brief Base class for unbound terms.
///
/// \tparam B The bound type this term produces when binding is successful.
template <typename B>
class ICEBERG_EXPORT UnboundTerm : public Unbound<B>, public Term {
 public:
  using BoundType = B;
};

/// \brief Base class for bound terms.
class ICEBERG_EXPORT BoundTerm : public Bound, public Term {
 public:
  ~BoundTerm() override;

  /// \brief Returns the type produced by this term.
  virtual std::shared_ptr<Type> type() const = 0;

  /// \brief Returns whether this term may produce null values.
  virtual bool MayProduceNull() const = 0;

  // TODO(gangwu): add a comparator function to Literal and BoundTerm.

  /// \brief Returns whether this term is equivalent to another.
  ///
  /// Two terms are equivalent if they produce the same values when evaluated.
  ///
  /// \param other Another bound term to compare against
  /// \return true if the terms are equivalent, false otherwise
  virtual bool Equals(const BoundTerm& other) const = 0;

  friend bool operator==(const BoundTerm& lhs, const BoundTerm& rhs) {
    return lhs.Equals(rhs);
  }
};

/// \brief A reference represents a named field in an expression.
class ICEBERG_EXPORT Reference {
 public:
  virtual ~Reference();

  /// \brief Returns the name of the referenced field.
  virtual std::string_view name() const = 0;
};

/// \brief A reference to an unbound named field.
class ICEBERG_EXPORT NamedReference
    : public Reference,
      public UnboundTerm<BoundReference>,
      public std::enable_shared_from_this<NamedReference> {
 public:
  /// \brief Create a named reference to a field.
  ///
  /// \param field_name The name of the field to reference
  static Result<std::unique_ptr<NamedReference>> Make(std::string field_name);

  ~NamedReference() override;

  std::string_view name() const override { return field_name_; }

  Result<std::shared_ptr<BoundReference>> Bind(const Schema& schema,
                                               bool case_sensitive) const override;

  std::shared_ptr<NamedReference> reference() override { return shared_from_this(); }

  std::string ToString() const override;

  Kind kind() const override { return Kind::kReference; }

 private:
  explicit NamedReference(std::string field_name);

  std::string field_name_;
};

/// \brief A reference to a bound field.
class ICEBERG_EXPORT BoundReference
    : public Reference,
      public BoundTerm,
      public std::enable_shared_from_this<BoundReference> {
 public:
  /// \brief Create a bound reference.
  ///
  /// \param field The schema field
  static Result<std::unique_ptr<BoundReference>> Make(
      SchemaField field, std::unique_ptr<StructLikeAccessor> accessor);

  ~BoundReference() override;

  const SchemaField& field() const { return field_; }

  std::string_view name() const override { return field_.name(); }

  std::string ToString() const override;

  Result<Literal> Evaluate(const StructLike& data) const override;

  std::shared_ptr<BoundReference> reference() override { return shared_from_this(); }

  std::shared_ptr<Type> type() const override { return field_.type(); }

  bool MayProduceNull() const override { return field_.optional(); }

  bool Equals(const BoundTerm& other) const override;

  Kind kind() const override { return Kind::kReference; }

 private:
  BoundReference(SchemaField field, std::unique_ptr<StructLikeAccessor> accessor);

  SchemaField field_;
  std::unique_ptr<StructLikeAccessor> accessor_;
};

/// \brief An unbound transform expression.
class ICEBERG_EXPORT UnboundTransform : public UnboundTerm<class BoundTransform> {
 public:
  /// \brief Create an unbound transform.
  ///
  /// \param ref The term to apply the transformation to
  /// \param transform The transformation function to apply
  static Result<std::unique_ptr<UnboundTransform>> Make(
      std::shared_ptr<NamedReference> ref, std::shared_ptr<Transform> transform);

  ~UnboundTransform() override;

  std::string ToString() const override;

  Result<std::shared_ptr<BoundTransform>> Bind(const Schema& schema,
                                               bool case_sensitive) const override;

  std::shared_ptr<NamedReference> reference() override { return ref_; }

  const std::shared_ptr<Transform>& transform() const { return transform_; }

  Kind kind() const override { return Kind::kTransform; }

 private:
  UnboundTransform(std::shared_ptr<NamedReference> ref,
                   std::shared_ptr<Transform> transform);

  std::shared_ptr<NamedReference> ref_;
  std::shared_ptr<Transform> transform_;
};

/// \brief A bound transform expression.
class ICEBERG_EXPORT BoundTransform : public BoundTerm {
 public:
  /// \brief Create a bound transform.
  ///
  /// \param ref The bound term to apply the transformation to
  /// \param transform The transform to apply
  /// \param transform_func The bound transform function to apply
  static Result<std::unique_ptr<BoundTransform>> Make(
      std::shared_ptr<BoundReference> ref, std::shared_ptr<Transform> transform,
      std::shared_ptr<TransformFunction> transform_func);

  ~BoundTransform() override;

  std::string ToString() const override;

  Result<Literal> Evaluate(const StructLike& data) const override;

  std::shared_ptr<BoundReference> reference() override { return ref_; }

  std::shared_ptr<Type> type() const override;

  bool MayProduceNull() const override;

  bool Equals(const BoundTerm& other) const override;

  const std::shared_ptr<Transform>& transform() const { return transform_; }

  Kind kind() const override { return Kind::kTransform; }

 private:
  BoundTransform(std::shared_ptr<BoundReference> ref,
                 std::shared_ptr<Transform> transform,
                 std::shared_ptr<TransformFunction> transform_func);

  std::shared_ptr<BoundReference> ref_;
  std::shared_ptr<Transform> transform_;
  std::shared_ptr<TransformFunction> transform_func_;
};

}  // namespace iceberg
