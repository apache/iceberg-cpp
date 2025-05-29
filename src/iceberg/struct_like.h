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

#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type.h"

namespace iceberg {

class Scalar;
class StructScalar;
class ArrayScalar;
class ScalarVisitor;
class StructLike;
class StructLikeVisitor;
/**
 * @brief Base class representing a generic scalar value.
 */
class ICEBERG_EXPORT Scalar {
 public:
  virtual ~Scalar() = default;

  /**
   * @brief Checks if the scalar represents a null value.
   * @return true if the value is null, false otherwise.
   */
  virtual bool IsNull() const = 0;

  /**
   * @brief Casts the scalar to a specific derived type.
   * @tparam T The target derived Scalar type.
   * @return Reference to the casted value.
   * @throws std::bad_cast if the cast fails.
   */
  template <typename T>
  const T& as() const {
    auto ptr = dynamic_cast<const T*>(this);
    if (!ptr) throw std::bad_cast();
    return *ptr;
  }

  /**
   * @brief Creates a deep copy of the scalar.
   * @return A shared pointer to the cloned scalar.
   */
  virtual std::shared_ptr<Scalar> clone() const = 0;

  /**
   * @brief Accepts a visitor for dispatching based on scalar type.
   * @param visitor The visitor to accept.
   */
  virtual void accept(ScalarVisitor& visitor) const = 0;
};

/**
 * @brief Visitor interface for Scalar, supporting type-dispatched operations.
 */
class ICEBERG_EXPORT ScalarVisitor {
 public:
  virtual ~ScalarVisitor() = default;

  /// Visit methods for primitive types
  virtual void visit(int32_t value) = 0;
  virtual void visit(int64_t value) = 0;
  virtual void visit(float value) = 0;
  virtual void visit(double value) = 0;
  virtual void visit(bool value) = 0;
  virtual void visit(const std::string& value) = 0;

  /**
   * @brief Called when visiting a null scalar.
   */
  virtual void visitNull() = 0;

  /// Visit methods for complex types
  virtual void visitStruct(const StructScalar& value) = 0;
  virtual void visitArray(const ArrayScalar& value) = 0;
};

/**
 * @brief Represents a scalar that holds a primitive value.
 * @tparam T The type of the primitive value.
 */
template <typename T>
class ICEBERG_EXPORT PrimitiveScalar : public Scalar {
 public:
  /**
   * @brief Constructs a PrimitiveScalar from an optional value.
   * @param value The optional primitive value.
   */
  explicit PrimitiveScalar(std::optional<T> value) : value_(std::move(value)) {}

  bool IsNull() const override { return !value_.has_value(); }

  /**
   * @brief Gets the actual value held by the scalar.
   * @return Reference to the value.
   * @note Behavior is undefined if the scalar is null.
   */
  const T& value() const { return *value_; }

  /**
   * @brief (Deprecated) Type-specific accessor for raw value.
   * @tparam U Must match T.
   * @return Reference to the value if types match.
   * @throws std::bad_cast if U does not match T.
   */
  template <typename U>
  const U& as() const {
    if constexpr (std::is_same_v<T, U>) return *value_;
    throw std::bad_cast();
  }

  std::shared_ptr<Scalar> clone() const override {
    return std::make_shared<PrimitiveScalar<T>>(value_);
  }

  void accept(ScalarVisitor& visitor) const override {
    if (IsNull()) {
      visitor.visitNull();
    } else {
      visitor.visit(*value_);
    }
  }

 private:
  std::optional<T> value_;
};

// Aliases for common primitive scalar types
using Int32Scalar = PrimitiveScalar<int32_t>;
using Int64Scalar = PrimitiveScalar<int64_t>;
using DoubleScalar = PrimitiveScalar<double>;
using StringScalar = PrimitiveScalar<std::string>;
using BoolScalar = PrimitiveScalar<bool>;

/**
 * @brief Base class for scalars that contain a vector of child Scalars.
 * @tparam Derived The concrete subclass type (CRTP).
 */
template <typename Derived>
class VectorScalarBase : public Scalar {
 public:
  /**
   * @brief Constructs a VectorScalarBase.
   * @param values Child scalar values.
   * @param is_null Whether the entire scalar is null.
   */
  explicit VectorScalarBase(std::vector<std::shared_ptr<Scalar>> values,
                            bool is_null = false)
      : values_(std::move(values)), is_null_(is_null) {}

  /**
   * @brief Gets the child scalar values.
   * @return Vector of shared pointers to child Scalars.
   */
  const std::vector<std::shared_ptr<Scalar>>& value() const { return values_; }

  bool IsNull() const override { return is_null_; }

  std::shared_ptr<Scalar> clone() const override {
    std::vector<std::shared_ptr<Scalar>> cloned;
    for (const auto& v : values_) {
      cloned.push_back(v->clone());
    }
    return std::make_shared<Derived>(std::move(cloned), is_null_);
  }

 protected:
  std::vector<std::shared_ptr<Scalar>> values_;
  bool is_null_;
};

/**
 * @brief Represents a struct-like scalar (tuple of fields).
 */
class StructScalar : public VectorScalarBase<StructScalar> {
 public:
  using VectorScalarBase::VectorScalarBase;

  void accept(ScalarVisitor& visitor) const override {
    if (IsNull()) {
      visitor.visitNull();
    } else {
      visitor.visitStruct(*this);
    }
  }
};

/**
 * @brief Represents an array-like scalar (list of values).
 */
class ArrayScalar : public VectorScalarBase<ArrayScalar> {
 public:
  using VectorScalarBase::VectorScalarBase;

  void accept(ScalarVisitor& visitor) const override {
    if (IsNull()) {
      visitor.visitNull();
    } else {
      visitor.visitArray(*this);
    }
  }
};

/**
 * @brief Abstract interface for structured field access.
 */
class ICEBERG_EXPORT StructLike {
 public:
  virtual ~StructLike() = default;

  /**
   * @brief Gets the type of the structure.
   * @return Reference to the StructType.
   */
  virtual const StructType& struct_type() const = 0;

  /**
   * @brief Gets the number of fields.
   * @return Number of fields.
   */
  virtual int32_t size() const = 0;

  /**
   * @brief Gets the value of a field by position.
   * @param pos The zero-based index of the field.
   * @return Reference to the scalar value.
   */
  virtual const Scalar& get(int32_t pos) const = 0;

  /**
   * @brief Sets the value of a field by position.
   * @param pos The zero-based index of the field.
   * @param value The scalar to assign.
   * @return Status indicating success or failure.
   */
  virtual Status set(int32_t pos, std::shared_ptr<Scalar> value) = 0;
};

/**
 * @brief Visitor interface for traversing StructLike fields.
 */
class ICEBERG_EXPORT StructLikeVisitor {
 public:
  virtual ~StructLikeVisitor() = default;

  /**
   * @brief Visits a single field of a StructLike object.
   * @param pos The index of the field.
   * @param type The type of the field.
   * @param value The scalar value of the field.
   */
  virtual void visitField(int pos, const Type& type, const Scalar& value) = 0;
};

}  // namespace iceberg
