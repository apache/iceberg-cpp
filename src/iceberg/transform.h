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

/// \file iceberg/transform.h

#include <cstdint>
#include <memory>
#include <variant>

#include "iceberg/arrow_c_data.h"
#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/formattable.h"

namespace iceberg {

/// \brief Transform types used for partitioning
enum class TransformType {
  /// Used to represent some customized transform that can't be recognized or supported
  /// now.
  kUnknown,
  /// Equal to source value, unmodified
  kIdentity,
  /// Hash of value, mod `N`
  kBucket,
  /// Value truncated to width `W`
  kTruncate,
  /// Extract a date or timestamp year, as years from 1970
  kYear,
  /// Extract a date or timestamp month, as months from 1970-01
  kMonth,
  /// Extract a date or timestamp day, as days from 1970-01-01
  kDay,
  /// Extract a timestamp hour, as hours from 1970-01-01 00:00:00
  kHour,
  /// Always produces `null`
  kVoid,
};

/// \brief Get the relative transform name
constexpr std::string_view TransformTypeToString(TransformType type) {
  switch (type) {
    case TransformType::kUnknown:
      return "unknown";
    case TransformType::kIdentity:
      return "identity";
    case TransformType::kBucket:
      return "bucket";
    case TransformType::kTruncate:
      return "truncate";
    case TransformType::kYear:
      return "year";
    case TransformType::kMonth:
      return "month";
    case TransformType::kDay:
      return "day";
    case TransformType::kHour:
      return "hour";
    case TransformType::kVoid:
      return "void";
  }
}

/// \brief Represents a transform used in partitioning or sorting in Iceberg.
///
/// This class supports binding to a source type and instantiating the corresponding
/// TransformFunction, as well as serialization-friendly introspection.
class ICEBERG_EXPORT Transform : public util::Formattable {
 public:
  /// \brief Returns a shared singleton instance of the Identity transform.
  ///
  /// This transform leaves values unchanged and is commonly used for direct partitioning.
  /// \return A shared pointer to the Identity transform.
  static std::shared_ptr<Transform> Identity();

  /// \brief Constructs a Transform of the specified type (for non-parametric types).
  /// \param transform_type The transform type (e.g., identity, year, day).
  explicit Transform(TransformType transform_type);

  /// \brief Constructs a parameterized Transform (e.g., bucket(16), truncate(4)).
  /// \param transform_type The transform type.
  /// \param param The integer parameter associated with the transform.
  Transform(TransformType transform_type, int32_t param);

  /// \brief Returns the transform type.
  TransformType transform_type() const;

  /// \brief Binds this transform to a source type, returning a typed TransformFunction.
  ///
  /// This creates a concrete transform implementation based on the transform type and
  /// parameter.
  /// \param source_type The source column type to bind to.
  /// \return A TransformFunction instance wrapped in `expected`, or an error on failure.
  expected<std::unique_ptr<TransformFunction>, Error> Bind(
      const std::shared_ptr<Type>& source_type) const;

  /// \brief Returns a string representation of this transform (e.g., "bucket[16]").
  std::string ToString() const override;

  /// \brief Equality comparison.
  friend bool operator==(const Transform& lhs, const Transform& rhs) {
    return lhs.Equals(rhs);
  }

  /// \brief Inequality comparison.
  friend bool operator!=(const Transform& lhs, const Transform& rhs) {
    return !(lhs == rhs);
  }

 private:
  /// \brief Checks equality with another Transform instance.
  [[nodiscard]] virtual bool Equals(const Transform& other) const;

  TransformType transform_type_;
  ///< Optional parameter (e.g., num_buckets, width)
  std::variant<std::monostate, int32_t> param_;
};

/// \brief A transform function used for partitioning.
class ICEBERG_EXPORT TransformFunction {
 public:
  virtual ~TransformFunction() = default;
  TransformFunction(TransformType transform_type, std::shared_ptr<Type> source_type);
  /// \brief Transform an input array to a new array
  virtual expected<ArrowArray, Error> Transform(const ArrowArray& data) = 0;
  /// \brief Get the transform type
  TransformType transform_type() const;
  /// \brief Get the source type of transform function
  const std::shared_ptr<Type>& source_type() const;
  /// \brief Get the result type of transform function
  virtual expected<std::shared_ptr<Type>, Error> ResultType() const = 0;

  friend bool operator==(const TransformFunction& lhs, const TransformFunction& rhs) {
    return lhs.Equals(rhs);
  }

  friend bool operator!=(const TransformFunction& lhs, const TransformFunction& rhs) {
    return !(lhs == rhs);
  }

 private:
  /// \brief Compare two partition specs for equality.
  [[nodiscard]] virtual bool Equals(const TransformFunction& other) const;

  TransformType transform_type_;
  std::shared_ptr<Type> source_type_;
};

ICEBERG_EXPORT expected<std::unique_ptr<Transform>, Error> TransformFromString(
    std::string_view str);

}  // namespace iceberg
