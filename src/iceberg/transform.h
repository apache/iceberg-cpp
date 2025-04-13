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
ICEBERG_EXPORT constexpr std::string_view TransformTypeToString(TransformType type);

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

  /// \brief Creates a shared instance of the Bucket transform.
  ///
  /// Buckets values using a hash modulo operation. Commonly used for distributing data.
  /// \param num_buckets The number of buckets.
  /// \return A shared pointer to the Bucket transform.
  static std::shared_ptr<Transform> Bucket(int32_t num_buckets);

  /// \brief Creates a shared instance of the Truncate transform.
  ///
  /// Truncates values to a fixed width (e.g., for strings or binary data).
  /// \param width The width to truncate to.
  /// \return A shared pointer to the Truncate transform.
  static std::shared_ptr<Transform> Truncate(int32_t width);

  /// \brief Creates a shared singleton instance of the Year transform.
  ///
  /// Extracts the year portion from a date or timestamp.
  /// \return A shared pointer to the Year transform.
  static std::shared_ptr<Transform> Year();

  /// \brief Creates a shared singleton instance of the Month transform.
  ///
  /// Extracts the month portion from a date or timestamp.
  /// \return A shared pointer to the Month transform.
  static std::shared_ptr<Transform> Month();

  /// \brief Creates a shared singleton instance of the Day transform.
  ///
  /// Extracts the day portion from a date or timestamp.
  /// \return A shared pointer to the Day transform.
  static std::shared_ptr<Transform> Day();

  /// \brief Creates a shared singleton instance of the Hour transform.
  ///
  /// Extracts the hour portion from a timestamp.
  /// \return A shared pointer to the Hour transform.
  static std::shared_ptr<Transform> Hour();

  /// \brief Creates a shared singleton instance of the Void transform.
  ///
  /// Ignores values and always returns null. Useful for testing or special cases.
  /// \return A shared pointer to the Void transform.
  static std::shared_ptr<Transform> Void();

  /// \brief Returns the transform type.
  TransformType transform_type() const;

  /// \brief Binds this transform to a source type, returning a typed TransformFunction.
  ///
  /// This creates a concrete transform implementation based on the transform type and
  /// parameter.
  /// \param source_type The source column type to bind to.
  /// \return A TransformFunction instance wrapped in `expected`, or an error on failure.
  Result<std::unique_ptr<TransformFunction>> Bind(
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
  /// \brief Constructs a Transform of the specified type (for non-parametric types).
  /// \param transform_type The transform type (e.g., identity, year, day).
  explicit Transform(TransformType transform_type);

  /// \brief Constructs a parameterized Transform (e.g., bucket(16), truncate(4)).
  /// \param transform_type The transform type.
  /// \param param The integer parameter associated with the transform.
  Transform(TransformType transform_type, int32_t param);

  /// \brief Checks equality with another Transform instance.
  [[nodiscard]] virtual bool Equals(const Transform& other) const;

  TransformType transform_type_;
  /// Optional parameter (e.g., num_buckets, width)
  std::variant<std::monostate, int32_t> param_;
};
/// \brief Converts a string representation of a transform into a Transform instance.
///
/// This function parses the provided string to identify the corresponding transform type
/// (e.g., "identity", "year", "bucket[16]"), and creates a shared pointer to the
/// corresponding Transform object. It supports both simple transforms (like "identity")
/// and parameterized transforms (like "bucket[16]" or "truncate[4]").
///
/// \param transform_str The string representation of the transform type.
/// \return A Result containing either a shared pointer to the corresponding Transform
/// instance or an Error if the string does not match any valid transform type.
ICEBERG_EXPORT Result<std::shared_ptr<Transform>> TransformFromString(
    std::string_view transform_str);

/// \brief A transform function used for partitioning.
class ICEBERG_EXPORT TransformFunction {
 public:
  virtual ~TransformFunction() = default;
  TransformFunction(TransformType transform_type, std::shared_ptr<Type> source_type);
  /// \brief Transform an input array to a new array
  virtual Result<ArrowArray> Transform(const ArrowArray& data) = 0;
  /// \brief Get the transform type
  TransformType transform_type() const;
  /// \brief Get the source type of transform function
  const std::shared_ptr<Type>& source_type() const;
  /// \brief Get the result type of transform function
  virtual Result<std::shared_ptr<Type>> ResultType() const = 0;

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

}  // namespace iceberg
