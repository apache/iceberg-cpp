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
#include <string_view>
#include <utility>
#include <variant>

#include "iceberg/expression/literal.h"
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
ICEBERG_EXPORT constexpr std::string_view TransformTypeToString(TransformType type) {
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
  std::unreachable();
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
  Result<std::shared_ptr<TransformFunction>> Bind(
      const std::shared_ptr<Type>& source_type) const;

  /// \brief Checks whether this function can be applied to the given Type.
  /// \param source_type The source type to check.
  /// \return true if this transform can be applied to the type, false otherwise
  bool CanTransform(const Type& source_type) const;

  /// \brief Whether the transform preserves the order of values (is monotonic).
  bool PreservesOrder() const;

  /// \brief Whether ordering by this transform's result satisfies the ordering of another
  /// transform's result.
  ///
  /// For example, sorting by day(ts) will produce an ordering that is also by month(ts)
  /// or year(ts). However, sorting by day(ts) will not satisfy the order of hour(ts) or
  /// identity(ts).
  /// \param other The other transform to compare with.
  /// \return true if ordering by this transform is equivalent to ordering by the other
  /// transform.
  bool SatisfiesOrderOf(const Transform& other) const;

  /// \brief Transforms a BoundPredicate to an inclusive predicate on the partition values
  /// produced by the transform.
  ///
  /// This inclusive transform guarantees that if predicate->Test(value) is true, then
  /// Projected(transform(value)) is true.
  /// \param name The name of the partition column.
  /// \param predicate The predicate to project.
  /// \return A Result containing either a unique pointer to the projected predicate,
  /// nullptr if the projection cannot be performed, or an Error if the projection fails.
  Result<std::unique_ptr<UnboundPredicate>> Project(
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate);

  /// \brief Transforms a BoundPredicate to a strict predicate on the partition values
  /// produced by the transform.
  ///
  /// This strict transform guarantees that if Projected(transform(value)) is true, then
  /// predicate->Test(value) is also true.
  /// \param name The name of the partition column.
  /// \param predicate The predicate to project.
  /// \return A Result containing either a unique pointer to the projected predicate,
  /// nullptr if the projection cannot be performed, or an Error if the projection fails.
  Result<std::unique_ptr<UnboundPredicate>> ProjectStrict(
      std::string_view name, const std::shared_ptr<BoundPredicate>& predicate);

  /// \brief Returns a string representation of this transform (e.g., "bucket[16]").
  std::string ToString() const override;

  /// \brief Return the unique transform name to check if similar transforms for the same
  /// source field are added multiple times in partition spec builder.
  std::string DedupName() const;

  /// \brief Generates a partition name for the transform.
  /// \param source_name The name of the source column.
  /// \return A string representation of the partition name.
  Result<std::string> GeneratePartitionName(std::string_view source_name) const;

  /// \brief Equality comparison.
  friend bool operator==(const Transform& lhs, const Transform& rhs) {
    return lhs.Equals(rhs);
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
  /// \brief Transform an input Literal to a new Literal
  ///
  /// All transforms must return null for a null input value.
  virtual Result<Literal> Transform(const Literal& literal) = 0;
  /// \brief Get the transform type
  TransformType transform_type() const;
  /// \brief Get the source type of transform function
  const std::shared_ptr<Type>& source_type() const;
  /// \brief Get the result type of transform function
  ///
  /// Note: This method defines both the physical and display representation of the
  /// partition field. The physical representation must conform to the Iceberg spec. The
  /// display representation can deviate from the spec, such as by transforming the value
  /// into a more human-readable format.
  virtual std::shared_ptr<Type> ResultType() const = 0;

  friend bool operator==(const TransformFunction& lhs, const TransformFunction& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two partition specs for equality.
  [[nodiscard]] virtual bool Equals(const TransformFunction& other) const;

  TransformType transform_type_;
  std::shared_ptr<Type> source_type_;
};

}  // namespace iceberg
