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

/// \file iceberg/transform_function.h

#include "iceberg/transform.h"

namespace iceberg {
/// \brief Identity transform that returns the input unchanged.
class ICEBERG_EXPORT IdentityTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  explicit IdentityTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Returns the same Literal as the input.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns the same type as source_type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create an IdentityTransform.
  /// \param source_type Type of the input data.
  /// \return A Result containing the IdentityTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

/// \brief Bucket transform that hashes input values into N buckets.
class ICEBERG_EXPORT BucketTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  /// \param num_buckets Number of buckets to hash into.
  BucketTransform(std::shared_ptr<Type> const& source_type, int32_t num_buckets);

  /// \brief Applies the bucket hash function to the input Literal.
  ///
  /// Reference:
  /// - https://iceberg.apache.org/spec/#appendix-b-32-bit-hash-requirements
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns INT32 as the output type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a BucketTransform.
  /// \param source_type Type of the input data.
  /// \param num_buckets Number of buckets to hash into.
  /// \return A Result containing the BucketTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type, int32_t num_buckets);

 private:
  int32_t num_buckets_;
};

/// \brief Truncate transform that truncates values to a specified width.
class ICEBERG_EXPORT TruncateTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  /// \param width The width to truncate to (e.g., for strings or numbers).
  TruncateTransform(std::shared_ptr<Type> const& source_type, int32_t width);

  /// \brief Truncates the input Literal to the specified width.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns the same type as source_type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a TruncateTransform.
  /// \param source_type Type of the input data.
  /// \param width The width to truncate to.
  /// \return A Result containing the TruncateTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type, int32_t width);

 private:
  int32_t width_;
};

/// \brief Year transform that extracts the year component from timestamp inputs.
class ICEBERG_EXPORT YearTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit YearTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extract a date or timestamp year, as years from 1970.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns INT32 as the output type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a YearTransform.
  /// \param source_type Type of the input data.
  /// \return A Result containing the YearTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

/// \brief Month transform that extracts the month component from timestamp inputs.
class ICEBERG_EXPORT MonthTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit MonthTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extract a date or timestamp month, as months from 1970-01-01.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns INT32 as the output type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a MonthTransform.
  /// \param source_type Type of the input data.
  /// \return A Result containing the MonthTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

/// \brief Day transform that extracts the day of the month from timestamp inputs.
class ICEBERG_EXPORT DayTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit DayTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extract a date or timestamp day, as days from 1970-01-01.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Return the result type of a day transform.
  ///
  /// Note: The physical representation conforms to the Iceberg spec as DateType is
  /// internally converted to int. The DateType returned here provides a more
  /// human-readable way to display the partition field.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a DayTransform.
  /// \param source_type Type of the input data.
  /// \return A Result containing the DayTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

/// \brief Hour transform that extracts the hour component from timestamp inputs.
class ICEBERG_EXPORT HourTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit HourTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extract a timestamp hour, as hours from 1970-01-01 00:00:00.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns INT32 as the output type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a HourTransform.
  /// \param source_type Type of the input data.
  /// \return A Result containing the HourTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

/// \brief Void transform that discards the input and always returns null.
class ICEBERG_EXPORT VoidTransform : public TransformFunction {
 public:
  /// \param source_type Input type (ignored).
  explicit VoidTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Returns a null literal.
  Result<Literal> Transform(const Literal& literal) override;

  /// \brief Returns the same type as source_type.
  std::shared_ptr<Type> ResultType() const override;

  /// \brief Create a VoidTransform.
  /// \param source_type Input type (ignored).
  /// \return A Result containing the VoidTransform or an error.
  static Result<std::unique_ptr<TransformFunction>> Make(
      std::shared_ptr<Type> const& source_type);
};

}  // namespace iceberg
