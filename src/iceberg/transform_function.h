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
class IdentityTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  explicit IdentityTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Returns the input array without modification.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns the same type as the source type if it is valid.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

/// \brief Bucket transform that hashes input values into N buckets.
class BucketTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  /// \param num_buckets Number of buckets to hash into.
  BucketTransform(std::shared_ptr<Type> const& source_type, int32_t num_buckets);

  /// \brief Applies the bucket hash function to the input array.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns INT32 as the output type.
  Result<std::shared_ptr<Type>> ResultType() const override;

 private:
  int32_t num_buckets_;
};

/// \brief Truncate transform that truncates values to a specified width.
class TruncateTransform : public TransformFunction {
 public:
  /// \param source_type Type of the input data.
  /// \param width The width to truncate to (e.g., for strings or numbers).
  TruncateTransform(std::shared_ptr<Type> const& source_type, int32_t width);

  /// \brief Truncates values in the input array to the specified width.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns the same type as source_type.
  Result<std::shared_ptr<Type>> ResultType() const override;

 private:
  int32_t width_;
};

/// \brief Year transform that extracts the year component from timestamp inputs.
class YearTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit YearTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extracts the year from each timestamp in the input array.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns INT32 as the output type.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

/// \brief Month transform that extracts the month component from timestamp inputs.
class MonthTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit MonthTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extracts the month (1-12) from each timestamp in the input array.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns INT32 as the output type.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

/// \brief Day transform that extracts the day of the month from timestamp inputs.
class DayTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit DayTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extracts the day (1-31) from each timestamp in the input array.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns INT32 as the output type.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

/// \brief Hour transform that extracts the hour component from timestamp inputs.
class HourTransform : public TransformFunction {
 public:
  /// \param source_type Must be a timestamp type.
  explicit HourTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Extracts the hour (0-23) from each timestamp in the input array.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns INT32 as the output type.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

/// \brief Void transform that discards the input and always returns null.
class VoidTransform : public TransformFunction {
 public:
  /// \param source_type Input type (ignored).
  explicit VoidTransform(std::shared_ptr<Type> const& source_type);

  /// \brief Returns an all-null array of the same length as the input.
  Result<ArrowArray> Transform(const ArrowArray& input) override;

  /// \brief Returns null type or a sentinel type indicating void.
  Result<std::shared_ptr<Type>> ResultType() const override;
};

}  // namespace iceberg
