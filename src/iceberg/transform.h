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

#include "iceberg/arrow_c_data.h"
#include "iceberg/error.h"
#include "iceberg/expected.h"
#include "iceberg/iceberg_export.h"
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

/// \brief A transform function used for partitioning.
class ICEBERG_EXPORT TransformFunction : public util::Formattable {
 public:
  explicit TransformFunction(TransformType type);
  /// \brief Transform an input array to a new array
  virtual expected<ArrowArray, Error> Transform(const ArrowArray& data) = 0;
  /// \brief Get the transform type
  virtual TransformType transform_type() const;

  std::string ToString() const override;

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
};

class IdentityTransformFunction : public TransformFunction {
 public:
  IdentityTransformFunction();
  /// \brief Transform will take an input array and transform it into a new array.
  expected<ArrowArray, Error> Transform(const ArrowArray& input) override;
};

}  // namespace iceberg
