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

/// \file iceberg/row/struct_like.h
/// Structures for viewing data in a row-based format.  This header contains the
/// definition of StructLike, ArrayLike, and MapLike which provide an unified
/// interface for viewing data from ArrowArray or structs like ManifestFile and
/// ManifestEntry.  Note that they do not carry type information and should be
/// used in conjunction with the schema to get the type information.

#include <functional>
#include <memory>
#include <span>
#include <string_view>
#include <variant>

#include "iceberg/expression/literal.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/decimal.h"

namespace iceberg {

/// \brief A scalar value depending on its data type.
///
/// Note that all string and binary values are stored as non-owned string_view
/// and their lifetime is managed by the wrapped object.
using Scalar = std::variant<std::monostate,  // for null
                            bool,            // for boolean
                            int32_t,         // for int, date
                            int64_t,  // for long, timestamp, timestamp_tz, and time
                            float,    // for float
                            double,   // for double
                            std::string_view,  // for non-owned string, binary and fixed
                            Decimal,           // for decimal
                            std::shared_ptr<StructLike>,  // for struct
                            std::shared_ptr<ArrayLike>,   // for list
                            std::shared_ptr<MapLike>>;    // for map

/// \brief Convert a Literal to a Scalar
Result<Scalar> LiteralToScalar(const Literal& literal);

/// \brief An immutable struct-like wrapper.
class ICEBERG_EXPORT StructLike {
 public:
  virtual ~StructLike() = default;

  /// \brief Get the field value at the given position.
  /// \param pos The position of the field in the struct.
  virtual Result<Scalar> GetField(size_t pos) const = 0;

  /// \brief Get the number of fields in the struct.
  virtual size_t num_fields() const = 0;
};

/// \brief An immutable array-like wrapper.
class ICEBERG_EXPORT ArrayLike {
 public:
  virtual ~ArrayLike() = default;

  /// \brief Get the array element at the given position.
  /// \param pos The position of the element in the array.
  virtual Result<Scalar> GetElement(size_t pos) const = 0;

  /// \brief Get the number of elements in the array.
  virtual size_t size() const = 0;
};

/// \brief An immutable map-like wrapper.
class ICEBERG_EXPORT MapLike {
 public:
  virtual ~MapLike() = default;

  /// \brief Get the key at the given position.
  /// \param pos The position of the key in the map.
  virtual Result<Scalar> GetKey(size_t pos) const = 0;

  /// \brief Get the value at the given position.
  /// \param pos The position of the value in the map.
  virtual Result<Scalar> GetValue(size_t pos) const = 0;

  /// \brief Get the number of entries in the map.
  virtual size_t size() const = 0;
};

/// \brief An accessor for a struct-like object.
class ICEBERG_EXPORT StructLikeAccessor {
 public:
  explicit StructLikeAccessor(std::shared_ptr<Type> type,
                              std::span<const size_t> position_path);

  /// \brief Get the scalar value at the given position.
  Result<Scalar> Get(const StructLike& struct_like) const {
    return accessor_(struct_like);
  }

  /// \brief Get the literal value at the given position.
  ///
  /// \return The literal value at the given position, or an error if it is
  /// not a primitive type.
  Result<Literal> GetLiteral(const StructLike& struct_like) const;

  /// \brief Get the type of the value that this accessor is bound to.
  const Type& type() const { return *type_; }

  /// \brief Get the position path of the value that this accessor bounded to.
  const std::vector<size_t>& position_path() const { return position_path_; }

 private:
  std::shared_ptr<Type> type_;
  std::function<Result<Scalar>(const StructLike&)> accessor_;
  std::vector<size_t> position_path_;
};

}  // namespace iceberg
