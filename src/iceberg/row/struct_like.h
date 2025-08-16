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

#include "iceberg/expression/literal.h"
#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief A variant type that can hold a value from Literal::Value, StructLike,
/// ArrayLike, or MapLike, depending on the data type.
using ViewValue = std::variant<Literal::Value, std::shared_ptr<StructLike>,
                               std::shared_ptr<ArrayLike>, std::shared_ptr<MapLike>>;

/// \brief An immutable struct-like wrapper.
class ICEBERG_EXPORT StructLike {
 public:
  virtual ~StructLike() = default;

  /// \brief Get the field value at the given position.
  /// \param pos The position of the field in the struct.
  virtual Result<ViewValue> GetField(size_t pos) const = 0;

  /// \brief Get the number of fields in the struct.
  virtual size_t num_fields() const = 0;
};

/// \brief An immutable array-like wrapper.
class ICEBERG_EXPORT ArrayLike {
 public:
  virtual ~ArrayLike() = default;

  /// \brief Get the array element at the given position.
  /// \param pos The position of the element in the array.
  virtual Result<ViewValue> GetElement(size_t pos) const = 0;

  /// \brief Get the number of elements in the array.
  virtual size_t size() const = 0;
};

/// \brief An immutable map-like wrapper.
class ICEBERG_EXPORT MapLike {
 public:
  virtual ~MapLike() = default;

  /// \brief Get the key at the given position.
  /// \param pos The position of the key in the map.
  virtual Result<ViewValue> GetKey(size_t pos) const = 0;

  /// \brief Get the value at the given position.
  /// \param pos The position of the value in the map.
  virtual Result<ViewValue> GetValue(size_t pos) const = 0;

  /// \brief Get the number of entries in the map.
  virtual size_t size() const = 0;
};

}  // namespace iceberg
