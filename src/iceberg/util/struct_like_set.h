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

/// \file iceberg/util/struct_like_set.h
/// A hash set implementation for StructLike rows.

#include <memory>
#include <memory_resource>
#include <unordered_set>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/struct_like.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Compare two StructLike rows by scalar values.
ICEBERG_EXPORT Result<bool> StructLikeEqual(const StructLike& lhs, const StructLike& rhs);

/// \brief A set of StructLike rows with type-aware hashing and equality.
///
/// As StructLike uses view semantics, this set makes deep copies of inserted rows
/// into an internal arena to ensure ownership and lifetime safety. Lookups are
/// transparent and do not require temporary allocation.
///
/// \tparam kValidate When true (default), Insert and Contains validate that each
/// row's scalar types match the schema passed to the constructor. Set to false
/// only when the caller guarantees schema conformance and the validation
/// overhead must be avoided.
template <bool kValidate = true>
class ICEBERG_TEMPLATE_CLASS_EXPORT StructLikeSet {
 public:
  static constexpr size_t kDefaultArenaInitialSize = 64 * 1024;

  /// \brief Create a StructLikeSet for the given struct type.
  explicit StructLikeSet(const StructType& type,
                         size_t arena_initial_size = kDefaultArenaInitialSize);

  ~StructLikeSet();

  /// \brief Insert a row into the set.
  Status Insert(const StructLike& row);

  /// \brief Check if the set contains a row.
  Result<bool> Contains(const StructLike& row) const;

  /// \brief Check if the set is empty.
  bool IsEmpty() const;

  /// \brief Get the number of elements in the set.
  size_t Size() const;

 private:
  /// \brief Transparent hash functor operating on StructLike.
  struct KeyHash {
    using is_transparent = void;
    size_t operator()(const std::unique_ptr<StructLike>& p) const noexcept;
    size_t operator()(const StructLike& s) const noexcept;
  };

  /// \brief Transparent equality functor operating on StructLike.
  struct KeyEqual {
    using is_transparent = void;
    bool operator()(const std::unique_ptr<StructLike>& lhs,
                    const std::unique_ptr<StructLike>& rhs) const noexcept;
    bool operator()(const StructLike& lhs,
                    const std::unique_ptr<StructLike>& rhs) const noexcept;
    bool operator()(const std::unique_ptr<StructLike>& lhs,
                    const StructLike& rhs) const noexcept;
  };

  /// \brief Create an arena-owned deep copy of a StructLike row.
  Result<std::unique_ptr<StructLike>> MakeArenaRow(const StructLike& row) const;

  /// \brief Deep copy a scalar value, copying strings into arena and
  /// recursively materializing nested types.
  Result<Scalar> DeepCopyScalar(const Scalar& scalar) const;

  /// \brief Copy string data into the arena and return a view into it.
  std::string_view CopyToArena(std::string_view src) const;

  std::vector<std::shared_ptr<Type>> field_types_;
  mutable std::pmr::monotonic_buffer_resource arena_;
  std::unordered_set<std::unique_ptr<StructLike>, KeyHash, KeyEqual> set_;
};

extern template class ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT StructLikeSet<true>;
extern template class ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT StructLikeSet<false>;

}  // namespace iceberg
