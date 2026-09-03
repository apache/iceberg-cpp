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

/// \file iceberg/util/iterator.h
/// \brief Pull-based iterator interface for fallible, lazily produced values.

#include <deque>
#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

/// \brief A pull-based iterator whose reads may fail.
///
/// Iterator implementations own any resources needed to produce values. Destroying an
/// iterator releases those resources, including when iteration stops before reaching the
/// end. Iterators are not thread-safe unless an implementation explicitly says otherwise.
///
/// \tparam T Value returned by the iterator.
template <typename T>
class Iterator {
 public:
  virtual ~Iterator() = default;

  Iterator() = default;
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;

  /// \brief Return the next value, or std::nullopt when the iterator is exhausted.
  virtual Result<std::optional<T>> Next() = 0;

  /// \brief Consume the remaining values into a vector.
  Result<std::vector<T>> ToVector() {
    if constexpr (!std::is_move_constructible_v<T>) {
      static_assert(std::is_copy_constructible_v<T>,
                    "Iterator::ToVector requires T to be move- or copy-constructible");

      // A vector cannot grow portably when T has an explicitly deleted move
      // constructor. Stage copy-only values in a deque, then use vector's
      // forward-range constructor to allocate the final storage once.
      std::deque<T> values;
      while (true) {
        auto result = Next();
        if (!result.has_value()) {
          return std::unexpected(std::move(result.error()));
        }
        auto& value = result.value();
        if (!value.has_value()) {
          return std::vector<T>(values.cbegin(), values.cend());
        }
        values.push_back(value.value());
      }
    } else {
      std::vector<T> values;
      while (true) {
        auto result = Next();
        if (!result.has_value()) {
          return std::unexpected(std::move(result.error()));
        }
        auto& value = result.value();
        if (!value.has_value()) {
          return values;
        }
        values.push_back(std::move_if_noexcept(value.value()));
      }
    }
  }
};

}  // namespace iceberg
