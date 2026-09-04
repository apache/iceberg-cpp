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

#include <optional>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/result.h"

namespace iceberg {

/// \brief A pull-based iterator whose reads may fail.
///
/// Iterator implementations own any resources needed to produce values. Destroying an
/// iterator releases those resources, including when iteration stops before reaching the
/// end. Iterators are not thread-safe unless an implementation explicitly says otherwise.
/// Once Next() returns an error or std::nullopt, the iterator is terminal. Subsequent
/// calls return the same terminal result without invoking the implementation again.
///
/// \tparam T Value returned by the iterator.
template <typename T>
class Iterator {
 public:
  virtual ~Iterator() = default;

  Iterator() = default;
  Iterator(const Iterator&) = delete;
  Iterator& operator=(const Iterator&) = delete;
  Iterator(Iterator&&) noexcept = default;
  Iterator& operator=(Iterator&&) noexcept = default;

  /// \brief Return the next value, or std::nullopt when the iterator is exhausted.
  ///
  /// After this method returns an error or std::nullopt, subsequent calls return the same
  /// terminal result without invoking NextImpl().
  Result<std::optional<T>> Next() {
    if (error_.has_value()) {
      return std::unexpected(*error_);
    }
    if (finished_) {
      return std::nullopt;
    }

    auto result = NextImpl();
    if (!result.has_value()) {
      error_ = result.error();
    } else if (!result.value().has_value()) {
      finished_ = true;
    }
    return result;
  }

  /// \brief Consume the remaining values into a vector.
  Result<std::vector<T>> ToVector() {
    static_assert(std::is_move_constructible_v<T> || std::is_copy_constructible_v<T>,
                  "Iterator::ToVector requires T to be move- or copy-constructible");

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

 protected:
  /// \brief Produce the next value for Next().
  ///
  /// Implementations must return std::nullopt when exhausted. Next() makes the
  /// terminal state sticky, so implementations are not called after exhaustion or error.
  virtual Result<std::optional<T>> NextImpl() = 0;

 private:
  bool finished_ = false;
  std::optional<Error> error_;
};

}  // namespace iceberg
