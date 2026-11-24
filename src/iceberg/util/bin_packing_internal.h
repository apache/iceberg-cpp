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

#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>

#include "iceberg/util/int128.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/stream.h"

namespace iceberg::internal::bin_packing {

// Mirrors org.apache.iceberg.util.BinPacking.
template <typename T>
class Bin {
 public:
  Bin(int64_t target_weight, int64_t max_size)
      : target_weight_(target_weight), max_size_(max_size) {}

  bool CanAdd(int64_t weight) const {
    return bin_weight_ + weight <= target_weight_ &&
           std::cmp_less(items_.size(), max_size_);
  }

  void Add(T item, int64_t weight) {
    bin_weight_ += weight;
    items_.push_back(std::move(item));
  }

  int128_t weight() const { return bin_weight_; }
  std::vector<T> items() && { return std::move(items_); }

 private:
  int64_t target_weight_;
  int64_t max_size_;
  std::vector<T> items_;
  // Widen the sum so two large weights cannot wrap and appear to fit.
  int128_t bin_weight_ = 0;
};

template <typename T>
class PackingIterator final : public Stream<std::vector<T>> {
 public:
  using NextItem = std::function<Result<std::optional<T>>()>;
  using WeightFunc = std::function<Result<int64_t>(const T&)>;

  PackingIterator(NextItem items, int64_t target_weight, int64_t lookback,
                  int64_t max_size, WeightFunc weight_func, bool largest_bin_first)
      : items_(std::move(items)),
        target_weight_(target_weight),
        lookback_(lookback),
        max_size_(max_size),
        weight_func_(std::move(weight_func)),
        largest_bin_first_(largest_bin_first) {
    ICEBERG_DCHECK(lookback > 0, "Bin look-back size must be greater than 0");
  }

 private:
  Result<std::optional<std::vector<T>>> NextImpl() override {
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto item, items_());
      if (!item) {
        break;
      }
      ICEBERG_ASSIGN_OR_RAISE(auto weight, weight_func_(*item));
      if (auto* bin = FindBin(weight)) {
        bin->Add(std::move(*item), weight);
      } else {
        auto new_bin = NewBin();
        new_bin.Add(std::move(*item), weight);
        bins_.push_back(std::move(new_bin));

        if (std::cmp_greater(bins_.size(), lookback_)) {
          auto bin_to_remove = largest_bin_first_ ? RemoveLargestBin() : RemoveFirst();
          return std::move(bin_to_remove).items();
        }
      }
    }

    if (bins_.empty()) {
      return std::nullopt;
    }
    return RemoveFirst().items();
  }

  Bin<T>* FindBin(int64_t weight) {
    for (auto& bin : bins_) {
      if (bin.CanAdd(weight)) {
        return &bin;
      }
    }
    return nullptr;
  }

  Bin<T> NewBin() const { return Bin<T>(target_weight_, max_size_); }

  Bin<T> RemoveLargestBin() {
    auto largest = std::ranges::max_element(bins_, {}, &Bin<T>::weight);
    auto bin = std::move(*largest);
    bins_.erase(largest);
    return bin;
  }

  Bin<T> RemoveFirst() {
    auto bin = std::move(bins_.front());
    bins_.pop_front();
    return bin;
  }

  std::list<Bin<T>> bins_;
  NextItem items_;
  int64_t target_weight_;
  int64_t lookback_;
  int64_t max_size_;
  WeightFunc weight_func_;
  bool largest_bin_first_;
};

// The range must outlive its iterators. Single-pass streams use PackingIterator directly.
template <typename T, std::ranges::forward_range Range>
class PackingIterable {
 public:
  using WeightFunc = typename PackingIterator<T>::WeightFunc;

  PackingIterable(Range& iterable, int64_t target_weight, int64_t lookback,
                  WeightFunc weight_func, bool largest_bin_first = false,
                  int64_t max_size = std::numeric_limits<int64_t>::max())
      : iterable_(iterable),
        target_weight_(target_weight),
        lookback_(lookback),
        weight_func_(std::move(weight_func)),
        largest_bin_first_(largest_bin_first),
        max_size_(max_size) {}

  Result<PackingIterator<T>> Iterator() const {
    ICEBERG_PRECHECK(lookback_ > 0, "Bin look-back size must be greater than 0: {}",
                     lookback_);
    auto it = std::ranges::begin(iterable_);
    auto end = std::ranges::end(iterable_);
    auto items = [it, end]() mutable -> Result<std::optional<T>> {
      if (it == end) {
        return std::nullopt;
      }
      return std::optional<T>{*it++};
    };
    return PackingIterator<T>(std::move(items), target_weight_, lookback_, max_size_,
                              weight_func_, largest_bin_first_);
  }

 private:
  Range& iterable_;
  int64_t target_weight_;
  int64_t lookback_;
  WeightFunc weight_func_;
  bool largest_bin_first_;
  int64_t max_size_;
};

template <typename T>
class ListPacker {
 public:
  using WeightFunc = typename PackingIterator<T>::WeightFunc;

  ListPacker(int64_t target_weight, int64_t lookback, bool largest_bin_first,
             int64_t max_items_per_bin = std::numeric_limits<int64_t>::max())
      : target_weight_(target_weight),
        lookback_(lookback),
        largest_bin_first_(largest_bin_first),
        max_items_per_bin_(max_items_per_bin) {}

  Result<std::vector<std::vector<T>>> PackEnd(const std::vector<T>& items,
                                              WeightFunc weight_func) const {
    ICEBERG_ASSIGN_OR_RAISE(auto bins,
                            Pack(std::views::reverse(items), std::move(weight_func)));
    for (auto& bin : bins) {
      std::ranges::reverse(bin);
    }
    std::ranges::reverse(bins);
    return bins;
  }

  template <typename Range>
  Result<std::vector<std::vector<T>>> Pack(const Range& items,
                                           WeightFunc weight_func) const {
    PackingIterable<T, const Range> iterable(items, target_weight_, lookback_,
                                             std::move(weight_func), largest_bin_first_,
                                             max_items_per_bin_);
    ICEBERG_ASSIGN_OR_RAISE(auto iter, iterable.Iterator());
    return iter.ToVector();
  }

 private:
  int64_t target_weight_;
  int64_t lookback_;
  bool largest_bin_first_;
  int64_t max_items_per_bin_;
};

}  // namespace iceberg::internal::bin_packing
