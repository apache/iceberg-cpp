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

#include <concepts>
#include <functional>
#include <iterator>
#include <ranges>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/executor.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/task_group.h"

namespace iceberg {

template <typename T, auto... Options>
struct ParallelReduce;

namespace internal {

template <typename T, auto... Options>
concept ParallelReducible = requires(std::vector<T>& values) {
  typename ParallelReduce<T, Options...>::result_type;
  {
    ParallelReduce<T, Options...>::Reduce(values)
  } -> std::same_as<typename ParallelReduce<T, Options...>::result_type>;
};

template <std::ranges::input_range InputRange, typename Task>
using ParallelCollectValueT =
    ResultValueT<std::invoke_result_t<std::remove_reference_t<Task>&,
                                      std::add_lvalue_reference_t<std::remove_reference_t<
                                          std::ranges::range_reference_t<InputRange>>>>>;

template <std::size_t I, typename... Args>
struct ParallelCollectTraits {
  using args_tuple_type = std::tuple<Args&&...>;
  using input_type = std::tuple_element_t<I * 2, args_tuple_type>;
  using task_type = std::tuple_element_t<I * 2 + 1, args_tuple_type>;
  using value_type = ParallelCollectValueT<input_type, task_type>;
};

template <typename InputRange, typename Task, auto... Options>
concept ParallelCollectible =
    std::ranges::forward_range<InputRange> && std::ranges::sized_range<InputRange> &&
    std::is_lvalue_reference_v<std::ranges::range_reference_t<InputRange>> &&
    requires(std::remove_reference_t<Task>& task,
             std::ranges::range_reference_t<InputRange> item) {
      { std::invoke(task, item) } -> AsResult;
      requires(!std::same_as<void, ParallelCollectValueT<InputRange, Task>>);
      requires std::default_initializable<ParallelCollectValueT<InputRange, Task>>;
      requires ParallelReducible<ParallelCollectValueT<InputRange, Task>, Options...>;
    };

}  // namespace internal

template <typename... Args>
struct ParallelReduce<std::unordered_set<Args...>> {
  using result_type = std::unordered_set<Args...>;

  template <std::ranges::input_range Values>
  static result_type Reduce(Values&& values) {
    result_type result;
    for (auto&& value : values) {
      result.merge(value);
    }
    return result;
  }
};

template <typename... Args>
struct ParallelReduce<std::vector<Args...>> {
  using result_type = std::vector<Args...>;

  template <std::ranges::input_range Values>
  static result_type Reduce(Values&& values) {
    return std::forward<Values>(values) | std::views::join | std::views::as_rvalue |
           std::ranges::to<result_type>();
  }
};

template <typename K, typename... VectorArgs, typename... MapArgs>
struct ParallelReduce<std::unordered_map<K, std::vector<VectorArgs...>, MapArgs...>> {
  using result_type = std::unordered_map<K, std::vector<VectorArgs...>, MapArgs...>;

  template <std::ranges::input_range Values>
  static result_type Reduce(Values&& values) {
    result_type result;
    for (auto&& value : values) {
      result.merge(value);
      for (auto& [key, entries] : value) {
        auto& out = result[key];
        out.insert(out.end(), std::make_move_iterator(entries.begin()),
                   std::make_move_iterator(entries.end()));
      }
    }
    return result;
  }
};

template <typename First, typename Second>
struct ParallelReduce<std::pair<First, Second>> {
  using result_type = std::pair<typename ParallelReduce<First>::result_type,
                                typename ParallelReduce<Second>::result_type>;

  template <std::ranges::forward_range Values>
  static result_type Reduce(Values&& values) {
    return {ParallelReduce<First>::Reduce(values | std::views::elements<0>),
            ParallelReduce<Second>::Reduce(values | std::views::elements<1>)};
  }
};

template <typename... Ts>
struct ParallelReduce<std::tuple<Ts...>> {
  using result_type = std::tuple<typename ParallelReduce<Ts>::result_type...>;

  template <std::ranges::forward_range Values>
  static result_type Reduce(Values&& values) {
    return Reduce(values, std::index_sequence_for<Ts...>{});
  }

 private:
  template <std::ranges::forward_range Values, std::size_t... I>
  static result_type Reduce(Values&& values, std::index_sequence<I...>) {
    return result_type{ParallelReduce<std::tuple_element_t<I, std::tuple<Ts...>>>::Reduce(
        values | std::views::elements<I>)...};
  }
};

template <auto... Options, typename... Args>
  requires(sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0 &&
           []<std::size_t... I>(std::index_sequence<I...>) consteval {
             return (internal::ParallelCollectible<
                         typename internal::ParallelCollectTraits<I, Args...>::input_type,
                         typename internal::ParallelCollectTraits<I, Args...>::task_type,
                         Options...> &&
                     ...);
           }(std::make_index_sequence<sizeof...(Args) / 2>{}))
auto ParallelCollect(OptionalExecutor executor, Args&&... args) {
  constexpr std::size_t pair_count = sizeof...(Args) / 2;
  using indices = std::make_index_sequence<pair_count>;

  auto args_tuple = std::forward_as_tuple(std::forward<Args>(args)...);

  auto values_tuple = [&]<std::size_t... I>(std::index_sequence<I...>) {
    return std::tuple{[&] {
      using traits = internal::ParallelCollectTraits<I, Args...>;

      return std::vector<typename traits::value_type>(
          std::ranges::size(std::get<I * 2>(args_tuple)));
    }()...};
  }(indices{});

  auto reduce_all = [&]<std::size_t... I>(std::index_sequence<I...>) {
    auto reduce_one = [&]<std::size_t PairIndex> {
      using traits = internal::ParallelCollectTraits<PairIndex, Args...>;
      using value_type = typename traits::value_type;
      return ParallelReduce<value_type, Options...>::Reduce(
          std::get<PairIndex>(values_tuple));
    };

    if constexpr (pair_count == 1) {
      return reduce_one.template operator()<0>();
    } else {
      return std::tuple{reduce_one.template operator()<I>()...};
    }
  };

  using result_type = decltype(reduce_all(indices{}));

  TaskGroup group;
  group.SetExecutor(executor);

  [&]<std::size_t... I>(std::index_sequence<I...>) {
    (
        [&] {
          for (auto&& [item, value] :
               std::views::zip(std::get<I * 2>(args_tuple), std::get<I>(values_tuple))) {
            group.Submit([&]() -> Status {
              ICEBERG_ASSIGN_OR_RAISE(value,
                                      std::invoke(std::get<I * 2 + 1>(args_tuple), item));
              return {};
            });
          }
        }(),
        ...);
  }(indices{});

  auto status = std::move(group).Run();
  if (!status.has_value()) {
    return Result<result_type>(std::unexpected<Error>(status.error()));
  }

  return Result<result_type>(reduce_all(indices{}));
}

}  // namespace iceberg
