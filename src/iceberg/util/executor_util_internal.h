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

template <typename Ref>
using ParallelCollectArgT =
    std::conditional_t<std::is_lvalue_reference_v<Ref>, Ref, std::remove_cvref_t<Ref>&&>;

template <typename T, auto... Options>
concept ParallelReducible = requires(std::vector<T>& values) {
  typename ParallelReduce<T, Options...>::result_type;
  {
    ParallelReduce<T, Options...>::Reduce(values)
  } -> std::same_as<typename ParallelReduce<T, Options...>::result_type>;
};

template <std::ranges::input_range InputRange, typename Task>
using ParallelCollectValueT = ResultValueT<std::invoke_result_t<
    std::remove_reference_t<Task>&,
    ParallelCollectArgT<std::ranges::range_reference_t<InputRange>>>>;

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
    (std::is_lvalue_reference_v<std::ranges::range_reference_t<InputRange>> ||
     std::constructible_from<
         std::remove_cvref_t<std::ranges::range_reference_t<InputRange>>,
         std::ranges::range_reference_t<InputRange>>) &&
    requires(std::remove_reference_t<Task>& task,
             ParallelCollectArgT<std::ranges::range_reference_t<InputRange>> item) {
      { std::invoke(task, std::forward<decltype(item)>(item)) } -> AsResult;
      requires(!std::same_as<void, ParallelCollectValueT<InputRange, Task>>);
      requires std::default_initializable<ParallelCollectValueT<InputRange, Task>>;
      requires ParallelReducible<ParallelCollectValueT<InputRange, Task>, Options...>;
    };

// Checked through a class template rather than a lambda in the requires-clause because
// older MSVC versions (e.g. 14.42) cannot expand the Args pack inside such a lambda.
template <typename Indices, typename ArgsTuple, auto... Options>
struct ParallelCollectibleArgs : std::false_type {};

template <std::size_t... I, typename... Args, auto... Options>
struct ParallelCollectibleArgs<std::index_sequence<I...>, std::tuple<Args...>, Options...>
    : std::bool_constant<(
          ParallelCollectible<typename ParallelCollectTraits<I, Args...>::input_type,
                              typename ParallelCollectTraits<I, Args...>::task_type,
                              Options...> &&
          ...)> {};

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

// The helpers below replace lambdas that were expanded over the pair index pack, which
// older MSVC versions (e.g. 14.42) cannot compile.
namespace internal {

template <typename ArgsTuple, std::size_t... I>
auto MakeParallelCollectValues(ArgsTuple& args_tuple, std::index_sequence<I...>) {
  return std::tuple{
      std::vector<ParallelCollectValueT<std::tuple_element_t<I * 2, ArgsTuple>,
                                        std::tuple_element_t<I * 2 + 1, ArgsTuple>>>(
          std::ranges::size(std::get<I * 2>(args_tuple)))...};
}

template <auto... Options, typename ValuesTuple, std::size_t... I>
auto ReduceParallelCollectValues(ValuesTuple& values_tuple, std::index_sequence<I...>) {
  if constexpr (sizeof...(I) == 1) {
    return ParallelReduce<typename std::tuple_element_t<0, ValuesTuple>::value_type,
                          Options...>::Reduce(std::get<0>(values_tuple));
  } else {
    return std::tuple{
        ParallelReduce<typename std::tuple_element_t<I, ValuesTuple>::value_type,
                       Options...>::Reduce(std::get<I>(values_tuple))...};
  }
}

template <std::size_t I, typename Group, typename ArgsTuple, typename ValuesTuple>
void SubmitParallelCollectPair(Group& group, ArgsTuple& args_tuple,
                               ValuesTuple& values_tuple) {
  using item_ref = std::ranges::range_reference_t<std::tuple_element_t<I * 2, ArgsTuple>>;

  for (auto&& [item, value] :
       std::views::zip(std::get<I * 2>(args_tuple), std::get<I>(values_tuple))) {
    if constexpr (std::is_lvalue_reference_v<item_ref>) {
      group.Submit([&]() -> Status {
        ICEBERG_ASSIGN_OR_RAISE(value,
                                std::invoke(std::get<I * 2 + 1>(args_tuple), item));
        return {};
      });
    } else {
      group.Submit([&, item = std::move(item)]() mutable -> Status {
        ICEBERG_ASSIGN_OR_RAISE(
            value, std::invoke(std::get<I * 2 + 1>(args_tuple), std::move(item)));
        return {};
      });
    }
  }
}

template <typename Group, typename ArgsTuple, typename ValuesTuple, std::size_t... I>
void SubmitParallelCollectTasks(Group& group, ArgsTuple& args_tuple,
                                ValuesTuple& values_tuple, std::index_sequence<I...>) {
  (SubmitParallelCollectPair<I>(group, args_tuple, values_tuple), ...);
}

}  // namespace internal

template <auto... Options, typename... Args>
  requires(
      sizeof...(Args) >= 2 && sizeof...(Args) % 2 == 0 &&
      internal::ParallelCollectibleArgs<std::make_index_sequence<sizeof...(Args) / 2>,
                                        std::tuple<Args...>, Options...>::value)
auto ParallelCollect(OptionalExecutor executor, Args&&... args) {
  constexpr std::size_t pair_count = sizeof...(Args) / 2;
  using indices = std::make_index_sequence<pair_count>;

  auto args_tuple = std::forward_as_tuple(std::forward<Args>(args)...);
  auto values_tuple = internal::MakeParallelCollectValues(args_tuple, indices{});

  using result_type = decltype(internal::ReduceParallelCollectValues<Options...>(
      values_tuple, indices{}));

  TaskGroup group;
  group.SetExecutor(executor);
  internal::SubmitParallelCollectTasks(group, args_tuple, values_tuple, indices{});

  auto status = std::move(group).Run();
  if (!status.has_value()) {
    return Result<result_type>(::iceberg::unexpected<Error>(status.error()));
  }

  return Result<result_type>(
      internal::ReduceParallelCollectValues<Options...>(values_tuple, indices{}));
}

}  // namespace iceberg
