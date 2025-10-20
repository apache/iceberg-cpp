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

/// \file iceberg/util/lazy.h
/// Lazy initialization utility.

#include <concepts>
#include <functional>
#include <mutex>

#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

template <auto InitFunc>
class Lazy {
  template <typename R>
  struct Trait;

  template <typename R, typename... Args>
  struct Trait<R (*)(Args...)> {
    using ReturnType = R::value_type;
  };

  using T = Trait<decltype(InitFunc)>::ReturnType;

 public:
  template <typename... Args>
    requires std::invocable<decltype(InitFunc), Args...> &&
             std::same_as<std::invoke_result_t<decltype(InitFunc), Args...>, Result<T>>
  Result<std::reference_wrapper<T>> Get(Args&&... args) const {
    Result<T> result;
    std::call_once(flag_, [&result, this, &args...]() {
      result = InitFunc(std::forward<Args>(args)...);
      if (result) {
        this->value_ = std::move(result.value());
      }
    });
    ICEBERG_RETURN_UNEXPECTED(result);
    return std::ref(value_);
  }

 private:
  mutable T value_;
  mutable std::once_flag flag_;
};

};  // namespace iceberg
