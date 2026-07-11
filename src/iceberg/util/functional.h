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

// Borrowed the file from Apache Arrow:
// https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/functional.h

#pragma once

/// \file iceberg/util/functional.h
/// \brief Provide move-only function wrappers.

#include <concepts>
#include <functional>
#include <memory>
#include <type_traits>
#include <utility>

#include "iceberg/iceberg_export.h"

namespace iceberg {

namespace internal {

template <typename R, typename Fn, typename... Args>
concept RvalueInvocable = std::constructible_from<std::remove_cvref_t<Fn>, Fn> &&
                          std::move_constructible<std::remove_cvref_t<Fn>> &&
                          std::is_invocable_r_v<R, std::remove_cvref_t<Fn>&&, Args...>;

}  // namespace internal

template <typename Signature>
class FnOnce;

template <typename R, typename... Args>
class ICEBERG_TEMPLATE_CLASS_EXPORT FnOnce<R(Args...)> {
 public:
  template <typename Fn>
    requires(!std::same_as<std::remove_cvref_t<Fn>, FnOnce> &&
             internal::RvalueInvocable<R, Fn, Args...>)
  explicit FnOnce(Fn&& fn) : impl_(std::make_unique<ImplFor<Fn>>(std::forward<Fn>(fn))) {}

  FnOnce(FnOnce&&) noexcept = default;
  FnOnce& operator=(FnOnce&&) noexcept = default;
  FnOnce(const FnOnce&) = delete;
  FnOnce& operator=(const FnOnce&) = delete;

  R operator()(Args... args) && {
    return std::move(*impl_).Invoke(std::forward<Args>(args)...);
  }

 private:
  struct Impl {
    virtual ~Impl() = default;
    virtual R Invoke(Args&&... args) && = 0;
  };

  template <typename Fn>
  struct ImplFor final : Impl {
    explicit ImplFor(Fn&& fn) : fn_(std::forward<Fn>(fn)) {}
    R Invoke(Args&&... args) && override {
      return std::invoke(std::move(fn_), std::forward<Args>(args)...);
    }
    std::remove_cvref_t<Fn> fn_;
  };

  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
