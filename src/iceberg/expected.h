/*
 * MIT License
 *
 * Copyright (c) 2024 zeus-cpp
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#pragma once

#include <exception>
#include <functional>
#include <type_traits>
#include <utility>

#include "iceberg/iceberg_export.h"

// NOLINTBEGIN

namespace iceberg {

namespace expected_detail {

template <class T, template <class...> class Template>
inline constexpr bool is_specialization_v =
    false;  // true if and only if T is a specialization of Template
template <template <class...> class Template, class... Types>
inline constexpr bool is_specialization_v<Template<Types...>, Template> = true;

template <class T, class U>
using is_void_or = std::conditional_t<std::is_void_v<T>, std::true_type, U>;
template <class T, class U>
inline constexpr bool is_void_or_v = is_void_or<T, U>::value;

template <class T>
inline constexpr bool is_trivially_destructible_or_void_v =
    is_void_or_v<T, std::is_trivially_destructible<T>>;

template <class T>
inline constexpr bool is_trivially_copy_constructible_or_void_v =
    is_void_or_v<T, std::is_trivially_copy_constructible<T>>;

template <class T>
inline constexpr bool is_trivially_move_constructible_or_void_v =
    is_void_or_v<T, std::is_trivially_move_constructible<T>>;

template <class T>
inline constexpr bool is_trivially_copy_assignable_or_void_v =
    is_void_or_v<T, std::is_trivially_copy_assignable<T>>;

template <class T>
inline constexpr bool is_trivially_move_assignable_or_void_v =
    is_void_or_v<T, std::is_trivially_move_assignable<T>>;

template <class T>
inline constexpr bool is_nothrow_copy_constructible_or_void_v =
    is_void_or_v<T, std::is_nothrow_copy_constructible<T>>;

template <class T>
inline constexpr bool is_nothrow_move_constructible_or_void_v =
    is_void_or_v<T, std::is_nothrow_move_constructible<T>>;

template <class T>
inline constexpr bool is_default_constructible_or_void_v =
    is_void_or_v<T, std::is_default_constructible<T>>;

template <class T>
inline constexpr bool is_copy_constructible_or_void_v =
    is_void_or_v<T, std::is_copy_constructible<T>>;

template <class T>
inline constexpr bool is_move_constructible_or_void_v =
    is_void_or_v<T, std::is_move_constructible<T>>;

template <class T>
inline constexpr bool is_copy_assignable_or_void_v =
    is_void_or_v<T, std::is_copy_assignable<T>>;

template <class T>
inline constexpr bool is_move_assignable_or_void_v =
    is_void_or_v<T, std::is_move_assignable<T>>;

template <class From, class To>
inline constexpr bool is_nothrow_convertible_v =
    noexcept(static_cast<To>(std::declval<From>()));

}  // namespace expected_detail

template <class E>
class unexpected;

namespace expected_detail {

template <class E>
struct is_error_type_valid : std::true_type {
  static_assert(std::is_object_v<E>, "E must be an object type");
  static_assert(!std::is_array_v<E>, "E must not be an array");
  static_assert(!std::is_const_v<E>, "E must not be const");
  static_assert(!std::is_volatile_v<E>, "E must not be volatile");
  static_assert(!expected_detail::is_specialization_v<std::remove_cv_t<E>, unexpected>,
                "E must not be unexpected");
};
template <class T>
inline constexpr bool is_error_type_valid_v = is_error_type_valid<T>::value;

}  // namespace expected_detail

template <class E>
class [[nodiscard]] unexpected {
 public:
  static_assert(expected_detail::is_error_type_valid_v<E>);

  unexpected() = delete;
  ~unexpected() = default;

  unexpected(const unexpected&) = default;
  unexpected(unexpected&&) = default;

  template <class... Args,
            typename std::enable_if_t<std::is_constructible_v<E, Args...>>* = nullptr>
  constexpr explicit unexpected(std::in_place_t, Args&&... args) noexcept(
      std::is_nothrow_constructible_v<E, Args...>)
      : m_val(std::forward<Args>(args)...) {}

  template <class U, class... Args,
            typename std::enable_if_t<std::is_constructible_v<
                E, std::initializer_list<U>&, Args...>>* = nullptr>
  constexpr explicit unexpected(
      std::in_place_t, std::initializer_list<U> l,
      Args&&... args) noexcept(std::is_nothrow_constructible_v<E,
                                                               std::initializer_list<U>&,
                                                               Args...>)
      : m_val(l, std::forward<Args>(args)...) {}

  template <class Err = E,
            typename std::enable_if_t<std::is_constructible_v<E, Err>>* = nullptr>
  constexpr explicit unexpected(Err&& e) noexcept(std::is_nothrow_constructible_v<E, Err>)
      : m_val(std::forward<Err>(e)) {}

  unexpected& operator=(const unexpected&) = default;
  unexpected& operator=(unexpected&&) = default;

  constexpr const E& error() const& noexcept { return m_val; }
  constexpr E& error() & noexcept { return m_val; }
  constexpr E&& error() && noexcept { return std::move(m_val); }
  constexpr const E&& error() const&& noexcept { return std::move(m_val); }

  constexpr void swap(unexpected& other) noexcept(std::is_nothrow_swappable_v<E>) {
    static_assert(std::is_swappable_v<E>, "E must be swappable");
    using std::swap;
    swap(m_val, other.m_val);
  }

  friend constexpr void swap(unexpected& x, unexpected& y) noexcept(noexcept(x.swap(y))) {
    x.swap(y);
  }

  template <class E2>
  friend constexpr bool operator==(
      const unexpected& lhs,
      const unexpected<E2>& rhs) noexcept(noexcept(lhs.m_val == rhs.error())) {
    return lhs.m_val == rhs.error();
  }
  template <class E2>
  friend constexpr bool operator!=(
      const unexpected& lhs,
      const unexpected<E2>& rhs) noexcept(noexcept(lhs.m_val != rhs.error())) {
    return lhs.m_val != rhs.error();
  }

 private:
  E m_val;
};

// deduction guide
template <class E>
unexpected(E) -> unexpected<E>;

struct unexpect_t {
  explicit unexpect_t() = default;
};
inline constexpr unexpect_t unexpect{};

template <class E>
class bad_expected_access;

template <>
class bad_expected_access<void> : public std::exception {
 public:
  const char* what() const noexcept override { return "Bad expected access"; }

 protected:
  bad_expected_access() = default;
  bad_expected_access(const bad_expected_access&) = default;
  bad_expected_access(bad_expected_access&&) = default;
  bad_expected_access& operator=(const bad_expected_access&) = default;
  bad_expected_access& operator=(bad_expected_access&&) = default;
};

template <class E>
class bad_expected_access : public bad_expected_access<void> {
 public:
  explicit bad_expected_access(E e) noexcept(std::is_nothrow_move_constructible_v<E>)
      : m_val(std::move(e)) {}

  const E& error() const& noexcept { return m_val; }
  E& error() & noexcept { return m_val; }
  const E&& error() const&& noexcept { return std::move(m_val); }
  E&& error() && noexcept { return std::move(m_val); }

 private:
  E m_val;
};

template <class T, class E>
class expected;

namespace expected_detail {
template <class T>

struct is_value_type_valid : std::true_type {
  static_assert(!std::is_reference_v<T>, "T must not be a reference");
  static_assert(!std::is_function_v<T>, "T must not be a function");
  static_assert(!std::is_array_v<T>,
                "T must not be an array to meet the requirements of Cpp17Destructible "
                "when T is not cv-void");
  static_assert(!std::is_same_v<T, std::remove_cv_t<std::in_place_t>>,
                "T must not be in_place_t");
  static_assert(!std::is_same_v<T, std::remove_cv_t<unexpect_t>>,
                "T must not be unexpect_t");
  static_assert(!expected_detail::is_specialization_v<std::remove_cv_t<T>, unexpected>,
                "T must not be unexpected");
};

template <class T>
inline constexpr bool is_value_type_valid_v = is_value_type_valid<T>::value;

template <class T, class E, class U>
using enable_forward_t = std::enable_if_t<
    std::is_constructible_v<T, U> &&
    !std::is_same_v<std::remove_cvref_t<U>, std::in_place_t> &&
    !std::is_same_v<expected<T, E>, std::remove_cvref_t<U>> &&
    !expected_detail::is_specialization_v<std::remove_cvref_t<U>, unexpected> &&
    (!std::is_same_v<std::remove_cv_t<T>, bool> ||  // LWG-3836
     !expected_detail::is_specialization_v<std::remove_cvref_t<U>, expected>)>;

template <class T, class E, class U, class G, class UF, class GF>
using enable_from_other_expected_t = std::enable_if_t<
    std::is_constructible_v<T, UF> && std::is_constructible_v<E, GF> &&
    std::disjunction_v<std::is_same<std::remove_cv_t<T>, bool>,  // LWG-3836
                       std::negation<std::disjunction<
                           std::is_constructible<T, expected<U, G>&>,
                           std::is_constructible<T, expected<U, G>>,
                           std::is_constructible<T, const expected<U, G>&>,
                           std::is_constructible<T, const expected<U, G>>,
                           std::is_convertible<expected<U, G>&, T>,
                           std::is_convertible<expected<U, G>&&, T>,
                           std::is_convertible<const expected<U, G>&, T>,
                           std::is_convertible<const expected<U, G>&&, T>,
                           std::is_constructible<unexpected<E>, expected<U, G>&>,
                           std::is_constructible<unexpected<E>, expected<U, G>>,
                           std::is_constructible<unexpected<E>, const expected<U, G>&>,
                           std::is_constructible<unexpected<E>, const expected<U, G>>>>>>;

template <class E, class U, class G, class GF>
using enable_from_other_void_expected_t =
    std::enable_if_t<std::is_void_v<U> && std::is_constructible_v<E, GF> &&
                     std::negation_v<std::disjunction<
                         std::is_constructible<unexpected<E>, expected<U, G>&>,
                         std::is_constructible<unexpected<E>, expected<U, G>>,
                         std::is_constructible<unexpected<E>, const expected<U, G>&>,
                         std::is_constructible<unexpected<E>, const expected<U, G>>>>>;

}  // namespace expected_detail

namespace expected_detail {

struct no_init_t {
  explicit no_init_t() = default;
};
inline constexpr no_init_t no_init{};

struct construct_with_invoke_result_t {
  explicit construct_with_invoke_result_t() = default;
};
inline constexpr construct_with_invoke_result_t construct_with_invoke_result{};

template <class T>
struct [[nodiscard]] ReinitGuard {
  static_assert(std::is_nothrow_move_constructible_v<T>);

  ReinitGuard(ReinitGuard const&) = delete;
  ReinitGuard& operator=(ReinitGuard const&) = delete;

  constexpr ReinitGuard(T* target, T* tmp) noexcept : _target(target), _tmp(tmp) {}
  constexpr ~ReinitGuard() noexcept {
    if (_target) {
      std::construct_at(_target, std::move(*_tmp));
    }
  }
  T* _target;
  T* _tmp;
};

template <class First, class Second, class... Args>
constexpr void reinit_expected(First& new_val, Second& old_val, Args&&... args) noexcept(
    std::is_nothrow_constructible_v<First, Args...>) {
  if constexpr (std::is_nothrow_constructible_v<First, Args...>) {
    if constexpr (!std::is_trivially_destructible_v<Second>) {
      old_val.~Second();
    }
    std::construct_at(std::addressof(new_val), std::forward<Args>(args)...);
  } else if constexpr (std::is_nothrow_move_constructible_v<First>) {
    First tmp(std::forward<Args>(args)...);
    if constexpr (!std::is_trivially_destructible_v<Second>) {
      old_val.~Second();
    }
    std::construct_at(std::addressof(new_val), std::move(tmp));
  } else {
    Second tmp(std::move(old_val));
    if constexpr (!std::is_trivially_destructible_v<Second>) {
      old_val.~Second();
    }

    expected_detail::ReinitGuard<Second> guard{std::addressof(old_val),
                                               std::addressof(tmp)};
    std::construct_at(std::addressof(new_val), std::forward<Args>(args)...);
    guard._target = nullptr;
  }
}

// Implements the storage of the values, and ensures that the destructor is
// trivial if it can be.
//
// This specialization is for when `T` and `E` are trivially destructible
template <class T, class E,
          bool = is_trivially_destructible_or_void_v<T> &&
                 std::is_trivially_destructible_v<E>>
struct storage_base {
  storage_base(storage_base const&) = default;
  storage_base(storage_base&&) = default;
  storage_base& operator=(storage_base const&) = default;
  storage_base& operator=(storage_base&&) = default;

  constexpr storage_base() noexcept(noexcept(T{})) : m_val(T{}), m_has_val(true) {}
  constexpr storage_base(no_init_t) noexcept : m_no_init(), m_has_val(false) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>>* = nullptr>
  constexpr explicit storage_base(std::in_place_t, Args&&... args) noexcept(
      noexcept(T(std::forward<Args>(args)...)))
      : m_val(std::forward<Args>(args)...), m_has_val(true) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<T, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(
      std::in_place_t, std::initializer_list<U> il,
      Args&&... args) noexcept(noexcept(T(il, std::forward<Args>(args)...)))
      : m_val(il, std::forward<Args>(args)...), m_has_val(true) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, Args&&... args) noexcept(
      noexcept(E(std::forward<Args>(args)...)))
      : m_unexpect(std::forward<Args>(args)...), m_has_val(false) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(
      unexpect_t, std::initializer_list<U> il,
      Args&&... args) noexcept(noexcept(E(il, std::forward<Args>(args)...)))
      : m_unexpect(il, std::forward<Args>(args)...), m_has_val(false) {}

  // helper ctor for transform()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<T>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_val(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(true) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, unexpect_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_unexpect(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(false) {}

  ~storage_base() = default;

  union {
    T m_val;
    E m_unexpect;
    char m_no_init;
  };
  bool m_has_val;
};

template <class T, class E>
struct storage_base<T, E, false> {
  storage_base(storage_base const&) = default;
  storage_base(storage_base&&) = default;
  storage_base& operator=(storage_base const&) = default;
  storage_base& operator=(storage_base&&) = default;

  constexpr storage_base() noexcept(noexcept(T{})) : m_val(T{}), m_has_val(true) {}
  constexpr storage_base(no_init_t) noexcept : m_no_init(), m_has_val(false) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<T, Args&&...>>* = nullptr>
  constexpr explicit storage_base(std::in_place_t, Args&&... args) noexcept(
      noexcept(T(std::forward<Args>(args)...)))
      : m_val(std::forward<Args>(args)...), m_has_val(true) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<T, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(
      std::in_place_t, std::initializer_list<U> il,
      Args&&... args) noexcept(noexcept(T(il, std::forward<Args>(args)...)))
      : m_val(il, std::forward<Args>(args)...), m_has_val(true) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, Args&&... args) noexcept(
      noexcept(E(std::forward<Args>(args)...)))
      : m_unexpect(std::forward<Args>(args)...), m_has_val(false) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(
      unexpect_t, std::initializer_list<U> il,
      Args&&... args) noexcept(noexcept(E(il, std::forward<Args>(args)...)))
      : m_unexpect(il, std::forward<Args>(args)...), m_has_val(false) {}

  // helper ctor for transform()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<T>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_val(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(true) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, unexpect_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_unexpect(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(false) {}

  constexpr ~storage_base() noexcept {
    if (m_has_val) {
      if constexpr (!std::is_trivially_destructible_v<T>) {
        m_val.~T();
      }
    } else {
      if constexpr (!std::is_trivially_destructible_v<E>) {
        m_unexpect.~E();
      }
    }
  }

  union {
    T m_val;
    E m_unexpect;
    char m_no_init;
  };
  bool m_has_val;
};

// `T` is `void`, `E` is trivially-destructible
template <class E>
struct storage_base<void, E, true> {
  storage_base(storage_base const&) = default;
  storage_base(storage_base&&) = default;
  storage_base& operator=(storage_base const&) = default;
  storage_base& operator=(storage_base&&) = default;

  constexpr storage_base() noexcept : m_has_val(true) {}

  constexpr storage_base(no_init_t) noexcept : m_val(), m_has_val(false) {}

  constexpr explicit storage_base(std::in_place_t) noexcept : m_has_val(true) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, Args&&... args)
      : m_unexpect(std::forward<Args>(args)...), m_has_val(false) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, std::initializer_list<U> il, Args&&... args)
      : m_unexpect(il, std::forward<Args>(args)...), m_has_val(false) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, unexpect_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_unexpect(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(false) {}

  ~storage_base() = default;

  struct dummy {};

  union {
    E m_unexpect;
    dummy m_val;
  };

  bool m_has_val;
};

// `T` is `void`, `E` is not trivially-destructible
template <class E>
struct storage_base<void, E, false> {
  storage_base(storage_base const&) = default;
  storage_base(storage_base&&) = default;
  storage_base& operator=(storage_base const&) = default;
  storage_base& operator=(storage_base&&) = default;

  constexpr storage_base() noexcept : m_has_val(true) {}

  constexpr storage_base(no_init_t) noexcept : m_val(), m_has_val(false) {}

  constexpr explicit storage_base(std::in_place_t) noexcept : m_has_val(true) {}

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, Args&&... args)
      : m_unexpect(std::forward<Args>(args)...), m_has_val(false) {}

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args&&...>>* = nullptr>
  constexpr explicit storage_base(unexpect_t, std::initializer_list<U> il, Args&&... args)
      : m_unexpect(il, std::forward<Args>(args)...), m_has_val(false) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit storage_base(construct_with_invoke_result_t, unexpect_t, Fn&& func,
                                  Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : m_unexpect(std::invoke(std::forward<Fn>(func), std::forward<Args>(args)...)),
        m_has_val(false) {}

  constexpr ~storage_base() noexcept {
    if (!m_has_val) {
      m_unexpect.~E();
    }
  }

  struct dummy {};

  union {
    E m_unexpect;
    dummy m_val;
  };

  bool m_has_val;
};

// This base class provides some handy member functions which can be used in
// further derived classes
template <class T, class E>
struct operations_base : storage_base<T, E> {
  using storage_base<T, E>::storage_base;

  template <class... Args>
  constexpr void construct(Args&&... args) noexcept(
      std::is_nothrow_constructible_v<T, Args...>) {
    std::construct_at(&this->m_val, std::forward<Args>(args)...);
    this->m_has_val = true;
  }

  template <class Rhs>
  constexpr void construct_with(Rhs&& rhs) noexcept(
      std::is_nothrow_constructible_v<T, Rhs>) {
    std::construct_at(&this->m_val, std::forward<Rhs>(rhs).get());
    this->m_has_val = true;
  }

  template <class... Args>
  constexpr void construct_error(Args&&... args) noexcept(
      std::is_nothrow_constructible_v<E, Args...>) {
    std::construct_at(&this->m_unexpect, std::forward<Args>(args)...);
    this->m_has_val = false;
  }

  constexpr T& get() & noexcept { return this->m_val; }
  constexpr const T& get() const& noexcept { return this->m_val; }
  constexpr T&& get() && noexcept(std::is_nothrow_move_constructible_v<T>) {
    return std::move(this->m_val);
  }
  constexpr const T&& get() const&& noexcept(std::is_nothrow_move_constructible_v<T>) {
    return std::move(this->m_val);
  }

  constexpr E& geterr() & noexcept { return this->m_unexpect; }
  constexpr const E& geterr() const& noexcept { return this->m_unexpect; }
  constexpr E&& geterr() && noexcept(std::is_nothrow_move_constructible_v<E>) {
    return std::move(this->m_unexpect);
  }
  constexpr const E&& geterr() const&& noexcept(std::is_nothrow_move_constructible_v<E>) {
    return std::move(this->m_unexpect);
  }
};

// This base class provides some handy member functions which can be used in
// further derived classes
template <class E>
struct operations_base<void, E> : storage_base<void, E> {
  using storage_base<void, E>::storage_base;

  constexpr void construct() noexcept { this->m_has_val = true; }

  // This function doesn't use its argument, but needs it so that code in
  // levels above this can work independently of whether T is void
  template <class Rhs>
  constexpr void construct_with(Rhs&&) noexcept {
    this->m_has_val = true;
  }

  template <class... Args>
  constexpr void construct_error(Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<E, Args...>) {
    std::construct_at(&this->m_unexpect, std::forward<Args>(args)...);
    this->m_has_val = false;
  }

  constexpr E& geterr() & noexcept { return this->m_unexpect; }
  constexpr const E& geterr() const& noexcept { return this->m_unexpect; }
  constexpr E&& geterr() && noexcept(std::is_nothrow_move_constructible_v<E>) {
    return std::move(this->m_unexpect);
  }
  constexpr const E&& geterr() const&& noexcept(std::is_nothrow_move_constructible_v<E>) {
    return std::move(this->m_unexpect);
  }
};

// This class manages conditionally having a trivial copy constructor
// This specialization is for when T and E are trivially copy constructible
template <class T, class E,
          bool Enabled =
              is_copy_constructible_or_void_v<T> && std::is_copy_constructible_v<E>,
          bool Trivially = is_trivially_copy_constructible_or_void_v<T> &&
                           std::is_trivially_copy_constructible_v<E>>
struct copy_ctor_base : operations_base<T, E> {
  using operations_base<T, E>::operations_base;
};

// This specialization is for when T or E are not copy constructible
template <class T, class E>
struct copy_ctor_base<T, E, false, false> : operations_base<T, E> {
  using operations_base<T, E>::operations_base;

  ~copy_ctor_base() = default;
  copy_ctor_base() = default;
  copy_ctor_base(const copy_ctor_base& rhs) = delete;
  copy_ctor_base(copy_ctor_base&& rhs) = default;
  copy_ctor_base& operator=(const copy_ctor_base& rhs) = default;
  copy_ctor_base& operator=(copy_ctor_base&& rhs) = default;
};

// This specialization is for when T or E are not trivially copy constructible
template <class T, class E>
struct copy_ctor_base<T, E, true, false> : operations_base<T, E> {
  using operations_base<T, E>::operations_base;

  ~copy_ctor_base() = default;
  copy_ctor_base() = default;

  constexpr copy_ctor_base(const copy_ctor_base& rhs)  //
      noexcept(is_nothrow_copy_constructible_or_void_v<T> &&
               std::is_nothrow_copy_constructible_v<E>)
      : operations_base<T, E>(no_init) {
    if (rhs.m_has_val) {
      this->construct_with(rhs);
    } else {
      this->construct_error(rhs.geterr());
    }
  }

  copy_ctor_base(copy_ctor_base&& rhs) = default;
  copy_ctor_base& operator=(const copy_ctor_base& rhs) = default;
  copy_ctor_base& operator=(copy_ctor_base&& rhs) = default;
};

// This class manages conditionally having a trivial move constructor
template <class T, class E,
          bool Enabled =
              is_move_constructible_or_void_v<T> && std::is_move_constructible_v<E>,
          bool Trivially = is_trivially_move_constructible_or_void_v<T> &&
                           std::is_trivially_move_constructible_v<E>>
struct move_ctor_base : copy_ctor_base<T, E> {
  using copy_ctor_base<T, E>::copy_ctor_base;
};

// This specialization is for when T or E are not move constructible
template <class T, class E>
struct move_ctor_base<T, E, false, false> : copy_ctor_base<T, E> {
  using copy_ctor_base<T, E>::copy_ctor_base;

  ~move_ctor_base() = default;
  move_ctor_base() = default;
  move_ctor_base(const move_ctor_base& rhs) = default;
  move_ctor_base(move_ctor_base&& rhs) = delete;
  move_ctor_base& operator=(const move_ctor_base& rhs) = default;
  move_ctor_base& operator=(move_ctor_base&& rhs) = default;
};

// This specialization is for when T or E are not trivially move constructible
template <class T, class E>
struct move_ctor_base<T, E, true, false> : copy_ctor_base<T, E> {
  using copy_ctor_base<T, E>::copy_ctor_base;

  ~move_ctor_base() = default;
  move_ctor_base() = default;
  move_ctor_base(const move_ctor_base& rhs) = default;

  constexpr move_ctor_base(move_ctor_base&& rhs)  //
      noexcept(is_nothrow_move_constructible_or_void_v<T> &&
               std::is_nothrow_move_constructible_v<E>)
      : copy_ctor_base<T, E>(no_init) {
    if (rhs.m_has_val) {
      this->construct_with(std::move(rhs));
    } else {
      this->construct_error(std::move(rhs.geterr()));
    }
  }

  move_ctor_base& operator=(const move_ctor_base& rhs) = default;
  move_ctor_base& operator=(move_ctor_base&& rhs) = default;
};

template <class T, class E>
inline constexpr bool is_expected_copy_assignable_v =
    is_copy_assignable_or_void_v<T> && is_copy_constructible_or_void_v<T> &&
    std::is_copy_assignable_v<E> && std::is_copy_constructible_v<E> &&
    (is_nothrow_move_constructible_or_void_v<T> ||
     std::is_nothrow_move_constructible_v<E>);

// LWG-4026
template <class T, class E>
inline constexpr bool is_expected_trivially_copy_assignable_v =
    is_trivially_copy_constructible_or_void_v<T> &&
    is_trivially_copy_assignable_or_void_v<T> && is_trivially_destructible_or_void_v<T> &&
    std::is_trivially_copy_constructible_v<E> && std::is_trivially_copy_assignable_v<E> &&
    std::is_trivially_destructible_v<E>;

// This class manages conditionally having a (possibly trivial) copy assignment operator
template <class T, class E, bool Enabled = is_expected_copy_assignable_v<T, E>,
          bool Trivially = is_expected_trivially_copy_assignable_v<T, E>>
struct copy_assign_base : move_ctor_base<T, E> {
  using move_ctor_base<T, E>::move_ctor_base;

  ~copy_assign_base() = default;
  copy_assign_base() = default;
  copy_assign_base(const copy_assign_base& rhs) = default;
  copy_assign_base(copy_assign_base&& rhs) = default;
  copy_assign_base& operator=(const copy_assign_base& rhs) = default;
  copy_assign_base& operator=(copy_assign_base&& rhs) = default;
};

template <class T, class E>
struct copy_assign_base<T, E, false, false> : move_ctor_base<T, E> {
  using move_ctor_base<T, E>::move_ctor_base;

  ~copy_assign_base() = default;
  copy_assign_base() = default;
  copy_assign_base(const copy_assign_base& rhs) = default;
  copy_assign_base(copy_assign_base&& rhs) = default;
  copy_assign_base& operator=(const copy_assign_base& rhs) = delete;
  copy_assign_base& operator=(copy_assign_base&& rhs) = default;
};

template <class T, class E>
struct copy_assign_base<T, E, true, false> : move_ctor_base<T, E> {
  using move_ctor_base<T, E>::move_ctor_base;

  ~copy_assign_base() = default;
  copy_assign_base() = default;
  copy_assign_base(const copy_assign_base& rhs) = default;
  copy_assign_base(copy_assign_base&& rhs) = default;

  constexpr copy_assign_base& operator=(const copy_assign_base& rhs)  //
      noexcept(std::is_nothrow_copy_constructible_v<T> &&
               std::is_nothrow_copy_assignable_v<T> &&
               std::is_nothrow_copy_constructible_v<E> &&
               std::is_nothrow_copy_assignable_v<E>) {
    if (this->m_has_val && rhs.m_has_val) {
      this->m_val = rhs.m_val;
    } else if (this->m_has_val) {
      expected_detail::reinit_expected(this->m_unexpect, this->m_val, rhs.m_unexpect);
    } else if (rhs.m_has_val) {
      expected_detail::reinit_expected(this->m_val, this->m_unexpect, rhs.m_val);
    } else {
      this->m_unexpect = rhs.m_unexpect;
    }
    this->m_has_val = rhs.m_has_val;
    return *this;
  }

  copy_assign_base& operator=(copy_assign_base&& rhs) = default;
};

template <class E>
struct copy_assign_base<void, E, true, false> : move_ctor_base<void, E> {
  using move_ctor_base<void, E>::move_ctor_base;

  ~copy_assign_base() = default;
  copy_assign_base() = default;
  copy_assign_base(const copy_assign_base& rhs) = default;
  copy_assign_base(copy_assign_base&& rhs) = default;

  constexpr copy_assign_base& operator=(const copy_assign_base& rhs) noexcept(
      std::is_nothrow_copy_constructible_v<E> && std::is_nothrow_copy_assignable_v<E>) {
    if (this->m_has_val && rhs.m_has_val) {
      // no-op
    } else if (this->m_has_val) {
      std::construct_at(std::addressof(this->m_unexpect), rhs.m_unexpect);
      this->m_has_val = false;
    } else if (rhs.m_has_val) {
      this->m_unexpect.~E();
      this->m_has_val = true;
    } else {
      this->m_unexpect = rhs.m_unexpect;
    }
    return *this;
  }

  copy_assign_base& operator=(copy_assign_base&& rhs) = default;
};

template <class T, class E>
inline constexpr bool is_expected_move_assignable_v =
    is_move_assignable_or_void_v<T> && is_move_constructible_or_void_v<T> &&
    std::is_move_assignable_v<E> && std::is_move_constructible_v<E> &&
    (is_nothrow_move_constructible_or_void_v<T> ||
     std::is_nothrow_move_constructible_v<E>);

// LWG-4026
template <class T, class E>
inline constexpr bool is_expected_trivially_move_assignable_v =
    is_trivially_move_constructible_or_void_v<T> &&
    is_trivially_move_assignable_or_void_v<T> && is_trivially_destructible_or_void_v<T> &&
    std::is_trivially_move_constructible_v<E> && std::is_trivially_move_assignable_v<E> &&
    std::is_trivially_destructible_v<E>;

// This class manages conditionally having a (possibly trivial) move assignment operator
template <class T, class E, bool Enabled = is_expected_move_assignable_v<T, E>,
          bool Trivially = is_expected_trivially_move_assignable_v<T, E>>
struct move_assign_base : copy_assign_base<T, E> {
  using copy_assign_base<T, E>::copy_assign_base;

  ~move_assign_base() = default;
  move_assign_base() = default;
  move_assign_base(const move_assign_base& rhs) = default;
  move_assign_base(move_assign_base&& rhs) = default;
  move_assign_base& operator=(const move_assign_base& rhs) = default;
  move_assign_base& operator=(move_assign_base&& rhs) = default;
};

template <class T, class E>
struct move_assign_base<T, E, false, false> : copy_assign_base<T, E> {
  using copy_assign_base<T, E>::copy_assign_base;

  ~move_assign_base() = default;
  move_assign_base() = default;
  move_assign_base(const move_assign_base& rhs) = default;
  move_assign_base(move_assign_base&& rhs) = default;
  move_assign_base& operator=(const move_assign_base& rhs) = default;
  move_assign_base& operator=(move_assign_base&& rhs) = delete;
};

template <class T, class E>
struct move_assign_base<T, E, true, false> : copy_assign_base<T, E> {
  using copy_assign_base<T, E>::copy_assign_base;

  ~move_assign_base() = default;
  move_assign_base() = default;
  move_assign_base(const move_assign_base& rhs) = default;
  move_assign_base(move_assign_base&& rhs) = default;
  move_assign_base& operator=(const move_assign_base& rhs) = default;

  constexpr move_assign_base& operator=(move_assign_base&& rhs)  //
      noexcept(std::is_nothrow_move_constructible_v<T> &&
               std::is_nothrow_move_assignable_v<T> &&
               std::is_nothrow_move_constructible_v<E> &&
               std::is_nothrow_move_assignable_v<E>) {
    if (this->m_has_val && rhs.m_has_val) {
      this->m_val = std::move(rhs.m_val);
    } else if (this->m_has_val) {
      expected_detail::reinit_expected(this->m_unexpect, this->m_val,
                                       std::move(rhs.m_unexpect));
    } else if (rhs.m_has_val) {
      expected_detail::reinit_expected(this->m_val, this->m_unexpect,
                                       std::move(rhs.m_val));
    } else {
      this->m_unexpect = std::move(rhs.m_unexpect);
    }
    this->m_has_val = rhs.m_has_val;
    return *this;
  }
};

template <class E>
struct move_assign_base<void, E, true, false> : copy_assign_base<void, E> {
  using copy_assign_base<void, E>::copy_assign_base;

  ~move_assign_base() = default;
  move_assign_base() = default;
  move_assign_base(const move_assign_base& rhs) = default;
  move_assign_base(move_assign_base&& rhs) = default;
  move_assign_base& operator=(const move_assign_base& rhs) = default;

  constexpr move_assign_base& operator=(move_assign_base&& rhs)  //
      noexcept(std::is_nothrow_move_constructible_v<E> &&
               std::is_nothrow_move_assignable_v<E>) {
    if (this->m_has_val && rhs.m_has_val) {
      // no-op
    } else if (this->m_has_val) {
      std::construct_at(std::addressof(this->m_unexpect), std::move(rhs.m_unexpect));
      this->m_has_val = false;
    } else if (rhs.m_has_val) {
      this->m_unexpect.~E();
      this->m_has_val = true;
    } else {
      this->m_unexpect = std::move(rhs.m_unexpect);
    }
    return *this;
  }
};

// This is needed to be able to construct the default_ctor_base which
// follows, while still conditionally deleting the default constructor.
struct default_constructor_tag {
  explicit default_constructor_tag() = default;
};

// default_ctor_base will ensure that expected has a deleted default
// consturctor if T is not default constructible.
// This specialization is for when T is default constructible
template <class T, class E, bool Enable = is_default_constructible_or_void_v<T>>
struct default_ctor_base {
  ~default_ctor_base() = default;
  default_ctor_base() = default;
  default_ctor_base(default_ctor_base const&) = default;
  default_ctor_base(default_ctor_base&&) = default;
  default_ctor_base& operator=(default_ctor_base const&) = default;
  default_ctor_base& operator=(default_ctor_base&&) = default;

  constexpr explicit default_ctor_base(default_constructor_tag) {}
};

// This specialization is for when T is not default constructible
template <class T, class E>
struct default_ctor_base<T, E, false> {
  ~default_ctor_base() = default;
  default_ctor_base() = delete;
  default_ctor_base(default_ctor_base const&) = default;
  default_ctor_base(default_ctor_base&&) = default;
  default_ctor_base& operator=(default_ctor_base const&) = default;
  default_ctor_base& operator=(default_ctor_base&&) = default;

  constexpr explicit default_ctor_base(default_constructor_tag) {}
};

}  // namespace expected_detail

/// An `expected<T, E>` object is an object that contains the storage for
/// another object and manages the lifetime of this contained object `T`.
/// Alternatively it could contain the storage for another unexpected object
/// `E`. The contained object may not be initialized after the expected object
/// has been initialized, and may not be destroyed before the expected object
/// has been destroyed. The initialization state of the contained object is
/// tracked by the expected object.

template <class T, class E>
class [[nodiscard]] expected : private expected_detail::move_assign_base<T, E>,
                               private expected_detail::default_ctor_base<T, E> {
  static_assert(expected_detail::is_value_type_valid_v<T>);
  static_assert(expected_detail::is_error_type_valid_v<E>);

  constexpr T* valptr() noexcept { return std::addressof(this->m_val); }
  constexpr const T* valptr() const noexcept { return std::addressof(this->m_val); }
  constexpr E* errptr() noexcept { return std::addressof(this->m_unexpect); }
  constexpr const E* errptr() const noexcept { return std::addressof(this->m_unexpect); }

  constexpr T& val() noexcept { return this->m_val; }
  constexpr E& err() noexcept { return this->m_unexpect; }

  constexpr const T& val() const noexcept { return this->m_val; }
  constexpr const E& err() const noexcept { return this->m_unexpect; }

  using impl_base = expected_detail::move_assign_base<T, E>;
  using ctor_base = expected_detail::default_ctor_base<T, E>;

 public:
  using value_type = T;
  using error_type = E;
  using unexpected_type = unexpected<E>;

  // default constructors

  constexpr expected() = default;
  constexpr expected(const expected& rhs) = default;
  constexpr expected(expected&& rhs) = default;

  // constructors for expected<U, G>

  // implicit const reference
  template <class U, class G,
            std::enable_if_t<std::is_convertible_v<const U&, T> &&
                             std::is_convertible_v<const G&, E>>* = nullptr,
            expected_detail::enable_from_other_expected_t<T, E, U, G, const U&,
                                                          const G&>* = nullptr>
  constexpr expected(const expected<U, G>& rhs)  //
      noexcept(std::is_nothrow_constructible_v<T, const U&> &&
               std::is_nothrow_constructible_v<E, const G&>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct(*rhs);
    } else {
      this->construct_error(rhs.error());
    }
  }

  // explicit const reference
  template <class U, class G,
            std::enable_if_t<!(std::is_convertible_v<const U&, T> &&
                               std::is_convertible_v<const G&, E>)>* = nullptr,
            expected_detail::enable_from_other_expected_t<T, E, U, G, const U&,
                                                          const G&>* = nullptr>
  constexpr explicit expected(const expected<U, G>& rhs)  //
      noexcept(std::is_nothrow_constructible_v<T, const U&> &&
               std::is_nothrow_constructible_v<E, const G&>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct(*rhs);
    } else {
      this->construct_error(rhs.error());
    }
  }

  // implicit rvalue
  template <class U, class G,
            std::enable_if_t<std::is_convertible_v<U, T> &&
                             std::is_convertible_v<G, E>>* = nullptr,
            expected_detail::enable_from_other_expected_t<T, E, U, G, U, G>* = nullptr>
  constexpr expected(expected<U, G>&& rhs)  //
      noexcept(std::is_nothrow_constructible_v<T, U> &&
               std::is_nothrow_constructible_v<E, G>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct(std::move(*rhs));
    } else {
      this->construct_error(std::move(rhs.error()));
    }
  }

  // explicit rvalue
  template <class U,  //
            class G,  //
            std::enable_if_t<!(std::is_convertible_v<U, T> &&
                               std::is_convertible_v<G, E>)>* = nullptr,  //
            expected_detail::enable_from_other_expected_t<T, E, U, G, U, G>* = nullptr>
  constexpr explicit expected(expected<U, G>&& rhs)  //
      noexcept(std::is_nothrow_constructible_v<T, U> &&
               std::is_nothrow_constructible_v<E, G>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct(std::move(*rhs));
    } else {
      this->construct_error(std::move(rhs.error()));
    }
  }

  // template <class U = T>
  //     expected(U &&)

  // implicit
  template <class U = T, std::enable_if_t<std::is_convertible_v<U, T>>* = nullptr,
            expected_detail::enable_forward_t<T, E, U>* = nullptr>
  constexpr expected(U&& v) noexcept(std::is_nothrow_constructible_v<T, U>)
      : expected(std::in_place, std::forward<U>(v)) {}

  // explicit
  template <class U = T, std::enable_if_t<!std::is_convertible_v<U, T>>* = nullptr,
            expected_detail::enable_forward_t<T, E, U>* = nullptr>
  constexpr explicit expected(U&& v) noexcept(std::is_nothrow_constructible_v<T, U>)
      : expected(std::in_place, std::forward<U>(v)) {}

  // constructors for unexpected<G>

  // explicit const unexpected<G> &
  template <class G,                                                           //
            std::enable_if_t<!std::is_convertible_v<const G&, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, const G&>>* = nullptr>
  explicit constexpr expected(const unexpected<G>& e) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : impl_base(unexpect, e.error()),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // implicit const unexpected<G> &
  template <class G,                                                          //
            std::enable_if_t<std::is_convertible_v<const G&, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, const G&>>* = nullptr>
  constexpr expected(unexpected<G> const& e) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : impl_base(unexpect, e.error()),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // explicit unexpected<G> &&
  template <class G,                                                    //
            std::enable_if_t<!std::is_convertible_v<G, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, G>>* = nullptr>
  explicit constexpr expected(unexpected<G>&& e) noexcept(
      std::is_nothrow_constructible_v<E, G>)
      : impl_base(unexpect, std::move(e.error())),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // implicit unexpected<G> &&
  template <class G,                                                   //
            std::enable_if_t<std::is_convertible_v<G, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, G>>* = nullptr>
  constexpr expected(unexpected<G>&& e) noexcept(std::is_nothrow_constructible_v<E, G>)
      : impl_base(unexpect, std::move(e.error())),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class... Args>
  //     expected(std::in_place_t, Args &&...)

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<T, Args...>>* = nullptr>
  constexpr explicit expected(std::in_place_t, Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<T, Args...>)
      : impl_base(std::in_place, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class U, class... Args>
  //     expected(std::in_place_t, std::initializer_list<U>, Args &&...)

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<T, std::initializer_list<U>&,
                                                     Args...>>* = nullptr>
  constexpr explicit expected(std::in_place_t, std::initializer_list<U> il,
                              Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<T, std::initializer_list<U>&, Args...>)
      : impl_base(std::in_place, il, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // helper ctor for transform()
  template <class Fn, class... Args>
  constexpr explicit expected(expected_detail::construct_with_invoke_result_t tag,
                              Fn&& func, Args&&... args)  //
      noexcept(noexcept(static_cast<T>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : impl_base(tag, std::forward<Fn>(func), std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class... Args>
  //     expected(unexpect_t, Args &&...)

  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args...>>* = nullptr>
  constexpr explicit expected(unexpect_t, Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<E, Args...>)
      : impl_base(unexpect, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class U, class... Args>
  //     expected(unexpect_t, std::initializer_list<U>, Args &&...)

  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args...>>* = nullptr>
  constexpr explicit expected(
      unexpect_t, std::initializer_list<U> il,
      Args&&... args) noexcept(std::is_nothrow_constructible_v<E,
                                                               std::initializer_list<U>&,
                                                               Args...>)
      : impl_base(unexpect, il, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit expected(expected_detail::construct_with_invoke_result_t tag1,
                              unexpect_t tag2, Fn&& func, Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : impl_base(tag1, tag2, std::forward<Fn>(func), std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // default assignment operators

  constexpr expected& operator=(const expected& rhs) = default;
  constexpr expected& operator=(expected&& rhs) = default;

  // assignments

  template <
      class U = T,                                                                      //
      std::enable_if_t<                                                                 //
          !std::is_same_v<expected, std::remove_cvref_t<U>> &&                          //
          !expected_detail::is_specialization_v<std::remove_cvref_t<U>, unexpected> &&  //
          std::is_constructible_v<T, U> &&                                              //
          std::is_assignable_v<T&, U> &&                                                //
          (std::is_nothrow_constructible_v<T, U> ||                                     //
           std::is_nothrow_move_constructible_v<T> ||                                   //
           std::is_nothrow_move_constructible_v<E>)                                     //
          >* = nullptr>
  constexpr expected& operator=(U&& v)  //
      noexcept(std::is_nothrow_constructible_v<T, U> &&
               std::is_nothrow_assignable_v<T&, U>) {
    if (has_value()) {
      val() = std::forward<U>(v);
    } else {
      expected_detail::reinit_expected(val(), err(), std::forward<U>(v));
      this->m_has_val = true;
    }

    return *this;
  }

  template <class G,                                         //
            class GF = const G&,                             //
            std::enable_if_t<                                //
                std::is_constructible_v<E, GF> &&            //
                std::is_assignable_v<E&, GF> &&              //
                (std::is_nothrow_constructible_v<E, GF> ||   //
                 std::is_nothrow_move_constructible_v<T> ||  //
                 std::is_nothrow_move_constructible_v<E>)    //
                >* = nullptr>
  constexpr expected& operator=(const unexpected<G>& rhs) noexcept(
      std::is_nothrow_constructible_v<E, GF> && std::is_nothrow_assignable_v<E&, GF>) {
    if (!has_value()) {
      err() = std::forward<GF>(rhs.error());
    } else {
      expected_detail::reinit_expected(err(), val(), std::forward<GF>(rhs.error()));
      this->m_has_val = false;
    }

    return *this;
  }

  template <class G,                                         //
            class GF = G,                                    //
            std::enable_if_t<                                //
                std::is_constructible_v<E, GF> &&            //
                std::is_assignable_v<E&, GF> &&              //
                (std::is_nothrow_constructible_v<E, GF> ||   //
                 std::is_nothrow_move_constructible_v<T> ||  //
                 std::is_nothrow_move_constructible_v<E>)    //
                >* = nullptr>
  constexpr expected& operator=(unexpected<G>&& rhs) noexcept(
      std::is_nothrow_constructible_v<E, GF> && std::is_nothrow_assignable_v<E&, GF>) {
    if (!has_value()) {
      err() = std::forward<GF>(rhs.error());
    } else {
      expected_detail::reinit_expected(err(), val(), std::forward<GF>(rhs.error()));
      this->m_has_val = false;
    }

    return *this;
  }

  template <class... Args,
            typename std::enable_if_t<std::is_nothrow_constructible_v<T, Args&&...>,
                                      bool> = false>
  constexpr T& emplace(Args&&... args) {
    if (has_value()) {
      if constexpr (!std::is_trivially_destructible_v<T>) {
        val().~T();
      }
    } else {
      if constexpr (!std::is_trivially_destructible_v<E>) {
        err().~E();
      }
      this->m_has_val = true;
    }

    return *std::construct_at(valptr(), std::forward<Args>(args)...);
  }

  template <class U, class... Args,
            std::enable_if_t<std::is_nothrow_constructible_v<T, std::initializer_list<U>&,
                                                             Args&&...>>* = nullptr>
  constexpr T& emplace(std::initializer_list<U> il, Args&&... args) {
    if (has_value()) {
      if constexpr (!std::is_trivially_destructible_v<T>) {
        val().~T();
      }
    } else {
      if constexpr (!std::is_trivially_destructible_v<E>) {
        err().~E();
      }
      this->m_has_val = true;
    }

    return *std::construct_at(valptr(), il, std::forward<Args>(args)...);
  }

  template <class OT = T, class OE = E>
  constexpr std::enable_if_t<std::is_swappable_v<OT> && std::is_swappable_v<OE> &&
                             std::is_move_constructible_v<OT> &&
                             std::is_move_constructible_v<OE> &&
                             (std::is_nothrow_move_constructible_v<OT> ||
                              std::is_nothrow_move_constructible_v<OE>)>
  swap(expected& rhs) noexcept(std::is_nothrow_move_constructible_v<T> &&
                               std::is_nothrow_swappable_v<T> &&
                               std::is_nothrow_move_constructible_v<E> &&
                               std::is_nothrow_swappable_v<E>) {
    using std::swap;
    if (this->m_has_val && rhs.m_has_val) {
      swap(this->m_val, rhs.m_val);  // ADL
    } else if (this->m_has_val) {
      if constexpr (std::is_nothrow_move_constructible_v<E>) {
        E tmp(std::move(rhs.error()));
        if constexpr (!std::is_trivially_destructible_v<E>) {
          rhs.error().~E();
        }

        if constexpr (std::is_nothrow_move_constructible_v<T>) {
          std::construct_at(std::addressof(rhs.m_val), std::move(this->m_val));
        } else {
          expected_detail::ReinitGuard<E> guard{std::addressof(rhs.error()),
                                                std::addressof(tmp)};
          std::construct_at(std::addressof(rhs.m_val), std::move(this->m_val));
          guard._target = nullptr;
        }

        if constexpr (!std::is_trivially_destructible_v<T>) {
          this->m_val.~T();
        }
        std::construct_at(std::addressof(this->error()), std::move(tmp));
      } else {
        T tmp(std::move(this->m_val));
        if constexpr (!std::is_trivially_destructible_v<T>) {
          this->m_val.~T();
        }

        expected_detail::ReinitGuard<T> guard{std::addressof(this->m_val),
                                              std::addressof(tmp)};
        std::construct_at(std::addressof(this->error()), std::move(rhs.error()));
        guard._target = nullptr;

        if constexpr (!std::is_trivially_destructible_v<E>) {
          rhs.error().~E();
        }
        std::construct_at(std::addressof(rhs.m_val), std::move(tmp));
      }

      this->m_has_val = false;
      rhs.m_has_val = true;
    } else if (rhs.m_has_val) {
      rhs.swap(*this);
    } else {
      swap(this->error(), rhs.error());  // ADL
    }
  }

  constexpr const T* operator->() const noexcept { return valptr(); }
  constexpr T* operator->() noexcept { return valptr(); }

  constexpr const T& operator*() const& noexcept { return val(); }
  constexpr T& operator*() & noexcept { return val(); }
  constexpr const T&& operator*() const&& noexcept { return std::move(val()); }
  constexpr T&& operator*() && noexcept { return std::move(val()); }

  constexpr bool has_value() const noexcept { return this->m_has_val; }
  constexpr explicit operator bool() const noexcept { return this->m_has_val; }

  constexpr const T& value() const& {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3843");
    if (!has_value()) throw bad_expected_access(error());
    return val();
  }
  constexpr T& value() & {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3843");
    if (!has_value()) throw bad_expected_access(std::as_const(error()));
    return val();
  }
  constexpr const T&& value() const&& {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3843");
    static_assert(std::is_constructible_v<E, decltype(std::move(error()))>,
                  "E must be constructible from const E&&, by LWG-3843");
    if (!has_value()) throw bad_expected_access(std::move(error()));
    return std::move(val());
  }
  constexpr T&& value() && {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3843");
    static_assert(std::is_constructible_v<E, decltype(std::move(error()))>,
                  "E must be constructible from E&&, by LWG-3843");
    if (!has_value()) throw bad_expected_access(std::move(error()));
    return std::move(val());
  }

  constexpr const E& error() const& noexcept { return err(); }
  constexpr E& error() & noexcept { return err(); }
  constexpr const E&& error() const&& noexcept { return std::move(err()); }
  constexpr E&& error() && noexcept { return std::move(err()); }

  template <class U>
  constexpr T value_or(U&& v) const& noexcept(
      std::is_nothrow_copy_constructible_v<T> &&
      expected_detail::is_nothrow_convertible_v<U, T>) {
    static_assert(std::is_copy_constructible_v<T>, "T must be copy-constructible");
    static_assert(std::is_convertible_v<U, T>, "is_convertible_v<U, T> must be true");
    if (this->m_has_val) {
      return this->m_val;
    } else {
      return static_cast<T>(std::forward<U>(v));
    }
  }

  template <class U>
  constexpr T value_or(U&& v) && noexcept(
      std::is_nothrow_move_constructible_v<T> &&
      expected_detail::is_nothrow_convertible_v<U, T>) {
    static_assert(std::is_move_constructible_v<T>, "T must be move-constructible");
    static_assert(std::is_convertible_v<U, T>, "is_convertible_v<U, T> must be true");
    if (this->m_has_val) {
      return std::move(this->m_val);
    } else {
      return static_cast<T>(std::forward<U>(v));
    }
  }

  template <class G = E>
  constexpr E error_or(G&& v) const&  //
      noexcept(std::is_nothrow_copy_constructible_v<E> &&
               expected_detail::is_nothrow_convertible_v<G, E>) {
    static_assert(std::is_copy_constructible_v<E>, "E must be copy-constructible");
    static_assert(std::is_convertible_v<G, E>, "is_convertible_v<G, E> must be true");
    if (this->m_has_val) {
      return static_cast<E>(std::forward<G>(v));
    } else {
      return this->m_unexpect;
    }
  }
  template <class G = E>
  constexpr E error_or(G&& v) &&  //
      noexcept(std::is_nothrow_move_constructible_v<E> &&
               expected_detail::is_nothrow_convertible_v<G, E>) {
    static_assert(std::is_move_constructible_v<E>, "E must be move-constructible");
    static_assert(std::is_convertible_v<G, E>, "is_convertible_v<G, E> must be true");
    if (this->m_has_val) {
      return static_cast<E>(std::forward<G>(v));
    } else {
      return std::move(this->m_unexpect);
    }
  }

  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&>>* = nullptr>
  constexpr auto and_then(F&& f) & {
    using U = std::remove_cvref_t<std::invoke_result_t<F, decltype((this->m_val))>>;
    static_assert(expected_detail::is_specialization_v<U, expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f), this->m_val);

    } else {
      return U(unexpect, error());
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE&>>* = nullptr>
  constexpr auto and_then(F&& f) const& {
    using U = std::remove_cvref_t<std::invoke_result_t<F, decltype((this->m_val))>>;
    static_assert(expected_detail::is_specialization_v<U, expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f), this->m_val);
    } else {
      return U(unexpect, error());
    }
  }

  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&&>>* = nullptr>
  constexpr auto and_then(F&& f) && {
    using U =
        std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(this->m_val))>>;
    static_assert(expected_detail::is_specialization_v<U, expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f), std::move(this->m_val));
    } else {
      return U(unexpect, std::move(error()));
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE>>* = nullptr>
  constexpr auto and_then(F&& f) const&& {
    using U =
        std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(this->m_val))>>;
    static_assert(expected_detail::is_specialization_v<U, expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f), std::move(this->m_val));
    } else {
      return U(unexpect, std::move(error()));
    }
  }

  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, UT&>>* = nullptr>
  constexpr auto or_else(F&& f) & {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_specialization_v<G, expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G(std::in_place, this->m_val);
    } else {
      return std::invoke(std::forward<F>(f), error());
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, const UT&>>* = nullptr>
  constexpr auto or_else(F&& f) const& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_specialization_v<G, expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G(std::in_place, this->m_val);
    } else {
      return std::invoke(std::forward<F>(f), error());
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, UT&&>>* = nullptr>
  constexpr auto or_else(F&& f) && {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_specialization_v<G, expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G(std::in_place, std::move(this->m_val));
    } else {
      return std::invoke(std::forward<F>(f), std::move(error()));
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, const UT>>* = nullptr>
  constexpr auto or_else(F&& f) const&& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_specialization_v<G, expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G(std::in_place, std::move(this->m_val));
    } else {
      return std::invoke(std::forward<F>(f), std::move(error()));
    }
  }

  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&>>* = nullptr>
  constexpr auto transform(F&& f) & {
    using U = std::remove_cvref_t<std::invoke_result_t<F, decltype((this->m_val))>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, error());
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f), this->m_val);
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f), this->m_val);
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE&>>* = nullptr>
  constexpr auto transform(F&& f) const& {
    using U = std::remove_cvref_t<std::invoke_result_t<F, decltype((this->m_val))>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, error());
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f), this->m_val);
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f), this->m_val);
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&&>>* = nullptr>
  constexpr auto transform(F&& f) && {
    using U =
        std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(this->m_val))>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, std::move(error()));
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f), std::move(this->m_val));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f), std::move(this->m_val));
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE>>* = nullptr>
  constexpr auto transform(F&& f) const&& {
    using U =
        std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(this->m_val))>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, std::move(error()));
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f), std::move(this->m_val));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f), std::move(this->m_val));
      }
    }
  }

  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, UT&>>* = nullptr>
  constexpr auto transform_error(F&& f) & {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<T, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>(std::in_place, this->m_val);
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), error());
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, const UT&>>* = nullptr>
  constexpr auto transform_error(F&& f) const& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<T, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>(std::in_place, this->m_val);
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), error());
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, UT&&>>* = nullptr>
  constexpr auto transform_error(F&& f) && {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<T, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>(std::in_place, std::move(this->m_val));
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), std::move(error()));
    }
  }
  template <class F, class UT = T,
            std::enable_if_t<std::is_constructible_v<UT, const UT>>* = nullptr>
  constexpr auto transform_error(F&& f) const&& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<T, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>(std::in_place, std::move(this->m_val));
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), std::move(error()));
    }
  }

  template <class T2, class E2>
  [[nodiscard]] friend constexpr std::enable_if_t<!std::is_void_v<T2>, bool> operator==(
      const expected& x,
      const expected<T2, E2>& y) noexcept(noexcept(*x == *y) &&
                                          noexcept(x.error() == y.error())) {
    if (x.has_value() != y.has_value()) {
      return false;
    } else if (x.has_value()) {
      return *x == *y;
    } else {
      return x.error() == y.error();
    }
  }

  template <class T2>
  [[nodiscard]] friend constexpr bool operator==(const expected& x, const T2& v)  //
      noexcept(noexcept(*x == v)) {
    if (x.has_value()) {
      return static_cast<bool>(*x == v);
    } else {
      return false;
    }
  }

  template <class E2>
  [[nodiscard]] friend constexpr bool operator==(
      const expected& x,
      const unexpected<E2>& e) noexcept(noexcept(x.error() == e.error())) {
    if (x.has_value()) {
      return false;
    } else {
      return static_cast<bool>(x.error() == e.error());
    }
  }
};

// standalone swap for non-void value type
template <
    class T, class E,
    std::enable_if_t<!std::is_void_v<T> &&                                         //
                     std::is_move_constructible_v<T> && std::is_swappable_v<T> &&  //
                     std::is_move_constructible_v<E> && std::is_swappable_v<E> &&  //
                     (std::is_nothrow_move_constructible_v<T> ||
                      std::is_nothrow_move_constructible_v<E>)>* = nullptr>
constexpr void swap(expected<T, E>& lhs,
                    expected<T, E>& rhs) noexcept(noexcept(lhs.swap(rhs))) {
  lhs.swap(rhs);
}

template <class E>
class [[nodiscard]] expected<void, E>
    : private expected_detail::move_assign_base<void, E>,
      private expected_detail::default_ctor_base<void, E> {
  static_assert(expected_detail::is_error_type_valid_v<E>);

  using T = void;

  constexpr E* errptr() noexcept { return std::addressof(this->m_unexpect); }
  constexpr const E* errptr() const noexcept { return std::addressof(this->m_unexpect); }

  constexpr void val() noexcept {}
  constexpr E& err() noexcept { return this->m_unexpect; }
  constexpr void val() const noexcept {}
  constexpr const E& err() const noexcept { return this->m_unexpect; }

  using impl_base = expected_detail::move_assign_base<T, E>;
  using ctor_base = expected_detail::default_ctor_base<T, E>;

 public:
  using value_type = T;
  using error_type = E;
  using unexpected_type = unexpected<E>;

  constexpr expected() = default;
  constexpr expected(const expected& rhs) = default;
  constexpr expected(expected&& rhs) = default;

  template <
      class U,                                                          //
      class G,                                                          //
      std::enable_if_t<std::is_convertible_v<const G&, E>>* = nullptr,  //
      expected_detail::enable_from_other_void_expected_t<E, U, G, const G&>* = nullptr>
  constexpr expected(const expected<U, G>& rhs) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct();
    } else {
      this->construct_error(rhs.error());
    }
  }

  template <
      class U,                                                           //
      class G,                                                           //
      std::enable_if_t<!std::is_convertible_v<const G&, E>>* = nullptr,  //
      expected_detail::enable_from_other_void_expected_t<E, U, G, const G&>* = nullptr>
  constexpr explicit expected(const expected<U, G>& rhs) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct();
    } else {
      this->construct_error(rhs.error());
    }
  }

  template <class U,                                                   //
            class G,                                                   //
            std::enable_if_t<std::is_convertible_v<G, E>>* = nullptr,  //
            expected_detail::enable_from_other_void_expected_t<E, U, G, G>* = nullptr>
  constexpr expected(expected<U, G>&& rhs) noexcept(std::is_nothrow_constructible_v<E, G>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct();
    } else {
      this->construct_error(std::move(rhs.error()));
    }
  }

  template <class U,                                                    //
            class G,                                                    //
            std::enable_if_t<!std::is_convertible_v<G, E>>* = nullptr,  //
            expected_detail::enable_from_other_void_expected_t<E, U, G, G>* = nullptr>
  constexpr explicit expected(expected<U, G>&& rhs) noexcept(
      std::is_nothrow_constructible_v<E, G>)
      : ctor_base(expected_detail::default_constructor_tag{}) {
    if (rhs.has_value()) {
      this->construct();
    } else {
      this->construct_error(std::move(rhs.error()));
    }
  }

  // constructors for unexpected<G>

  // explicit const unexpected<G> &
  template <class G,                                                           //
            std::enable_if_t<!std::is_convertible_v<const G&, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, const G&>>* = nullptr>
  explicit constexpr expected(const unexpected<G>& e) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : impl_base(unexpect, e.error()),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // implicit const unexpected<G> &
  template <class G,                                                          //
            std::enable_if_t<std::is_convertible_v<const G&, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, const G&>>* = nullptr>
  constexpr expected(unexpected<G> const& e) noexcept(
      std::is_nothrow_constructible_v<E, const G&>)
      : impl_base(unexpect, e.error()),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // explicit unexpected<G> &&
  template <class G,                                                    //
            std::enable_if_t<!std::is_convertible_v<G, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, G>>* = nullptr>
  explicit constexpr expected(unexpected<G>&& e) noexcept(
      std::is_nothrow_constructible_v<E, G>)
      : impl_base(unexpect, std::move(e.error())),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // implicit unexpected<G> &&
  template <class G,                                                   //
            std::enable_if_t<std::is_convertible_v<G, E>>* = nullptr,  //
            std::enable_if_t<std::is_constructible_v<E, G>>* = nullptr>
  constexpr expected(unexpected<G>&& e) noexcept(std::is_nothrow_constructible_v<E, G>)
      : impl_base(unexpect, std::move(e.error())),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // expected(std::in_place_t)
  constexpr explicit expected(std::in_place_t) noexcept
      : impl_base(std::in_place), ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class... Args>
  //     expected(unexpect_t, Args &&...)
  template <class... Args,
            std::enable_if_t<std::is_constructible_v<E, Args...>>* = nullptr>
  constexpr explicit expected(unexpect_t, Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<E, Args...>)
      : impl_base(unexpect, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // template<class U, class... Args>
  //     expected(unexpect_t, std::initializer_list<U>, Args &&...)
  template <class U, class... Args,
            std::enable_if_t<std::is_constructible_v<E, std::initializer_list<U>&,
                                                     Args...>>* = nullptr>
  constexpr explicit expected(unexpect_t, std::initializer_list<U> il, Args&&... args)  //
      noexcept(std::is_nothrow_constructible_v<E, std::initializer_list<U>&, Args...>)
      : impl_base(unexpect, il, std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  // helper ctor for transform_error()
  template <class Fn, class... Args>
  constexpr explicit expected(expected_detail::construct_with_invoke_result_t tag1,
                              unexpect_t tag2, Fn&& func, Args&&... args)  //
      noexcept(noexcept(static_cast<E>(std::invoke(std::forward<Fn>(func),
                                                   std::forward<Args>(args)...))))
      : impl_base(tag1, tag2, std::forward<Fn>(func), std::forward<Args>(args)...),
        ctor_base(expected_detail::default_constructor_tag{}) {}

  expected& operator=(const expected& rhs) = default;
  expected& operator=(expected&& rhs) = default;

  template <class G,                               //
            class GF = const G&,                   //
            std::enable_if_t<                      //
                std::is_constructible_v<E, GF> &&  //
                std::is_assignable_v<E&, GF>       //
                >* = nullptr>
  constexpr expected& operator=(const unexpected<G>& rhs)  //
      noexcept(std::is_nothrow_constructible_v<E, GF> &&
               std::is_nothrow_assignable_v<E&, GF>) {
    if (has_value()) {
      std::construct_at(errptr(), std::forward<GF>(rhs.error()));
      this->m_has_val = false;
    } else {
      error() = std::forward<GF>(rhs.error());
    }

    return *this;
  }

  template <class G,                               //
            class GF = G,                          //
            std::enable_if_t<                      //
                std::is_constructible_v<E, GF> &&  //
                std::is_assignable_v<E&, GF>       //
                >* = nullptr>
  constexpr expected& operator=(unexpected<G>&& rhs) noexcept(
      std::is_nothrow_constructible_v<E, GF> && std::is_nothrow_assignable_v<E&, GF>) {
    if (has_value()) {
      std::construct_at(errptr(), std::forward<GF>(rhs.error()));
      this->m_has_val = false;
    } else {
      error() = std::forward<GF>(rhs.error());
    }

    return *this;
  }

  constexpr void emplace() {
    if (!has_value()) {
      if constexpr (!std::is_trivially_destructible_v<E>) {
        err().~E();
      }
      this->m_has_val = true;
    }
  }

  template <class OE = E>
  constexpr std::enable_if_t<std::is_swappable_v<OE> && std::is_move_constructible_v<OE>>
  swap(expected& rhs) noexcept(std::is_nothrow_move_constructible_v<E> &&
                               std::is_nothrow_swappable_v<E>) {
    using std::swap;
    if (this->m_has_val && rhs.m_has_val) {
      // do nothing
    } else if (this->m_has_val) {
      std::construct_at(std::addressof(error()), std::move(rhs.error()));
      if constexpr (!std::is_trivially_destructible_v<E>) {
        rhs.error().~E();
      }
      this->m_has_val = false;
      rhs.m_has_val = true;
    } else if (rhs.m_has_val) {
      std::construct_at(std::addressof(rhs.error()), std::move(error()));
      if constexpr (!std::is_trivially_destructible_v<E>) {
        error().~E();
      }
      this->m_has_val = true;
      rhs.m_has_val = false;
    } else {
      swap(error(), rhs.error());  // ADL
    }
  }

  constexpr void operator*() const noexcept { return val(); }

  constexpr bool has_value() const noexcept { return this->m_has_val; }
  constexpr explicit operator bool() const noexcept { return this->m_has_val; }

  constexpr void value() const& {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3940");
    if (!has_value()) throw bad_expected_access(err());
  }

  constexpr void value() && {
    static_assert(std::is_copy_constructible_v<E>,
                  "E must be copy constructible, by LWG-3940");
    static_assert(std::is_move_constructible_v<E>,
                  "E must be move constructible, by LWG-3940");
    if (!has_value()) throw bad_expected_access(std::move(err()));
  }

  constexpr const E& error() const& noexcept { return err(); }
  constexpr E& error() & noexcept { return err(); }
  constexpr const E&& error() const&& noexcept { return std::move(err()); }
  constexpr E&& error() && noexcept { return std::move(err()); }

  template <class G = E>
  constexpr E error_or(G&& v) const& noexcept(
      std::is_nothrow_copy_constructible_v<E> &&
      expected_detail::is_nothrow_convertible_v<G, E>) {
    static_assert(std::is_copy_constructible_v<E>, "E must be copy-constructible");
    static_assert(std::is_convertible_v<G, E>, "is_convertible_v<G, E> must be true");
    if (this->m_has_val) {
      return static_cast<E>(std::forward<G>(v));
    } else {
      return this->m_unexpect;
    }
  }
  template <class G = E>
  constexpr E error_or(G&& v) && noexcept(
      std::is_nothrow_move_constructible_v<E> &&
      expected_detail::is_nothrow_convertible_v<G, E>) {
    static_assert(std::is_move_constructible_v<E>, "E must be move-constructible");
    static_assert(std::is_convertible_v<G, E>, "is_convertible_v<G, E> must be true");
    if (this->m_has_val) {
      return static_cast<E>(std::forward<G>(v));
    } else {
      return std::move(this->m_unexpect);
    }
  }

  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&>>* = nullptr>
  constexpr auto and_then(F&& f) & {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_specialization_v<U, iceberg::expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f));
    } else {
      return U(unexpect, error());
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE&>>* = nullptr>
  constexpr auto and_then(F&& f) const& {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_specialization_v<U, iceberg::expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f));
    } else {
      return U(unexpect, error());
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&&>>* = nullptr>
  constexpr auto and_then(F&& f) && {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_specialization_v<U, iceberg::expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f));
    } else {
      return U(unexpect, std::move(error()));
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE>>* = nullptr>
  constexpr auto and_then(F&& f) const&& {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_specialization_v<U, iceberg::expected>,
                  "U (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename U::error_type, E>,
                  "The error type must be the same after calling the F");

    if (has_value()) {
      return std::invoke(std::forward<F>(f));
    } else {
      return U(unexpect, std::move(error()));
    }
  }

  template <class F>
  constexpr auto or_else(F&& f) & {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_specialization_v<G, iceberg::expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G();
    } else {
      return std::invoke(std::forward<F>(f), error());
    }
  }
  template <class F>
  constexpr auto or_else(F&& f) const& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_specialization_v<G, iceberg::expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G();
    } else {
      return std::invoke(std::forward<F>(f), error());
    }
  }
  template <class F>
  constexpr auto or_else(F&& f) && {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_specialization_v<G, iceberg::expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G();
    } else {
      return std::invoke(std::forward<F>(f), std::move(error()));
    }
  }
  template <class F>
  constexpr auto or_else(F&& f) const&& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_specialization_v<G, iceberg::expected>,
                  "G (return type of F) must be specialization of expected");
    static_assert(std::is_same_v<typename G::value_type, T>,
                  "The value type must be the same after calling the F");

    if (has_value()) {
      return G();
    } else {
      return std::invoke(std::forward<F>(f), std::move(error()));
    }
  }

  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&>>* = nullptr>
  constexpr auto transform(F&& f) & {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, error());
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f));
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE&>>* = nullptr>
  constexpr auto transform(F&& f) const& {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, error());
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f));
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, GE&&>>* = nullptr>
  constexpr auto transform(F&& f) && {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, std::move(error()));
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f));
      }
    }
  }
  template <class F, class GE = E,
            std::enable_if_t<std::is_constructible_v<GE, const GE>>* = nullptr>
  constexpr auto transform(F&& f) const&& {
    using U = std::remove_cvref_t<std::invoke_result_t<F>>;
    static_assert(expected_detail::is_value_type_valid_v<U>,
                  "U must be a valid type for expected<U, E>");
    // FIXME another constraint needed here
    if (!has_value()) {
      return expected<U, E>(unexpect, std::move(error()));
    } else {
      if constexpr (std::is_void_v<U>) {
        std::invoke(std::forward<F>(f));
        return expected<U, E>{};
      } else {
        return expected<U, E>(expected_detail::construct_with_invoke_result_t{},
                              std::forward<F>(f));
      }
    }
  }

  template <class F>
  constexpr auto transform_error(F&& f) & {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<void, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>();
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), error());
    }
  }
  template <class F>
  constexpr auto transform_error(F&& f) const& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(error())>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<void, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>();
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), error());
    }
  }
  template <class F>
  constexpr auto transform_error(F&& f) && {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<void, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>();
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), std::move(error()));
    }
  }
  template <class F>
  constexpr auto transform_error(F&& f) const&& {
    using G = std::remove_cvref_t<std::invoke_result_t<F, decltype(std::move(error()))>>;
    static_assert(expected_detail::is_error_type_valid_v<G>,
                  "G must be a valid type for expected<void, G>");
    // FIXME another constraint needed here
    if (has_value()) {
      return expected<T, G>();
    } else {
      return expected<T, G>(expected_detail::construct_with_invoke_result_t{}, unexpect,
                            std::forward<F>(f), std::move(error()));
    }
  }

  template <class T2, class E2>
  [[nodiscard]] friend constexpr std::enable_if_t<std::is_void_v<T2>, bool> operator==(
      const expected& x,
      const expected<T2, E2>& y) noexcept(noexcept(x.error() == y.error())) {
    if (x.has_value() != y.has_value()) {
      return false;
    } else {
      return x.has_value() || static_cast<bool>(x.error() == y.error());
    }
  }

  template <class E2>
  [[nodiscard]] friend constexpr bool operator==(
      const expected& x,
      const unexpected<E2>& e) noexcept(noexcept(x.error() == e.error())) {
    if (x.has_value()) {
      return false;
    } else {
      return static_cast<bool>(x.error() == e.error());
    }
  }
};

// standalone swap for void value type
template <class E, std::enable_if_t<std::is_move_constructible_v<E> &&
                                    std::is_swappable_v<E>>* = nullptr>
constexpr void swap(expected<void, E>& lhs,
                    expected<void, E>& rhs) noexcept(noexcept(lhs.swap(rhs))) {
  lhs.swap(rhs);
}

}  // namespace iceberg

// NOLINTEND
