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

#include "iceberg/expected.h"

#include <gtest/gtest.h>

TEST(ExpectedTest, DefaultCons) {
  iceberg::expected<int, int> e1;
  iceberg::expected<int, int> e2{};
  EXPECT_EQ(e1.value(), 0);
  EXPECT_EQ(e2.value(), 0);
}

TEST(ExpectedTest, InplaceCons) {
  iceberg::expected<void, int> e1{iceberg::unexpect};
  iceberg::expected<void, int> e2{iceberg::unexpect, 42};
  iceberg::expected<int, int> e3{iceberg::unexpect};
  iceberg::expected<int, int> e4{iceberg::unexpect, 42};

  EXPECT_EQ(e1.error(), 0);
  EXPECT_EQ(e2.error(), 42);
  EXPECT_EQ(e3.error(), 0);
  EXPECT_EQ(e4.error(), 42);
}

TEST(ExpectedTest, ImplicitConversion) {
  iceberg::expected<int, int> e = 42;

  EXPECT_EQ(e.value(), 42);
}

TEST(ExpectedTest, ExplicitConversion) {
  iceberg::expected<std::string, int> e1 = iceberg::unexpected<int>(42);
  iceberg::expected<std::string, int> e2{iceberg::unexpect, 42};
  EXPECT_EQ(e1.error(), 42);
  EXPECT_EQ(e2.error(), 42);
}

TEST(ExpectedTest, ImplicitConversionFrom) {
  EXPECT_FALSE((std::is_convertible_v<iceberg::expected<std::string, int>, int>));
}

TEST(ExpectedTest, ExplictVoidE) {
  iceberg::expected<void, int> e1 = {};
  iceberg::expected<void, int> e2 = iceberg::unexpected{42};

  EXPECT_TRUE(e1.has_value());
  EXPECT_TRUE((std::is_same_v<decltype(e1.value()), void>));
  EXPECT_EQ(e1.error(), 0);
  EXPECT_EQ(e2.error(), 42);
}

TEST(ExpectedTest, Emplace) {
  struct S {
    S(int i, double d) noexcept : i(i), d(d) {}
    int i;
    double d;
  };
  iceberg::expected<S, std::nullopt_t> e{iceberg::unexpect, std::nullopt};
  e.emplace(42, 3.14);
  EXPECT_EQ(e.value().i, 42);
  EXPECT_EQ(e.value().d, 3.14);
}

TEST(ExpectedTest, Equality) {
  iceberg::expected<int, int> const v1;
  iceberg::expected<int, int> const v2{42};
  iceberg::expected<int, int> const v3 = 42;
  iceberg::expected<int, int> const e1{iceberg::unexpect, 0};
  iceberg::expected<int, int> const e2{iceberg::unexpect, 42};
  iceberg::expected<int, int> const e3 = iceberg::unexpected(42);
  EXPECT_TRUE(v1 != v2);
  EXPECT_TRUE(v2 == v3);
  EXPECT_TRUE(v1 != e1);
  EXPECT_TRUE(v1 != e2);
  EXPECT_TRUE(e1 != e2);
  EXPECT_TRUE(e2 == e3);
  EXPECT_TRUE(e1 != v1);
  EXPECT_TRUE(e1 != v2);
}

TEST(ExpectedTest, UnexpectedError) {
  auto e = iceberg::unexpected(42);
  EXPECT_EQ(e.error(), 42);
}

TEST(ExpectedTest, BadExpectedAccess) {
  iceberg::expected<void, int> e = iceberg::unexpected(42);
  EXPECT_THROW(
      {
        try {
          e.value();
        } catch (const iceberg::bad_expected_access<int>& ex) {
          // and this tests that it has the correct message
          EXPECT_EQ(ex.error(), 42);
          throw;
        }
      },
      iceberg::bad_expected_access<int>);
}

TEST(ExpectedTest, ExpectedVoidEEmplace) {
  iceberg::expected<void, int> e1{std::in_place};
  iceberg::expected<void, int> e2;
  e2.emplace();
}

TEST(ExpectedTest, Swap) {
  iceberg::expected<int, int> e1{42};
  iceberg::expected<int, int> e2{1'337};

  e1.swap(e2);
  EXPECT_EQ(e1.value(), 1'337);
  EXPECT_EQ(e2.value(), 42);

  swap(e1, e2);
  EXPECT_EQ(e1.value(), 42);
  EXPECT_EQ(e2.value(), 1'337);
}

TEST(ExpectedTest, CopyInitialization) {
  {
    iceberg::expected<std::string, int> e = {};
    EXPECT_TRUE(e);
  }
}

TEST(ExpectedTest, BraceContructedValue) {
  {  // non-void
    iceberg::expected<int, int> e{{}};
    EXPECT_TRUE(e);
  }
#if !defined(__GNUC__)
  {  // std::string
    iceberg::expected<std::string, int> e{{}};
    EXPECT_TRUE(e);
  }
#endif
}

TEST(ExpectedTest, LWG3836) {
  struct BaseError {};
  struct DerivedError : BaseError {};

  iceberg::expected<bool, DerivedError> e1(false);
  iceberg::expected<bool, BaseError> e2(e1);
  EXPECT_TRUE(!e2.value());

  iceberg::expected<void, DerivedError> e3{};
  iceberg::expected<void, BaseError> e4(e3);
  EXPECT_TRUE(e4.has_value());
}

TEST(ExpectedTest, Assignment) {
  // non-void
  {
    iceberg::expected<int, int> e1{iceberg::unexpect, 1'337};
    iceberg::expected<int, int> e2{42};

    e1 = e2;
    EXPECT_TRUE(e1);
  }

  // void
  {
    iceberg::expected<void, int> e1{iceberg::unexpect, 1'337};
    iceberg::expected<void, int> e2{};

    e1 = e2;
    EXPECT_TRUE(e1);
  }
}

TEST(ExpectedTest, Move) {
  {
    struct user_provided_move {
      user_provided_move() = default;
      user_provided_move(user_provided_move&&) noexcept {}
    };

    struct defaulted_move {
      defaulted_move() = default;
      defaulted_move(defaulted_move&&) = default;
    };

    {
      using Expected = iceberg::expected<user_provided_move, int>;
      Expected t1;
      Expected tm1(std::move(t1));
      EXPECT_TRUE(tm1);
    }
    {
      using Expected = iceberg::expected<defaulted_move, int>;
      Expected t2;
      Expected tm2(std::move(t2));  // should compile
      EXPECT_TRUE(tm2);
    }
  }

  {
    class MoveOnly {
     public:
      MoveOnly() = default;

      // Non-copyable
      MoveOnly(const MoveOnly&) = delete;
      MoveOnly& operator=(const MoveOnly&) = delete;

      // Movable trivially
      MoveOnly(MoveOnly&&) = default;
      MoveOnly& operator=(MoveOnly&&) = default;
    };

    {
      using Expected = iceberg::expected<MoveOnly, int>;
      Expected a{};
      Expected b = std::move(a);  // should compile
    }
  }
}

namespace {

template <bool ShouldEqual, typename T, typename U>
constexpr bool EqualityTester(T const& lhs, U const& rhs) {
  return (lhs == rhs) == ShouldEqual && (lhs != rhs) != ShouldEqual &&
         (rhs == lhs) == ShouldEqual && (rhs != lhs) != ShouldEqual;
}

struct Type1 {
  std::string value;

  explicit Type1(std::string v) : value(std::move(v)) {}
  explicit Type1(const char* v) : value(v) {}
  Type1(Type1 const&) = delete;
  Type1& operator=(Type1 const&) = delete;
  Type1(Type1&& other) = default;
  Type1& operator=(Type1&&) = default;
  ~Type1() = default;

  bool operator==(Type1 const& rhs) const { return value == rhs.value; }
};

struct Type2 {
  std::string value;

  explicit Type2(std::string v) : value(std::move(v)) {}
  explicit Type2(const char* v) : value(v) {}
  Type2(Type2 const&) = delete;
  Type2& operator=(Type2 const&) = delete;
  Type2(Type2&& other) = default;
  Type2& operator=(Type2&&) = default;
  ~Type2() = default;

  bool operator==(Type2 const& rhs) const { return value == rhs.value; }
};

inline bool operator==(Type1 const& lhs, Type2 const& rhs) {
  return lhs.value == rhs.value;
}

inline bool operator==(Type2 const& lhs, Type1 const& rhs) { return rhs == lhs; }

}  // namespace

TEST(ExpectedTest, EqualityTest) {
  // expected<T, E> (T is not void)
  {
    using T = Type1;
    using E = Type2;
    using Expected = iceberg::expected<T, E>;

    // compare with same type expected<T, E>
    {
      Expected const value1("value1");
      Expected const value2("value2");
      Expected const value1Copy("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      Expected const error2 = iceberg::unexpected<E>("error2");
      Expected const error1Copy = iceberg::unexpected<E>("error1");

      EXPECT_TRUE(EqualityTester<true>(value1, value1Copy));
      EXPECT_TRUE(EqualityTester<false>(value1, value2));
      EXPECT_TRUE(EqualityTester<true>(error1, error1Copy));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error1));
    }

    // compare with different type expected<T2, E2>
    {
      using T2 = Type2;
      using E2 = Type1;
      ASSERT_FALSE((std::is_same_v<T, T2>));
      ASSERT_FALSE((std::is_same_v<E, E2>));
      using Expected2 = iceberg::expected<T2, E2>;

      Expected const value1("value1");
      Expected2 const value2("value2");
      Expected2 const value1Same("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      Expected2 const error2 = iceberg::unexpected<E2>("error2");
      Expected2 const error1Same = iceberg::unexpected<E2>("error1");

      EXPECT_TRUE(EqualityTester<true>(value1, value1Same));
      EXPECT_TRUE(EqualityTester<false>(value1, value2));
      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error1));
    }

    // compare with same value type T
    {
      Expected const value1("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      T const value2("value2");
      T const value1Same("value1");

      EXPECT_TRUE(EqualityTester<true>(value1, value1Same));
      EXPECT_TRUE(EqualityTester<false>(value1, value2));
      EXPECT_TRUE(EqualityTester<false>(error1, value2));
    }

    // compare with different value type T2
    {
      using T2 = Type2;
      ASSERT_FALSE((std::is_same_v<T, T2>));

      Expected const value1("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      T2 const value2("value2");
      T2 const value1Same("value1");

      EXPECT_TRUE(EqualityTester<true>(value1, value1Same));
      EXPECT_TRUE(EqualityTester<false>(value1, value2));
      EXPECT_TRUE(EqualityTester<false>(error1, value2));
    }

    // compare with same error type unexpected<E>
    {
      Expected const value1("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      auto const error2 = iceberg::unexpected<E>("error2");
      auto const error1Same = iceberg::unexpected<E>("error1");

      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error2));
    }

    // compare with different error type unexpected<E2>
    {
      using E2 = Type1;
      ASSERT_FALSE((std::is_same_v<E, E2>));

      Expected const value1("value1");
      Expected const error1 = iceberg::unexpected<E>("error1");
      auto const error2 = iceberg::unexpected<E2>("error2");
      auto const error1Same = iceberg::unexpected<E2>("error1");

      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error2));
    }
  }

  // expected<void, E>
  {
    using E = Type1;
    using Expected = iceberg::expected<void, E>;

    // compare with same type expected<void, E>
    {
      Expected const value1;
      Expected const value2;
      Expected const error1 = iceberg::unexpected<E>("error1");
      Expected const error2 = iceberg::unexpected<E>("error2");
      Expected const error1Copy = iceberg::unexpected<E>("error1");

      EXPECT_TRUE(EqualityTester<true>(value1, value2));
      EXPECT_TRUE(EqualityTester<true>(error1, error1Copy));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error1));
    }

    // compare with different type expected<void, E2>
    {
      using E2 = Type2;
      ASSERT_FALSE((std::is_same_v<E, E2>));
      using Expected2 = iceberg::expected<void, E2>;

      Expected const value1;
      Expected2 const value2;
      Expected const error1 = iceberg::unexpected<E>("error1");
      Expected2 const error2 = iceberg::unexpected<E2>("error2");
      Expected2 const error1Same = iceberg::unexpected<E2>("error1");

      EXPECT_TRUE(EqualityTester<true>(value1, value2));
      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error1));
    }

    // compare with same error type unexpected<E>
    {
      Expected const value1;
      Expected const error1 = iceberg::unexpected<E>("error1");
      auto const error2 = iceberg::unexpected<E>("error2");
      auto const error1Same = iceberg::unexpected<E>("error1");

      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error2));
    }

    // compare with different error type unexpected<E2>
    {
      using E2 = Type2;
      ASSERT_FALSE((std::is_same_v<E, E2>));

      Expected const value1;
      Expected const error1 = iceberg::unexpected<E>("error1");
      auto const error2 = iceberg::unexpected<E2>("error2");
      auto const error1Same = iceberg::unexpected<E2>("error1");

      EXPECT_TRUE(EqualityTester<true>(error1, error1Same));
      EXPECT_TRUE(EqualityTester<false>(error1, error2));
      EXPECT_TRUE(EqualityTester<false>(value1, error2));
    }
  }
}

TEST(ExpectedTest, AndThen) {
  using T = std::string;
  using Expected = iceberg::expected<int, T>;

  {  // non-void
    using Expected2 = iceberg::expected<double, T>;

    Expected e = 42;

    auto const newVal = e.and_then([](int x) { return Expected2{x * 2}; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
  {  // void
    using Expected2 = iceberg::expected<void, T>;

    Expected e = 42;

    auto const newVal = e.and_then([](int) -> Expected2 { return Expected2{}; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
}

TEST(ExpectedTest, VoidAndThen) {
  using T = std::string;
  using Expected = iceberg::expected<void, T>;

  {  // non-void
    using Expected2 = iceberg::expected<double, T>;

    Expected e{};

    auto const newVal = e.and_then([]() { return Expected2{}; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
  {  // void
    using Expected2 = iceberg::expected<void, T>;

    Expected e{};

    auto const newVal = e.and_then([]() -> Expected2 { return Expected2{}; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
}

TEST(ExpectedTest, OrElse) {
  using T = std::string;
  using Expected = iceberg::expected<T, int>;

  {  // non-void
    using Expected2 = iceberg::expected<T, double>;

    Expected e{};

    auto const newVal = e.or_else([](auto&&) { return Expected2{}; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
}

TEST(ExpectedTest, VoidOrElse) {
  using Expected = iceberg::expected<void, int>;

  {  // void
    using Expected2 = iceberg::expected<void, double>;

    Expected e{};

    auto const newVal = e.or_else([](auto&&) { return Expected2{}; });

    EXPECT_TRUE(newVal.has_value());
    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected2>));
  }
}

TEST(ExpectedTest, Transform) {
  using Expected = iceberg::expected<int, int>;

  {
    Expected e{42};

    auto const newVal = e.transform([](int n) { return n + 100; });

    EXPECT_TRUE((std::is_same_v<std::remove_cv_t<decltype(newVal)>, Expected>));
    EXPECT_EQ(newVal.value(), 142);
  }

  {
    Expected e = iceberg::unexpected{-1};
    auto const newVal = e.transform_error([](int n) { return n + 100; });

    EXPECT_EQ(newVal.error(), 99);
  }
}

namespace {
struct FromType {};

enum class NoThrowConvertible {
  Yes,
  No,
};

template <NoThrowConvertible N>
struct ToType;

template <>
struct ToType<NoThrowConvertible::Yes> {
  ToType() = default;
  ToType(ToType const&) = default;
  ToType(ToType&&) = default;

  explicit ToType(FromType const&) noexcept {}
};

template <>
struct ToType<NoThrowConvertible::No> {
  ToType() = default;
  ToType(ToType const&) = default;
  ToType(ToType&&) = default;

  explicit ToType(FromType const&) noexcept(false) {}
};

}  // namespace

TEST(ExpectedTest, ValueOrNoexcept) {
  {  // noexcept(true)
    using T = ToType<NoThrowConvertible::Yes>;
    using E = int;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_TRUE(noexcept(e.value_or(FromType{})));
  }
  {  // noexcept(false)
    using T = ToType<NoThrowConvertible::No>;
    using E = int;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_FALSE(noexcept(e.value_or(FromType{})));
  }
}

TEST(ExpectedTest, ErrorOrNoexcept) {
  {  // noexcept(true)
    using T = int;
    using E = ToType<NoThrowConvertible::Yes>;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_TRUE(noexcept(e.error_or(FromType{})));
  }
  {  // noexcept(false)
    using T = int;
    using E = ToType<NoThrowConvertible::No>;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_FALSE(noexcept(e.error_or(FromType{})));
  }
}

TEST(ExpectedTest, VoidTErrorOrNoxcept) {
  {  // noexcept(true)
    using T = void;
    using E = ToType<NoThrowConvertible::Yes>;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_TRUE(noexcept(e.error_or(FromType{})));
  }
  {  // noexcept(false)
    using T = void;
    using E = ToType<NoThrowConvertible::No>;
    using Expected = iceberg::expected<T, E>;
    Expected e;
    EXPECT_FALSE(noexcept(e.error_or(FromType{})));
  }
}
