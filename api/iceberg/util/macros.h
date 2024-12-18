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

#include <cstdint>

#define ICEBERG_EXPAND(x) x
#define ICEBERG_STRINGIFY(x) #x
#define ICEBERG_CONCAT(x, y) x##y

#ifndef ICEBERG_DISALLOW_COPY_AND_ASSIGN
#  define ICEBERG_DISALLOW_COPY_AND_ASSIGN(TypeName) \
    TypeName(const TypeName&) = delete;              \
    void operator=(const TypeName&) = delete
#endif

#ifndef ICEBERG_DEFAULT_MOVE_AND_ASSIGN
#  define ICEBERG_DEFAULT_MOVE_AND_ASSIGN(TypeName) \
    TypeName(TypeName&&) = default;                 \
    TypeName& operator=(TypeName&&) = default
#endif

// With ICEBERG_PREDICT_FALSE, GCC and clang can be told that a certain branch is
// not likely to be taken (for instance, a CHECK failure), and use that information in
// static analysis. Giving the compiler this information can affect the generated code
// layout in the absence of better information (i.e. -fprofile-arcs). [1] explains how
// this feature can be used to improve code generation. It was written as a positive
// comment to a negative article about the use of these annotations.
//
// ICEBERG_COMPILER_ASSUME allows the compiler to assume that a given expression is
// true, without evaluating it, and to optimise based on this assumption [2]. If this
// condition is violated at runtime, the behavior is undefined. This can be useful to
// generate both faster and smaller code in compute kernels.
//
// IMPORTANT: Different optimisers are likely to react differently to this annotation!
// It should be used with care when we can prove by some means that the assumption
// is (1) guaranteed to always hold and (2) is useful for optimization [3]. If the
// assumption is pessimistic, it might even block the compiler from decisions that
// could lead to better code [4]. If you have a good intuition for what the compiler
// can do with assumptions [5], you can use this macro to guide it and end up with
// results you would only get with more complex code transformations.
// `clang -S -emit-llvm` can be used to check how the generated code changes with
// your specific use of this macro.
//
// [1] https://lobste.rs/s/uwgtkt/don_t_use_likely_unlikely_attributes#c_xi3wmc
// [2] "Portable assumptions"
//     https://www.open-std.org/jtc1/sc22/wg21/docs/papers/2021/p1774r4.pdf
// [3] "Assertions Are Pessimistic, Assumptions Are Optimistic"
//     https://blog.regehr.org/archives/1096
// [4] https://discourse.llvm.org/t/llvm-assume-blocks-optimization/71609
// [5] J. Doerfert et al. 2019. "Performance Exploration Through Optimistic Static
//     Program Annotations". https://github.com/jdoerfert/PETOSPA/blob/master/ISC19.pdf
#define ICEBERG_UNUSED(x) (void)(x)
#ifdef ICEBERG_WARN_DOCUMENTATION
#  define ICEBERG_ARG_UNUSED(x) x
#else
#  define ICEBERG_ARG_UNUSED(x)
#endif
#if defined(__GNUC__)  // GCC and compatible compilers (clang, Intel ICC)
#  define ICEBERG_NORETURN __attribute__((noreturn))
#  define ICEBERG_NOINLINE __attribute__((noinline))
#  define ICEBERG_FORCE_INLINE __attribute__((always_inline))
#  define ICEBERG_PREDICT_FALSE(x) (__builtin_expect(!!(x), 0))
#  define ICEBERG_PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#  define ICEBERG_RESTRICT __restrict
#  if defined(__clang__)  // clang-specific
#    define ICEBERG_COMPILER_ASSUME(expr) __builtin_assume(expr)
#  else  // GCC-specific
#    if __GNUC__ >= 13
#      define ICEBERG_COMPILER_ASSUME(expr) __attribute__((assume(expr)))
#    else
// GCC does not have a built-in assume intrinsic before GCC 13, so we use an
// if statement and __builtin_unreachable() to achieve the same effect [2].
// Unlike clang's __builtin_assume and C++23's [[assume(expr)]], using this
// on GCC won't warn about side-effects in the expression, so make sure expr
// is side-effect free when working with GCC versions before 13 (Jan-2024),
// otherwise clang/MSVC builds will fail in CI.
#      define ICEBERG_COMPILER_ASSUME(expr) \
        if (expr) {                         \
        } else {                            \
          __builtin_unreachable();          \
        }
#    endif  // __GNUC__ >= 13
#  endif
#elif defined(_MSC_VER)  // MSVC
#  define ICEBERG_NORETURN __declspec(noreturn)
#  define ICEBERG_NOINLINE __declspec(noinline)
#  define ICEBERG_FORCE_INLINE __forceinline
#  define ICEBERG_PREDICT_FALSE(x) (x)
#  define ICEBERG_PREDICT_TRUE(x) (x)
#  define ICEBERG_RESTRICT __restrict
#  define ICEBERG_COMPILER_ASSUME(expr) __assume(expr)
#else
#  define ICEBERG_NORETURN
#  define ICEBERG_NOINLINE
#  define ICEBERG_FORCE_INLINE
#  define ICEBERG_PREDICT_FALSE(x) (x)
#  define ICEBERG_PREDICT_TRUE(x) (x)
#  define ICEBERG_RESTRICT
#  define ICEBERG_COMPILER_ASSUME(expr)
#endif

// ----------------------------------------------------------------------
// From googletest

// When you need to test the private or protected members of a class,
// use the FRIEND_TEST macro to declare your tests as friends of the
// class.  For example:
//
// class MyClass {
//  private:
//   void MyMethod();
//   FRIEND_TEST(MyClassTest, MyMethod);
// };
//
// class MyClassTest : public testing::Test {
//   // ...
// };
//
// TEST_F(MyClassTest, MyMethod) {
//   // Can call MyClass::MyMethod() here.
// }

#define FRIEND_TEST(test_case_name, test_name) \
  friend class test_case_name##_##test_name##_Test
