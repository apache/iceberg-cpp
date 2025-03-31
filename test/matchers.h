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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/error.h"

/*
 * \brief Define custom matchers for expected<T, Error> values
 *
 * Example usage of these matchers:
 *
 * Basic assertions:
 *
 *   // Check that a result is ok
 *   EXPECT_THAT(result, IsOk());
 *
 *   // Check that a result is an error of a specific kind
 *   EXPECT_THAT(result, IsError(ErrorKind::kNoSuchTable));
 *
 *   // Check that an error message contains a specific substring
 *   EXPECT_THAT(result, HasErrorMessage("table not found"));
 *
 * Value inspection:
 *
 *   // Check that a result has a value that equals 42
 *   EXPECT_THAT(result, HasValue(42));
 *
 *   // Check that a result has a value that satisfies a complex condition
 *   EXPECT_THAT(result, HasValue(AllOf(Gt(10), Lt(50))));
 *
 * Combined assertions:
 *
 *   // Check that the result value has a specific property
 *   EXPECT_THAT(result, ResultIs(Property(&MyType::name, "example")));
 *
 *   // Check that the error matches specific criteria
 *   EXPECT_THAT(result, ErrorIs(AllOf(
 *     Property(&Error::kind, ErrorKind::kNoSuchTable),
 *     Property(&Error::message, HasSubstr("table not found"))
 *   )));
 */

namespace iceberg {

// IsOk matcher that checks if the expected value has a value (not an error)
MATCHER(IsOk, "is an Ok result") {
  if (arg.has_value()) {
    return true;
  }
  *result_listener << "which contains error: " << arg.error().message;
  return false;
}

// IsError matcher that checks if the expected value contains an error
MATCHER_P(IsError, kind, "is an Error with the specified kind") {
  if (!arg.has_value()) {
    if (arg.error().kind == kind) {
      return true;
    }
    *result_listener << "which contains error kind " << static_cast<int>(arg.error().kind)
                     << " but expected " << static_cast<int>(kind)
                     << ", message: " << arg.error().message;
    return false;
  }
  *result_listener << "which is not an error but a value";
  return false;
}

// HasErrorMessage matcher that checks if the expected value contains an error with a
// specific message or substring
MATCHER_P(HasErrorMessage, message_substr,
          "is an Error with message containing the substring") {
  if (!arg.has_value()) {
    if (arg.error().message.find(message_substr) != std::string::npos) {
      return true;
    }
    *result_listener << "which contains error with message '" << arg.error().message
                     << "' that doesn't contain '" << message_substr << "'";
    return false;
  }
  *result_listener << "which is not an error but a value";
  return false;
}

// HasValue matcher that checks if the expected value contains a value that matches the
// given matcher
template <typename MatcherT>
class HasValueMatcher {
 public:
  explicit HasValueMatcher(MatcherT matcher) : matcher_(std::move(matcher)) {}

  template <typename T>
  bool MatchAndExplain(const T& value,
                       ::testing::MatchResultListener* result_listener) const {
    if (!value.has_value()) {
      *result_listener << "which is an error: " << value.error().message;
      return false;
    }

    return ::testing::MatcherCast<const typename T::value_type&>(matcher_)
        .MatchAndExplain(*value, result_listener);
  }

  void DescribeTo(std::ostream* os) const {
    *os << "has a value that ";
    matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const {
    *os << "does not have a value that ";
    matcher_.DescribeTo(os);
  }

 private:
  MatcherT matcher_;
};

// Factory function for HasValueMatcher
template <typename MatcherT>
auto HasValue(MatcherT&& matcher) {
  return ::testing::MakePolymorphicMatcher(
      HasValueMatcher<std::decay_t<MatcherT>>(std::forward<MatcherT>(matcher)));
}

// Overload for the common case where we just want to check for presence of any value
inline auto HasValue() { return IsOk(); }

// Matcher that checks an expected value against an expected value and a matcher
template <typename MatcherT>
class ResultMatcher {
 public:
  explicit ResultMatcher(bool should_have_value, MatcherT matcher)
      : should_have_value_(should_have_value), matcher_(std::move(matcher)) {}

  template <typename T>
  bool MatchAndExplain(const T& value,
                       ::testing::MatchResultListener* result_listener) const {
    if (value.has_value() != should_have_value_) {
      if (should_have_value_) {
        *result_listener << "which is an error: " << value.error().message;
      } else {
        *result_listener << "which is a value, not an error";
      }
      return false;
    }

    if (should_have_value_) {
      return ::testing::MatcherCast<const typename T::value_type&>(matcher_)
          .MatchAndExplain(*value, result_listener);
    } else {
      return ::testing::MatcherCast<const typename T::error_type&>(matcher_)
          .MatchAndExplain(value.error(), result_listener);
    }
  }

  void DescribeTo(std::ostream* os) const {
    if (should_have_value_) {
      *os << "has a value that ";
    } else {
      *os << "has an error that ";
    }
    matcher_.DescribeTo(os);
  }

  void DescribeNegationTo(std::ostream* os) const {
    if (should_have_value_) {
      *os << "does not have a value that ";
    } else {
      *os << "does not have an error that ";
    }
    matcher_.DescribeTo(os);
  }

 private:
  bool should_have_value_;
  MatcherT matcher_;
};

// Factory function for ResultMatcher for values
template <typename MatcherT>
auto ResultIs(MatcherT&& matcher) {
  return ::testing::MakePolymorphicMatcher(
      ResultMatcher<std::decay_t<MatcherT>>(true, std::forward<MatcherT>(matcher)));
}

// Factory function for ResultMatcher for errors
template <typename MatcherT>
auto ErrorIs(MatcherT&& matcher) {
  return ::testing::MakePolymorphicMatcher(
      ResultMatcher<std::decay_t<MatcherT>>(false, std::forward<MatcherT>(matcher)));
}

}  // namespace iceberg
