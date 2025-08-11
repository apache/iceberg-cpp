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

#include "iceberg/transform.h"

#include <format>
#include <regex>
#include <utility>

#include "iceberg/transform_function.h"
#include "iceberg/type.h"

namespace iceberg {
namespace {
constexpr std::string_view kUnknownName = "unknown";
constexpr std::string_view kIdentityName = "identity";
constexpr std::string_view kBucketName = "bucket";
constexpr std::string_view kTruncateName = "truncate";
constexpr std::string_view kYearName = "year";
constexpr std::string_view kMonthName = "month";
constexpr std::string_view kDayName = "day";
constexpr std::string_view kHourName = "hour";
constexpr std::string_view kVoidName = "void";
}  // namespace

constexpr std::string_view TransformTypeToString(TransformType type) {
  switch (type) {
    case TransformType::kUnknown:
      return kUnknownName;
    case TransformType::kIdentity:
      return kIdentityName;
    case TransformType::kBucket:
      return kBucketName;
    case TransformType::kTruncate:
      return kTruncateName;
    case TransformType::kYear:
      return kYearName;
    case TransformType::kMonth:
      return kMonthName;
    case TransformType::kDay:
      return kDayName;
    case TransformType::kHour:
      return kHourName;
    case TransformType::kVoid:
      return kVoidName;
  }
  std::unreachable();
}

std::shared_ptr<Transform> Transform::Identity() {
  static auto instance =
      std::shared_ptr<Transform>(new Transform(TransformType::kIdentity));
  return instance;
}

std::shared_ptr<Transform> Transform::Year() {
  static auto instance = std::shared_ptr<Transform>(new Transform(TransformType::kYear));
  return instance;
}

std::shared_ptr<Transform> Transform::Month() {
  static auto instance = std::shared_ptr<Transform>(new Transform(TransformType::kMonth));
  return instance;
}

std::shared_ptr<Transform> Transform::Day() {
  static auto instance = std::shared_ptr<Transform>(new Transform(TransformType::kDay));
  return instance;
}

std::shared_ptr<Transform> Transform::Hour() {
  static auto instance = std::shared_ptr<Transform>(new Transform(TransformType::kHour));
  return instance;
}

std::shared_ptr<Transform> Transform::Void() {
  static auto instance = std::shared_ptr<Transform>(new Transform(TransformType::kVoid));
  return instance;
}

std::shared_ptr<Transform> Transform::Bucket(int32_t num_buckets) {
  return std::shared_ptr<Transform>(new Transform(TransformType::kBucket, num_buckets));
}

std::shared_ptr<Transform> Transform::Truncate(int32_t width) {
  return std::shared_ptr<Transform>(new Transform(TransformType::kTruncate, width));
}

Transform::Transform(TransformType transform_type) : transform_type_(transform_type) {}

Transform::Transform(TransformType transform_type, int32_t param)
    : transform_type_(transform_type), param_(param) {}

TransformType Transform::transform_type() const { return transform_type_; }

Result<std::unique_ptr<TransformFunction>> Transform::Bind(
    const std::shared_ptr<Type>& source_type) const {
  auto type_str = TransformTypeToString(transform_type_);

  switch (transform_type_) {
    case TransformType::kIdentity:
      return IdentityTransform::Make(source_type);

    case TransformType::kBucket: {
      if (auto param = std::get_if<int32_t>(&param_)) {
        return BucketTransform::Make(source_type, *param);
      }
      return InvalidArgument("Bucket requires int32 param, none found in transform '{}'",
                             type_str);
    }

    case TransformType::kTruncate: {
      if (auto param = std::get_if<int32_t>(&param_)) {
        return TruncateTransform::Make(source_type, *param);
      }
      return InvalidArgument(
          "Truncate requires int32 param, none found in transform '{}'", type_str);
    }

    case TransformType::kYear:
      return YearTransform::Make(source_type);
    case TransformType::kMonth:
      return MonthTransform::Make(source_type);
    case TransformType::kDay:
      return DayTransform::Make(source_type);
    case TransformType::kHour:
      return HourTransform::Make(source_type);
    case TransformType::kVoid:
      return VoidTransform::Make(source_type);

    default:
      return NotSupported("Unsupported transform type: '{}'", type_str);
  }
}

bool TransformFunction::Equals(const TransformFunction& other) const {
  return transform_type_ == other.transform_type_ && *source_type_ == *other.source_type_;
}

std::string Transform::ToString() const {
  switch (transform_type_) {
    case TransformType::kIdentity:
    case TransformType::kYear:
    case TransformType::kMonth:
    case TransformType::kDay:
    case TransformType::kHour:
    case TransformType::kVoid:
    case TransformType::kUnknown:
      return std::format("{}", TransformTypeToString(transform_type_));
    case TransformType::kBucket:
    case TransformType::kTruncate:
      return std::format("{}[{}]", TransformTypeToString(transform_type_),
                         std::get<int32_t>(param_));
  }
  std::unreachable();
}

TransformFunction::TransformFunction(TransformType transform_type,
                                     std::shared_ptr<Type> source_type)
    : transform_type_(transform_type), source_type_(std::move(source_type)) {}

TransformType TransformFunction::transform_type() const { return transform_type_; }

std::shared_ptr<Type> const& TransformFunction::source_type() const {
  return source_type_;
}

bool Transform::Equals(const Transform& other) const {
  return transform_type_ == other.transform_type_ && param_ == other.param_;
}

Result<std::shared_ptr<Transform>> TransformFromString(std::string_view transform_str) {
  if (transform_str == kIdentityName) return Transform::Identity();
  if (transform_str == kYearName) return Transform::Year();
  if (transform_str == kMonthName) return Transform::Month();
  if (transform_str == kDayName) return Transform::Day();
  if (transform_str == kHourName) return Transform::Hour();
  if (transform_str == kVoidName) return Transform::Void();

  // Match bucket[16] or truncate[4]
  static const std::regex param_regex(
      std::format(R"(({}|{})\[(\d+)\])", kBucketName, kTruncateName));
  std::string str(transform_str);
  std::smatch match;
  if (std::regex_match(str, match, param_regex)) {
    const std::string type_str = match[1];
    const int32_t param = std::stoi(match[2]);

    if (type_str == kBucketName) {
      return Transform::Bucket(param);
    }
    if (type_str == kTruncateName) {
      return Transform::Truncate(param);
    }
  }

  return InvalidArgument("Invalid Transform string: {}", transform_str);
}

}  // namespace iceberg
