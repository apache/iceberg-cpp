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

#include "iceberg/transform_function.h"
#include "iceberg/type.h"

namespace iceberg {

std::shared_ptr<Transform> Transform::Identity() {
  static auto instance = std::make_shared<Transform>(TransformType::kIdentity);
  return instance;
}

Transform::Transform(TransformType transform_type) : transform_type_(transform_type) {}

Transform::Transform(TransformType transform_type, int32_t param)
    : transform_type_(transform_type), param_(param) {}

TransformType Transform::transform_type() const { return transform_type_; }

expected<std::unique_ptr<TransformFunction>, Error> Transform::Bind(
    const std::shared_ptr<Type>& source_type) const {
  auto type_str = TransformTypeToString(transform_type_);

  switch (transform_type_) {
    case TransformType::kIdentity:
      return std::make_unique<IdentityTransform>(source_type);

    case TransformType::kBucket: {
      if (auto param = std::get_if<int32_t>(&param_)) {
        return std::make_unique<BucketTransform>(source_type, *param);
      }
      return unexpected<Error>({
          .kind = ErrorKind::kInvalidArgument,
          .message = std::format(
              "Bucket requires int32 param, none found in transform '{}'", type_str),
      });
    }

    case TransformType::kTruncate: {
      if (auto param = std::get_if<int32_t>(&param_)) {
        return std::make_unique<TruncateTransform>(source_type, *param);
      }
      return unexpected<Error>({
          .kind = ErrorKind::kInvalidArgument,
          .message = std::format(
              "Truncate requires int32 param, none found in transform '{}'", type_str),
      });
    }

    case TransformType::kYear:
      return std::make_unique<YearTransform>(source_type);
    case TransformType::kMonth:
      return std::make_unique<MonthTransform>(source_type);
    case TransformType::kDay:
      return std::make_unique<DayTransform>(source_type);
    case TransformType::kHour:
      return std::make_unique<HourTransform>(source_type);
    case TransformType::kVoid:
      return std::make_unique<VoidTransform>(source_type);

    default:
      return unexpected<Error>({
          .kind = ErrorKind::kNotSupported,
          .message = std::format("Unsupported transform type: '{}'", type_str),
      });
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

expected<std::unique_ptr<Transform>, Error> TransformFromString(std::string_view str) {
  if (str == "identity") {
    return std::make_unique<Transform>(TransformType::kIdentity);
  }
  return unexpected<Error>({.kind = ErrorKind::kInvalidArgument,
                            .message = std::format("Invalid Transform string: {}", str)});
}

}  // namespace iceberg
