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

namespace iceberg {

namespace {
/// \brief Get the relative transform name
constexpr std::string_view ToString(TransformType type) {
  switch (type) {
    case TransformType::kUnknown:
      return "unknown";
    case TransformType::kIdentity:
      return "identity";
    case TransformType::kBucket:
      return "bucket";
    case TransformType::kTruncate:
      return "truncate";
    case TransformType::kYear:
      return "year";
    case TransformType::kMonth:
      return "month";
    case TransformType::kDay:
      return "day";
    case TransformType::kHour:
      return "hour";
    case TransformType::kVoid:
      return "void";
    default:
      return "invalid";
  }
}
}  // namespace

TransformFunction::TransformFunction(TransformType type) : transform_type_(type) {}

TransformType TransformFunction::transform_type() const { return transform_type_; }

std::string TransformFunction::ToString() const {
  return std::format("{}", iceberg::ToString(transform_type_));
}

bool TransformFunction::Equals(const TransformFunction& other) const {
  return transform_type_ == other.transform_type_;
}

IdentityTransformFunction::IdentityTransformFunction()
    : TransformFunction(TransformType::kIdentity) {}

expected<ArrowArray, Error> IdentityTransformFunction::Transform(
    const ArrowArray& input) {
  return unexpected<Error>({.kind = ErrorKind::kNotSupported,
                            .message = "IdentityTransformFunction::Transform"});
}

expected<std::unique_ptr<TransformFunction>, Error> TransformFunctionFromString(
    std::string_view str) {
  if (str == "identity") {
    return std::make_unique<IdentityTransformFunction>();
  }
  return unexpected<Error>(
      {.kind = ErrorKind::kInvalidArgument,
       .message = "Invalid TransformFunction string: " + std::string(str)});
}

}  // namespace iceberg
