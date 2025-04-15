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

#include "iceberg/transform_function.h"

#include <format>

#include "iceberg/type.h"

namespace iceberg {

IdentityTransform::IdentityTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kIdentity, source_type) {}

Result<ArrowArray> IdentityTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "IdentityTransform::Transform"});
}

Result<std::shared_ptr<Type>> IdentityTransform::ResultType() const {
  auto src_type = source_type();
  if (!src_type || !src_type->is_primitive()) {
    return unexpected(Error{
        .kind = ErrorKind::kNotSupported,
        .message = std::format("{} is not a valid input type for identity transform",
                               src_type ? src_type->ToString() : "null")});
  }
  return src_type;
}

BucketTransform::BucketTransform(std::shared_ptr<Type> const& source_type,
                                 int32_t num_buckets)
    : TransformFunction(TransformType::kBucket, source_type), num_buckets_(num_buckets) {}

Result<ArrowArray> BucketTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "BucketTransform::Transform"});
}

Result<std::shared_ptr<Type>> BucketTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "BucketTransform::result_type"});
}

TruncateTransform::TruncateTransform(std::shared_ptr<Type> const& source_type,
                                     int32_t width)
    : TransformFunction(TransformType::kTruncate, source_type), width_(width) {}

Result<ArrowArray> TruncateTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "TruncateTransform::Transform"});
}

Result<std::shared_ptr<Type>> TruncateTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "TruncateTransform::result_type"});
}

YearTransform::YearTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kTruncate, source_type) {}

Result<ArrowArray> YearTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "YearTransform::Transform"});
}

Result<std::shared_ptr<Type>> YearTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "YearTransform::result_type"});
}

MonthTransform::MonthTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kMonth, source_type) {}

Result<ArrowArray> MonthTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "MonthTransform::Transform"});
}

Result<std::shared_ptr<Type>> MonthTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "MonthTransform::result_type"});
}

DayTransform::DayTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kDay, source_type) {}

Result<ArrowArray> DayTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "DayTransform::Transform"});
}

Result<std::shared_ptr<Type>> DayTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "DayTransform::result_type"});
}

HourTransform::HourTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kHour, source_type) {}

Result<ArrowArray> HourTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "HourTransform::Transform"});
}

Result<std::shared_ptr<Type>> HourTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "HourTransform::result_type"});
}

VoidTransform::VoidTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kVoid, source_type) {}

Result<ArrowArray> VoidTransform::Transform(const ArrowArray& input) {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "VoidTransform::Transform"});
}

Result<std::shared_ptr<Type>> VoidTransform::ResultType() const {
  return unexpected<Error>(
      {.kind = ErrorKind::kNotImplemented, .message = "VoidTransform::result_type"});
}

}  // namespace iceberg
