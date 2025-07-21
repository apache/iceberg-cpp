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

#include "iceberg/type.h"

namespace iceberg {

IdentityTransform::IdentityTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kIdentity, source_type) {}

Result<ArrowArray> IdentityTransform::Transform(const ArrowArray& input) {
  return NotImplemented("IdentityTransform::Transform");
}

Result<std::shared_ptr<Type>> IdentityTransform::ResultType() const {
  return source_type();
}

Result<std::unique_ptr<TransformFunction>> IdentityTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type || !source_type->is_primitive()) {
    return NotSupported("{} is not a valid input type for identity transform",
                        source_type ? source_type->ToString() : "null");
  }
  return std::make_unique<IdentityTransform>(source_type);
}

BucketTransform::BucketTransform(std::shared_ptr<Type> const& source_type,
                                 int32_t num_buckets)
    : TransformFunction(TransformType::kBucket, source_type), num_buckets_(num_buckets) {}

Result<ArrowArray> BucketTransform::Transform(const ArrowArray& input) {
  return NotImplemented("BucketTransform::Transform");
}

Result<std::shared_ptr<Type>> BucketTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kInt:
    case TypeId::kLong:
    case TypeId::kDecimal:
    case TypeId::kDate:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kString:
    case TypeId::kUuid:
    case TypeId::kFixed:
    case TypeId::kBinary:
      return iceberg::int32();
    default:
      return NotSupported("{} is not a valid input type for bucket transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> BucketTransform::Make(
    std::shared_ptr<Type> const& source_type, int32_t num_buckets) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for bucket transform");
  }
  return std::make_unique<BucketTransform>(source_type, num_buckets);
}

TruncateTransform::TruncateTransform(std::shared_ptr<Type> const& source_type,
                                     int32_t width)
    : TransformFunction(TransformType::kTruncate, source_type), width_(width) {}

Result<ArrowArray> TruncateTransform::Transform(const ArrowArray& input) {
  return NotImplemented("TruncateTransform::Transform");
}

Result<std::shared_ptr<Type>> TruncateTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kInt:
    case TypeId::kLong:
    case TypeId::kDecimal:
    case TypeId::kString:
    case TypeId::kBinary:
      return src_type;
    default:
      return NotSupported("{} is not a valid input type for truncate transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> TruncateTransform::Make(
    std::shared_ptr<Type> const& source_type, int32_t width) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for truncate transform");
  }
  return std::make_unique<TruncateTransform>(source_type, width);
}

YearTransform::YearTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kTruncate, source_type) {}

Result<ArrowArray> YearTransform::Transform(const ArrowArray& input) {
  return NotImplemented("YearTransform::Transform");
}

Result<std::shared_ptr<Type>> YearTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return iceberg::int32();
    default:
      return NotSupported("{} is not a valid input type for year transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> YearTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for year transform");
  }
  return std::make_unique<YearTransform>(source_type);
}

MonthTransform::MonthTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kMonth, source_type) {}

Result<ArrowArray> MonthTransform::Transform(const ArrowArray& input) {
  return NotImplemented("MonthTransform::Transform");
}

Result<std::shared_ptr<Type>> MonthTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return iceberg::int32();
    default:
      return NotSupported("{} is not a valid input type for month transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> MonthTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for month transform");
  }
  return std::make_unique<MonthTransform>(source_type);
}

DayTransform::DayTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kDay, source_type) {}

Result<ArrowArray> DayTransform::Transform(const ArrowArray& input) {
  return NotImplemented("DayTransform::Transform");
}

Result<std::shared_ptr<Type>> DayTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return iceberg::date();
    default:
      return NotSupported("{} is not a valid input type for day transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> DayTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for day transform");
  }
  return std::make_unique<DayTransform>(source_type);
}

HourTransform::HourTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kHour, source_type) {}

Result<ArrowArray> HourTransform::Transform(const ArrowArray& input) {
  return NotImplemented("HourTransform::Transform");
}

Result<std::shared_ptr<Type>> HourTransform::ResultType() const {
  auto src_type = source_type();
  switch (src_type->type_id()) {
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      return iceberg::int32();
    default:
      return NotSupported("{} is not a valid input type for hour transform",
                          src_type->ToString());
  }
}

Result<std::unique_ptr<TransformFunction>> HourTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for hour transform");
  }
  return std::make_unique<HourTransform>(source_type);
}

VoidTransform::VoidTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kVoid, source_type) {}

Result<ArrowArray> VoidTransform::Transform(const ArrowArray& input) {
  return NotImplemented("VoidTransform::Transform");
}

Result<std::shared_ptr<Type>> VoidTransform::ResultType() const { return source_type(); }

Result<std::unique_ptr<TransformFunction>> VoidTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for void transform");
  }
  return std::make_unique<VoidTransform>(source_type);
}

}  // namespace iceberg
