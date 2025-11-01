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

#include <cassert>

#include "iceberg/expression/literal.h"
#include "iceberg/type.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/bucket_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/temporal_util.h"
#include "iceberg/util/truncate_util.h"

namespace iceberg {

IdentityTransform::IdentityTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kIdentity, source_type) {}

Result<Literal> IdentityTransform::Transform(const Literal& literal) { return literal; }

std::shared_ptr<Type> IdentityTransform::ResultType() const { return source_type(); }

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

Result<Literal> BucketTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto bucket_index,
                          BucketUtils::BucketIndex(literal, num_buckets_))

  return Literal::Int(bucket_index);
}

std::shared_ptr<Type> BucketTransform::ResultType() const { return int32(); }

Result<std::unique_ptr<TransformFunction>> BucketTransform::Make(
    std::shared_ptr<Type> const& source_type, int32_t num_buckets) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for bucket transform");
  }
  switch (source_type->type_id()) {
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
      break;
    default:
      return NotSupported("{} is not a valid input type for bucket transform",
                          source_type->ToString());
  }
  if (num_buckets <= 0) {
    return InvalidArgument("Number of buckets must be positive, got {}", num_buckets);
  }
  return std::make_unique<BucketTransform>(source_type, num_buckets);
}

TruncateTransform::TruncateTransform(std::shared_ptr<Type> const& source_type,
                                     int32_t width)
    : TransformFunction(TransformType::kTruncate, source_type), width_(width) {}

Result<Literal> TruncateTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  return TruncateUtils::TruncateLiteral(literal, width_);
}

std::shared_ptr<Type> TruncateTransform::ResultType() const { return source_type(); }

Result<std::unique_ptr<TransformFunction>> TruncateTransform::Make(
    std::shared_ptr<Type> const& source_type, int32_t width) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for truncate transform");
  }
  switch (source_type->type_id()) {
    case TypeId::kInt:
    case TypeId::kLong:
    case TypeId::kDecimal:
    case TypeId::kString:
    case TypeId::kBinary:
      break;
    default:
      return NotSupported("{} is not a valid input type for truncate transform",
                          source_type->ToString());
  }
  if (width <= 0) {
    return InvalidArgument("Width must be positive, got {}", width);
  }
  return std::make_unique<TruncateTransform>(source_type, width);
}

YearTransform::YearTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kTruncate, source_type) {}

Result<Literal> YearTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  return TemporalUtils::ExtractYear(literal);
}

std::shared_ptr<Type> YearTransform::ResultType() const { return int32(); }

Result<std::unique_ptr<TransformFunction>> YearTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for year transform");
  }
  switch (source_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      break;
    default:
      return NotSupported("{} is not a valid input type for year transform",
                          source_type->ToString());
  }
  return std::make_unique<YearTransform>(source_type);
}

MonthTransform::MonthTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kMonth, source_type) {}

Result<Literal> MonthTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  return TemporalUtils::ExtractMonth(literal);
}

std::shared_ptr<Type> MonthTransform::ResultType() const { return int32(); }

Result<std::unique_ptr<TransformFunction>> MonthTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for month transform");
  }
  switch (source_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      break;
    default:
      return NotSupported("{} is not a valid input type for month transform",
                          source_type->ToString());
  }
  return std::make_unique<MonthTransform>(source_type);
}

DayTransform::DayTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kDay, source_type) {}

Result<Literal> DayTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  return TemporalUtils::ExtractDay(literal);
}

std::shared_ptr<Type> DayTransform::ResultType() const { return date(); }

Result<std::unique_ptr<TransformFunction>> DayTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for day transform");
  }
  switch (source_type->type_id()) {
    case TypeId::kDate:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      break;
    default:
      return NotSupported("{} is not a valid input type for day transform",
                          source_type->ToString());
  }
  return std::make_unique<DayTransform>(source_type);
}

HourTransform::HourTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kHour, source_type) {}

Result<Literal> HourTransform::Transform(const Literal& literal) {
  ICEBERG_DCHECK(*literal.type() == *source_type(),
                 "Literal type must match source type");
  return TemporalUtils::ExtractHour(literal);
}

std::shared_ptr<Type> HourTransform::ResultType() const { return int32(); }

Result<std::unique_ptr<TransformFunction>> HourTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for hour transform");
  }
  switch (source_type->type_id()) {
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      break;
    default:
      return NotSupported("{} is not a valid input type for hour transform",
                          source_type->ToString());
  }
  return std::make_unique<HourTransform>(source_type);
}

VoidTransform::VoidTransform(std::shared_ptr<Type> const& source_type)
    : TransformFunction(TransformType::kVoid, source_type) {}

Result<Literal> VoidTransform::Transform(const Literal& literal) {
  return literal.IsNull() ? literal : Literal::Null(literal.type());
}

std::shared_ptr<Type> VoidTransform::ResultType() const { return source_type(); }

Result<std::unique_ptr<TransformFunction>> VoidTransform::Make(
    std::shared_ptr<Type> const& source_type) {
  if (!source_type) {
    return NotSupported("null is not a valid input type for void transform");
  }
  return std::make_unique<VoidTransform>(source_type);
}

}  // namespace iceberg
