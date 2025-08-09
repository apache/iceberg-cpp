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
#include <chrono>
#include <type_traits>
#include <utility>
#include <variant>

#include "iceberg/expression/literal.h"
#include "iceberg/type.h"
#include "iceberg/util/murmurhash3_internal.h"
#include "iceberg/util/truncate_utils.h"

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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply bucket transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  int32_t hash_value = 0;
  std::visit(
      [&](auto&& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, int32_t>) {
          MurmurHash3_x86_32(&value, sizeof(int32_t), 0, &hash_value);
        } else if constexpr (std::is_same_v<T, int64_t>) {
          MurmurHash3_x86_32(&value, sizeof(int64_t), 0, &hash_value);
        } else if constexpr (std::is_same_v<T, std::array<uint8_t, 16>>) {
          MurmurHash3_x86_32(value.data(), sizeof(uint8_t) * 16, 0, &hash_value);
        } else if constexpr (std::is_same_v<T, std::string>) {
          MurmurHash3_x86_32(value.data(), value.size(), 0, &hash_value);
        } else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) {
          MurmurHash3_x86_32(value.data(), value.size(), 0, &hash_value);
        } else if constexpr (std::is_same_v<T, std::monostate> ||
                             std::is_same_v<T, bool> || std::is_same_v<T, float> ||
                             std::is_same_v<T, double> ||
                             std::is_same_v<T, Literal::BelowMin> ||
                             std::is_same_v<T, Literal::AboveMax>) {
          std::unreachable();
        } else {
          static_assert(false, "Unhandled type in BucketTransform::Transform");
        }
      },
      literal.value());

  // Calculate the bucket index
  int32_t bucket_index =
      (hash_value & std::numeric_limits<int32_t>::max()) % num_buckets_;

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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply truncate transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }
  if (literal.IsNull()) [[unlikely]] {
    // Return null as is
    return literal;
  }

  switch (source_type()->type_id()) {
    case TypeId::kInt: {
      auto value = std::get<int32_t>(literal.value());
      return Literal::Int(TruncateUtils::TruncateInteger(value, width_));
    }
    case TypeId::kLong: {
      auto value = std::get<int64_t>(literal.value());
      return Literal::Long(TruncateUtils::TruncateInteger(value, width_));
    }
    case TypeId::kDecimal: {
      // TODO(zhjwpku): Handle decimal truncation logic here
      return NotImplemented("Truncate for Decimal is not implemented yet");
    }
    case TypeId::kString: {
      // Strings are truncated to a valid UTF-8 string with no more than L code points.
      auto value = std::get<std::string>(literal.value());
      return Literal::String(TruncateUtils::TruncateUTF8(std::move(value), width_));
    }
    case TypeId::kBinary: {
      /// In contrast to strings, binary values do not have an assumed encoding and are
      /// truncated to L bytes.
      auto value = std::get<std::vector<uint8_t>>(literal.value());
      if (value.size() > static_cast<size_t>(width_)) {
        value.resize(width_);
      }
      return Literal::Binary(std::move(value));
    }
    default:
      std::unreachable();
  }
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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply year transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  using namespace std::chrono;  // NOLINT
  switch (source_type()->type_id()) {
    case TypeId::kDate: {
      auto value = std::get<int32_t>(literal.value());
      auto epoch = sys_days(year{1970} / January / 1);
      auto ymd = year_month_day(epoch + days{value});
      return Literal::Int(static_cast<int32_t>(ymd.year()));
    }
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      auto value = std::get<int64_t>(literal.value());
      // Convert microseconds-since-epoch into a `year_month_day` object
      auto ymd = year_month_day(floor<days>(sys_time<microseconds>(microseconds{value})));
      return Literal::Int(static_cast<int32_t>(ymd.year()));
    }
    default:
      std::unreachable();
  }
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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply month transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  using namespace std::chrono;  // NOLINT
  switch (source_type()->type_id()) {
    case TypeId::kDate: {
      auto value = std::get<int32_t>(literal.value());
      auto epoch = sys_days(year{1970} / January / 1);
      auto ymd = year_month_day(epoch + days{value});
      auto epoch_ymd = year_month_day(epoch);
      auto delta = ymd.year() - epoch_ymd.year();
      // Calculate the month as months from 1970-01
      // Note: January is month 1, so we subtract 1 to get zero-based
      // month count.
      return Literal::Int(static_cast<int32_t>(delta.count() * 12 +
                                               static_cast<unsigned>(ymd.month()) - 1));
    }
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      auto value = std::get<int64_t>(literal.value());
      // Convert microseconds-since-epoch into a `year_month_day` object
      auto ymd = year_month_day(floor<days>(sys_time<microseconds>(microseconds{value})));
      auto epoch_ymd = year_month_day(year{1970} / January / 1);
      auto delta = ymd.year() - epoch_ymd.year();
      // Calculate the month as months from 1970-01
      // Note: January is month 1, so we subtract 1 to get zero-based
      // month count.
      return Literal::Int(static_cast<int32_t>(delta.count() * 12 +
                                               static_cast<unsigned>(ymd.month()) - 1));
    }
    default:
      std::unreachable();
  }
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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply day transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }
  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  using namespace std::chrono;  // NOLINT
  switch (source_type()->type_id()) {
    case TypeId::kDate: {
      return Literal::Int(std::get<int32_t>(literal.value()));
    }
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      auto value = std::get<int64_t>(literal.value());
      // Convert microseconds to `sys_days` (chronological days since epoch)
      auto timestamp = sys_time<microseconds>(microseconds{value});
      auto days_since_epoch = floor<days>(timestamp);

      return Literal::Int(
          static_cast<int32_t>(days_since_epoch.time_since_epoch().count()));
    }
    default:
      std::unreachable();
  }
}

std::shared_ptr<Type> DayTransform::ResultType() const { return int32(); }

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
  assert(literal.type() == source_type());
  if (literal.IsBelowMin() || literal.IsAboveMax()) {
    return InvalidArgument(
        "Cannot apply hour transform to literal with value {} of type {}",
        literal.ToString(), source_type()->ToString());
  }

  if (literal.IsNull()) [[unlikely]] {
    return Literal::Null(int32());
  }

  using namespace std::chrono;  // NOLINT
  switch (source_type()->type_id()) {
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz: {
      auto value = std::get<int64_t>(literal.value());
      // Create a `sys_time` object from the microseconds value
      auto timestamp = sys_time<microseconds>(microseconds{value});

      // Convert the time since epoch directly into hours
      auto hours_since_epoch = duration_cast<hours>(timestamp.time_since_epoch()).count();

      return Literal::Int(static_cast<int32_t>(hours_since_epoch));
    }
    default:
      std::unreachable();
  }
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
