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

#include "iceberg/row/struct_like.h"

#include <utility>

#include "iceberg/result.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

StructLikeAccessor::StructLikeAccessor(std::shared_ptr<Type> type,
                                       std::span<const size_t> position_path)
    : type_(std::move(type)) {
  if (position_path.size() == 1) {
    accessor_ = [pos =
                     position_path[0]](const StructLike& struct_like) -> Result<Scalar> {
      return struct_like.GetField(pos);
    };
  } else if (position_path.size() == 2) {
    accessor_ = [pos0 = position_path[0], pos1 = position_path[1]](
                    const StructLike& struct_like) -> Result<Scalar> {
      ICEBERG_ASSIGN_OR_RAISE(auto first_level_field, struct_like.GetField(pos0));
      if (!std::holds_alternative<std::shared_ptr<StructLike>>(first_level_field)) {
        return InvalidSchema("Encountered non-struct in the position path [{},{}]", pos0,
                             pos1);
      }
      return std::get<std::shared_ptr<StructLike>>(first_level_field)->GetField(pos1);
    };
  } else if (!position_path.empty()) {
    accessor_ = [position_path](const StructLike& struct_like) -> Result<Scalar> {
      std::vector<std::shared_ptr<StructLike>> backups;
      const StructLike* current_struct_like = &struct_like;
      for (size_t i = 0; i < position_path.size() - 1; ++i) {
        ICEBERG_ASSIGN_OR_RAISE(auto field,
                                current_struct_like->GetField(position_path[i]));
        if (!std::holds_alternative<std::shared_ptr<StructLike>>(field)) {
          return InvalidSchema("Encountered non-struct in the position path [{}]",
                               position_path);
        }
        backups.push_back(std::get<std::shared_ptr<StructLike>>(field));
        current_struct_like = backups.back().get();
      }
      return current_struct_like->GetField(position_path.back());
    };
  } else {
    accessor_ = [](const StructLike&) -> Result<Scalar> {
      return Invalid("Cannot read StructLike with empty position path");
    };
  }
}

Result<Literal> StructLikeAccessor::GetLiteral(const StructLike& struct_like) const {
  if (!type_->is_primitive()) {
    return NotSupported("Cannot get literal value for non-primitive type {}",
                        type_->ToString());
  }

  ICEBERG_ASSIGN_OR_RAISE(auto scalar, Get(struct_like));

  if (std::holds_alternative<std::monostate>(scalar)) {
    return Literal::Null(internal::checked_pointer_cast<PrimitiveType>(type_));
  }

  switch (type_->type_id()) {
    case TypeId::kBoolean:
      return Literal::Boolean(std::get<bool>(scalar));
    case TypeId::kInt:
      return Literal::Int(std::get<int32_t>(scalar));
    case TypeId::kLong:
      return Literal::Long(std::get<int64_t>(scalar));
    case TypeId::kFloat:
      return Literal::Float(std::get<float>(scalar));
    case TypeId::kDouble:
      return Literal::Double(std::get<double>(scalar));
    case TypeId::kString:
      return Literal::String(std::string(std::get<std::string_view>(scalar)));
    case TypeId::kBinary: {
      auto binary_data = std::get<std::string_view>(scalar);
      return Literal::Binary(
          std::vector<uint8_t>(binary_data.cbegin(), binary_data.cend()));
    }
    case TypeId::kDecimal: {
      const auto& decimal_type = internal::checked_cast<const DecimalType&>(*type_);
      return Literal::Decimal(std::get<Decimal>(scalar).value(), decimal_type.precision(),
                              decimal_type.scale());
    }
    case TypeId::kDate:
      return Literal::Date(std::get<int32_t>(scalar));
    case TypeId::kTime:
      return Literal::Time(std::get<int64_t>(scalar));
    case TypeId::kTimestamp:
      return Literal::Timestamp(std::get<int64_t>(scalar));
    case TypeId::kTimestampTz:
      return Literal::TimestampTz(std::get<int64_t>(scalar));
    case TypeId::kFixed: {
      const auto& fixed_data = std::get<std::string_view>(scalar);
      return Literal::Fixed(std::vector<uint8_t>(fixed_data.cbegin(), fixed_data.cend()));
    }
    case TypeId::kUuid:
      // TODO(gangwu): Implement UUID type
    default:
      return NotSupported("Cannot convert scalar to literal of type {}",
                          type_->ToString());
  }

  std::unreachable();
}

}  // namespace iceberg
