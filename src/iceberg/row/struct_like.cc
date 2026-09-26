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

#include <string>
#include <utility>
#include <vector>

#include "iceberg/result.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/formatter_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<Scalar> LiteralToScalar(const Literal& literal) {
  if (literal.IsNull()) {
    return Scalar{std::monostate{}};
  }

  switch (literal.type()->type_id()) {
    case TypeId::kBoolean:
      return Scalar{std::get<bool>(literal.value())};
    case TypeId::kInt:
    case TypeId::kDate:
      return Scalar{std::get<int32_t>(literal.value())};
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
    case TypeId::kTimestampNs:
    case TypeId::kTimestampTzNs:
      return Scalar{std::get<int64_t>(literal.value())};
    case TypeId::kFloat:
      return Scalar{std::get<float>(literal.value())};
    case TypeId::kDouble:
      return Scalar{std::get<double>(literal.value())};
    case TypeId::kString: {
      const auto& str = std::get<std::string>(literal.value());
      return Scalar{std::string_view(str)};
    }
    case TypeId::kBinary:
    case TypeId::kFixed: {
      const auto& bytes = std::get<std::vector<uint8_t>>(literal.value());
      return Scalar{
          std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
    }
    case TypeId::kUuid: {
      const auto& uuid = std::get<Uuid>(literal.value());
      const auto& bytes = uuid.bytes();
      return Scalar{
          std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size())};
    }
    case TypeId::kDecimal:
      return Scalar{std::get<Decimal>(literal.value())};
    default:
      return NotSupported("Cannot convert literal of type {} to Scalar",
                          literal.type()->ToString());
  }
}

namespace {

Result<Scalar> GetCheckedField(const StructLike& row, size_t pos, bool optional) {
  ICEBERG_ASSIGN_OR_RAISE(auto field, row.GetField(pos));
  if (!optional && std::holds_alternative<std::monostate>(field)) {
    return InvalidArgument("Required field at position {} is null", pos);
  }
  return field;
}

Result<std::shared_ptr<StructLike>> GetParentStruct(const StructLike& row, size_t pos,
                                                    bool optional) {
  ICEBERG_ASSIGN_OR_RAISE(auto field, GetCheckedField(row, pos, optional));
  if (std::holds_alternative<std::monostate>(field)) return std::shared_ptr<StructLike>{};
  if (!std::holds_alternative<std::shared_ptr<StructLike>>(field)) {
    return InvalidSchema("Encountered non-struct at position {}", pos);
  }
  auto parent = std::get<std::shared_ptr<StructLike>>(std::move(field));
  if (!parent && !optional) {
    return InvalidArgument("Required field at position {} is null", pos);
  }
  return parent;
}

}  // namespace

StructLikeAccessor::StructLikeAccessor(std::shared_ptr<Type> type,
                                       std::span<const size_t> position_path,
                                       std::vector<bool> is_optional)
    : type_(std::move(type)),
      position_path_(position_path.begin(), position_path.end()),
      is_optional_(std::move(is_optional)) {
  if (position_path_.size() != is_optional_.size()) {
    accessor_ = [](const StructLike&) -> Result<Scalar> {
      return InvalidArgument("Optionality count does not match position path");
    };
    return;
  }
  if (position_path_.size() == 1) {
    accessor_ = [pos = position_path_[0], optional = static_cast<bool>(is_optional_[0])](
                    const StructLike& struct_like) -> Result<Scalar> {
      return GetCheckedField(struct_like, pos, optional);
    };
  } else if (position_path_.size() == 2) {
    accessor_ = [pos0 = position_path_[0], pos1 = position_path_[1],
                 optional = static_cast<bool>(is_optional_[0]),
                 leaf_optional = static_cast<bool>(is_optional_[1])](
                    const StructLike& struct_like) -> Result<Scalar> {
      ICEBERG_ASSIGN_OR_RAISE(auto nested, GetParentStruct(struct_like, pos0, optional));
      if (!nested) return Scalar{std::monostate{}};
      return GetCheckedField(*nested, pos1, leaf_optional);
    };
  } else if (!position_path_.empty()) {
    accessor_ = [this](const StructLike& struct_like) -> Result<Scalar> {
      std::vector<std::shared_ptr<StructLike>> backups;
      backups.reserve(position_path_.size() - 1);
      const StructLike* current_struct_like = &struct_like;
      for (size_t i = 0; i < position_path_.size() - 1; ++i) {
        ICEBERG_ASSIGN_OR_RAISE(
            auto parent,
            GetParentStruct(*current_struct_like, position_path_[i], is_optional_[i]));
        if (!parent) return Scalar{std::monostate{}};
        backups.push_back(std::move(parent));
        current_struct_like = backups.back().get();
      }
      return GetCheckedField(*current_struct_like, position_path_.back(),
                             is_optional_.back());
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
    case TypeId::kTimestampNs:
      return Literal::TimestampNs(std::get<int64_t>(scalar));
    case TypeId::kTimestampTzNs:
      return Literal::TimestampTzNs(std::get<int64_t>(scalar));
    case TypeId::kFixed: {
      const auto& fixed_data = std::get<std::string_view>(scalar);
      return Literal::Fixed(std::vector<uint8_t>(fixed_data.cbegin(), fixed_data.cend()));
    }
    case TypeId::kUuid: {
      const auto& uuid_data = std::get<std::string_view>(scalar);
      ICEBERG_ASSIGN_OR_RAISE(
          auto uuid,
          Uuid::FromBytes(std::span<const uint8_t>(
              reinterpret_cast<const uint8_t*>(uuid_data.data()), uuid_data.size())));
      return Literal::UUID(uuid);
    }
    default:
      return NotSupported("Cannot convert scalar to literal of type {}",
                          type_->ToString());
  }

  std::unreachable();
}

}  // namespace iceberg
