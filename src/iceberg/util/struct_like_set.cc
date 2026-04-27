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

#include "iceberg/util/struct_like_set.h"

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <utility>

#include "iceberg/result.h"
#include "iceberg/type.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

/// \brief Helper for std::visit with multiple lambdas.
template <class... Ts>
struct Overloaded : Ts... {
  using Ts::operator()...;
};
template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

/// \brief A StructLike that owns its field values in a vector of Scalars.
class ArenaStructLike : public StructLike {
 public:
  explicit ArenaStructLike(std::pmr::vector<Scalar> fields)
      : fields_(std::move(fields)) {}

  Result<Scalar> GetField(size_t pos) const override {
    ICEBERG_PRECHECK(pos < fields_.size(), "field position {} out of range [0, {})", pos,
                     fields_.size());
    return fields_[pos];
  }

  size_t num_fields() const override { return fields_.size(); }

 private:
  std::pmr::vector<Scalar> fields_;
};

/// \brief An ArrayLike that owns its element values in a vector of Scalars.
class ArenaArrayLike : public ArrayLike {
 public:
  explicit ArenaArrayLike(std::pmr::vector<Scalar> elements)
      : elements_(std::move(elements)) {}

  Result<Scalar> GetElement(size_t pos) const override {
    ICEBERG_PRECHECK(pos < elements_.size(), "element position {} out of range [0, {})",
                     pos, elements_.size());
    return elements_[pos];
  }

  size_t size() const override { return elements_.size(); }

 private:
  std::pmr::vector<Scalar> elements_;
};

/// \brief A MapLike that owns its key/value data in vectors of Scalars.
class ArenaMapLike : public MapLike {
 public:
  ArenaMapLike(std::pmr::vector<Scalar> keys, std::pmr::vector<Scalar> values)
      : keys_(std::move(keys)), values_(std::move(values)) {}

  Result<Scalar> GetKey(size_t pos) const override {
    ICEBERG_PRECHECK(pos < keys_.size(), "key position {} out of range [0, {})", pos,
                     keys_.size());
    return keys_[pos];
  }

  Result<Scalar> GetValue(size_t pos) const override {
    ICEBERG_PRECHECK(pos < values_.size(), "value position {} out of range [0, {})", pos,
                     values_.size());
    return values_[pos];
  }

  size_t size() const override { return keys_.size(); }

 private:
  std::pmr::vector<Scalar> keys_;
  std::pmr::vector<Scalar> values_;
};

constexpr uint32_t kCanonicalFloatNaNBits = 0x7fc00000U;
constexpr uint64_t kCanonicalDoubleNaNBits = 0x7ff8000000000000ULL;

uint32_t CanonicalFloatBits(float value) {
  if (std::isnan(value)) {
    return kCanonicalFloatNaNBits;
  }
  return std::bit_cast<uint32_t>(value);
}

uint64_t CanonicalDoubleBits(double value) {
  if (std::isnan(value)) {
    return kCanonicalDoubleNaNBits;
  }
  return std::bit_cast<uint64_t>(value);
}

Result<size_t> HashScalar(const Scalar& scalar);

/// \brief Hash a string_view using Java's String.hashCode() algorithm
size_t HashStringView(std::string_view sv) {
  size_t result = 177;
  for (unsigned char ch : sv) {
    result = 31 * result + ch;
  }
  return result;
}

/// \brief Hash a StructLike using Java's StructLikeHash algorithm
Result<size_t> HashStructLike(const StructLike& s) {
  size_t result = 97;
  size_t len = s.num_fields();
  result = 41 * result + len;
  for (size_t i = 0; i < len; ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto field_hash, s.GetField(i).and_then(HashScalar));
    result = 41 * result + field_hash;
  }
  return result;
}

/// \brief Hash an ArrayLike using Java's ListHash algorithm
Result<size_t> HashArrayLike(const ArrayLike& a) {
  size_t result = 17;
  size_t len = a.size();
  result = 37 * result + len;
  for (size_t i = 0; i < len; ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto elem, a.GetElement(i).and_then(HashScalar));
    result = 37 * result + elem;
  }
  return result;
}

Result<size_t> HashMapLike(const MapLike& m) {
  size_t result = 17;
  size_t len = m.size();
  result = 37 * result + len;
  for (size_t i = 0; i < len; ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto key_hash, m.GetKey(i).and_then(HashScalar));
    ICEBERG_ASSIGN_OR_RAISE(auto value_hash, m.GetValue(i).and_then(HashScalar));
    result = 37 * result + key_hash;
    result = 37 * result + value_hash;
  }
  return result;
}

size_t HashStructLikeUnchecked(const StructLike& s) noexcept {
  auto result = HashStructLike(s);
  ICEBERG_DCHECK(result.has_value(), "Validated StructLike hash must not fail");
  return result.value_or(0);
}

Result<size_t> HashScalar(const Scalar& scalar) {
  return std::visit(
      Overloaded{
          [](std::monostate) -> Result<size_t> { return 0; },
          [](bool v) -> Result<size_t> { return std::hash<bool>{}(v); },
          [](int32_t v) -> Result<size_t> { return std::hash<int32_t>{}(v); },
          [](int64_t v) -> Result<size_t> { return std::hash<int64_t>{}(v); },
          [](float v) -> Result<size_t> {
            return static_cast<size_t>(CanonicalFloatBits(v));
          },
          [](double v) -> Result<size_t> {
            uint64_t bits = CanonicalDoubleBits(v);
            return static_cast<size_t>(bits ^ (bits >> 32));
          },
          [](std::string_view v) -> Result<size_t> { return HashStringView(v); },
          [](const Decimal& v) -> Result<size_t> {
            return std::hash<uint64_t>{}(v.low()) ^ (std::hash<int64_t>{}(v.high()) << 1);
          },
          [](const std::shared_ptr<StructLike>& v) -> Result<size_t> {
            return v ? HashStructLike(*v) : Result<size_t>{0};
          },
          [](const std::shared_ptr<ArrayLike>& v) -> Result<size_t> {
            return v ? HashArrayLike(*v) : Result<size_t>{0};
          },
          [](const std::shared_ptr<MapLike>& v) -> Result<size_t> {
            return v ? HashMapLike(*v) : Result<size_t>{0};
          },
      },
      scalar);
}

Result<bool> ScalarEqual(const Scalar& lhs, const Scalar& rhs);

std::string_view ScalarTypeName(const Scalar& scalar) {
  return std::visit(
      Overloaded{
          [](std::monostate) -> std::string_view { return "null"; },
          [](bool) -> std::string_view { return "boolean"; },
          [](int32_t) -> std::string_view { return "int32"; },
          [](int64_t) -> std::string_view { return "int64"; },
          [](float) -> std::string_view { return "float"; },
          [](double) -> std::string_view { return "double"; },
          [](std::string_view) -> std::string_view { return "string"; },
          [](const Decimal&) -> std::string_view { return "decimal"; },
          [](const std::shared_ptr<StructLike>&) -> std::string_view { return "struct"; },
          [](const std::shared_ptr<ArrayLike>&) -> std::string_view { return "list"; },
          [](const std::shared_ptr<MapLike>&) -> std::string_view { return "map"; },
      },
      scalar);
}

Status ValidateScalarAgainstType(const Scalar& scalar, const Type& type);

Status ValidateStructLikeAgainstType(const StructLike& row, const StructType& type) {
  ICEBERG_PRECHECK(row.num_fields() == type.fields().size(),
                   "StructLike row has {} fields but expected {}", row.num_fields(),
                   type.fields().size());
  for (size_t i = 0; i < row.num_fields(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto scalar, row.GetField(i));
    ICEBERG_RETURN_UNEXPECTED(
        ValidateScalarAgainstType(scalar, *type.fields()[i].type()));
  }
  return {};
}

Status ValidateArrayLikeAgainstType(const ArrayLike& array, const ListType& type) {
  for (size_t i = 0; i < array.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto scalar, array.GetElement(i));
    ICEBERG_RETURN_UNEXPECTED(ValidateScalarAgainstType(scalar, *type.element().type()));
  }
  return {};
}

Status ValidateMapLikeAgainstType(const MapLike& map, const MapType& type) {
  for (size_t i = 0; i < map.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto key, map.GetKey(i));
    ICEBERG_ASSIGN_OR_RAISE(auto value, map.GetValue(i));
    ICEBERG_RETURN_UNEXPECTED(ValidateScalarAgainstType(key, *type.key().type()));
    ICEBERG_RETURN_UNEXPECTED(ValidateScalarAgainstType(value, *type.value().type()));
  }
  return {};
}

Status ValidateScalarAgainstType(const Scalar& scalar, const Type& type) {
  if (std::holds_alternative<std::monostate>(scalar)) {
    return {};
  }

  switch (type.type_id()) {
    case TypeId::kBoolean:
      ICEBERG_PRECHECK(std::holds_alternative<bool>(scalar),
                       "Expected boolean but got {}", ScalarTypeName(scalar));
      return {};
    case TypeId::kInt:
    case TypeId::kDate:
      ICEBERG_PRECHECK(std::holds_alternative<int32_t>(scalar), "Expected {} but got {}",
                       type.ToString(), ScalarTypeName(scalar));
      return {};
    case TypeId::kLong:
    case TypeId::kTime:
    case TypeId::kTimestamp:
    case TypeId::kTimestampTz:
      ICEBERG_PRECHECK(std::holds_alternative<int64_t>(scalar), "Expected {} but got {}",
                       type.ToString(), ScalarTypeName(scalar));
      return {};
    case TypeId::kFloat:
      ICEBERG_PRECHECK(std::holds_alternative<float>(scalar), "Expected float but got {}",
                       ScalarTypeName(scalar));
      return {};
    case TypeId::kDouble:
      ICEBERG_PRECHECK(std::holds_alternative<double>(scalar),
                       "Expected double but got {}", ScalarTypeName(scalar));
      return {};
    case TypeId::kDecimal:
      ICEBERG_PRECHECK(std::holds_alternative<Decimal>(scalar),
                       "Expected decimal but got {}", ScalarTypeName(scalar));
      return {};
    case TypeId::kString:
    case TypeId::kBinary:
      ICEBERG_PRECHECK(std::holds_alternative<std::string_view>(scalar),
                       "Expected {} but got {}", type.ToString(), ScalarTypeName(scalar));
      return {};
    case TypeId::kFixed: {
      ICEBERG_PRECHECK(std::holds_alternative<std::string_view>(scalar),
                       "Expected fixed but got {}", ScalarTypeName(scalar));
      const auto& fixed = static_cast<const FixedType&>(type);
      auto value = std::get<std::string_view>(scalar);
      ICEBERG_PRECHECK(value.size() == fixed.length(),
                       "Expected fixed({}) but got byte length {}", fixed.length(),
                       value.size());
      return {};
    }
    case TypeId::kUuid: {
      ICEBERG_PRECHECK(std::holds_alternative<std::string_view>(scalar),
                       "Expected uuid but got {}", ScalarTypeName(scalar));
      auto value = std::get<std::string_view>(scalar);
      ICEBERG_PRECHECK(value.size() == 16, "Expected uuid byte length 16 but got {}",
                       value.size());
      return {};
    }
    case TypeId::kStruct: {
      ICEBERG_PRECHECK(std::holds_alternative<std::shared_ptr<StructLike>>(scalar),
                       "Expected struct but got {}", ScalarTypeName(scalar));
      const auto& row = std::get<std::shared_ptr<StructLike>>(scalar);
      ICEBERG_PRECHECK(row, "Expected struct but got null");
      return ValidateStructLikeAgainstType(
          *row, internal::checked_cast<const StructType&>(type));
    }
    case TypeId::kList: {
      ICEBERG_PRECHECK(std::holds_alternative<std::shared_ptr<ArrayLike>>(scalar),
                       "Expected list but got {}", ScalarTypeName(scalar));
      const auto& array = std::get<std::shared_ptr<ArrayLike>>(scalar);
      ICEBERG_PRECHECK(array, "Expected ArrayLike but got null");
      return ValidateArrayLikeAgainstType(*array,
                                          internal::checked_cast<const ListType&>(type));
    }
    case TypeId::kMap: {
      ICEBERG_PRECHECK(std::holds_alternative<std::shared_ptr<MapLike>>(scalar),
                       "Expected map but got {}", ScalarTypeName(scalar));
      const auto& map = std::get<std::shared_ptr<MapLike>>(scalar);
      ICEBERG_PRECHECK(map, "Expected MapLike but got null");
      return ValidateMapLikeAgainstType(*map,
                                        internal::checked_cast<const MapType&>(type));
    }
  }

  std::unreachable();
}

Status ValidateRowAgainstTypes(const StructLike& row,
                               std::span<const std::shared_ptr<Type>> field_types) {
  if (row.num_fields() != field_types.size()) {
    return InvalidArgument("StructLike row has {} fields but expected {}",
                           row.num_fields(), field_types.size());
  }
  for (size_t i = 0; i < field_types.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto scalar, row.GetField(i));
    ICEBERG_RETURN_UNEXPECTED(ValidateScalarAgainstType(scalar, *field_types[i]));
  }
  return {};
}

Result<bool> StructLikeEqual(const StructLike& lhs, const StructLike& rhs) {
  if (lhs.num_fields() != rhs.num_fields()) {
    return false;
  }
  for (size_t i = 0; i < lhs.num_fields(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto fa, lhs.GetField(i));
    ICEBERG_ASSIGN_OR_RAISE(auto fb, rhs.GetField(i));
    ICEBERG_ASSIGN_OR_RAISE(auto equal, ScalarEqual(fa, fb));
    if (!equal) {
      return false;
    }
  }
  return true;
}

Result<bool> ArrayLikeEqual(const ArrayLike& lhs, const ArrayLike& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto ea, lhs.GetElement(i));
    ICEBERG_ASSIGN_OR_RAISE(auto eb, rhs.GetElement(i));
    ICEBERG_ASSIGN_OR_RAISE(auto equal, ScalarEqual(ea, eb));
    if (!equal) {
      return false;
    }
  }
  return true;
}

Result<bool> MapLikeEqual(const MapLike& lhs, const MapLike& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }
  for (size_t i = 0; i < lhs.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto ka, lhs.GetKey(i));
    ICEBERG_ASSIGN_OR_RAISE(auto kb, rhs.GetKey(i));
    ICEBERG_ASSIGN_OR_RAISE(auto va, lhs.GetValue(i));
    ICEBERG_ASSIGN_OR_RAISE(auto vb, rhs.GetValue(i));
    ICEBERG_ASSIGN_OR_RAISE(auto keys_equal, ScalarEqual(ka, kb));
    if (!keys_equal) {
      return false;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto values_equal, ScalarEqual(va, vb));
    if (!values_equal) {
      return false;
    }
  }
  return true;
}

bool StructLikeEqualUnchecked(const StructLike& lhs, const StructLike& rhs) noexcept {
  auto result = StructLikeEqual(lhs, rhs);
  ICEBERG_DCHECK(result.has_value(), "Validated StructLike equality must not fail");
  return result.value_or(false);
}

Result<bool> ScalarEqual(const Scalar& lhs, const Scalar& rhs) {
  if (lhs.index() != rhs.index()) {
    return false;
  }

  return std::visit(
      Overloaded{
          [](std::monostate, const Scalar&) -> Result<bool> { return true; },
          [](bool v, const Scalar& other) -> Result<bool> {
            return v == std::get<bool>(other);
          },
          [](int32_t v, const Scalar& other) -> Result<bool> {
            return v == std::get<int32_t>(other);
          },
          [](int64_t v, const Scalar& other) -> Result<bool> {
            return v == std::get<int64_t>(other);
          },
          [](float v, const Scalar& other) -> Result<bool> {
            return CanonicalFloatBits(v) == CanonicalFloatBits(std::get<float>(other));
          },
          [](double v, const Scalar& other) -> Result<bool> {
            return CanonicalDoubleBits(v) == CanonicalDoubleBits(std::get<double>(other));
          },
          [](std::string_view v, const Scalar& other) -> Result<bool> {
            return v == std::get<std::string_view>(other);
          },
          [](const Decimal& v, const Scalar& other) -> Result<bool> {
            return v == std::get<Decimal>(other);
          },
          [](const std::shared_ptr<StructLike>& l, const Scalar& other) -> Result<bool> {
            const auto& r = std::get<std::shared_ptr<StructLike>>(other);
            if (!l && !r) return true;
            if (!l || !r) return false;
            return StructLikeEqual(*l, *r);
          },
          [](const std::shared_ptr<ArrayLike>& l, const Scalar& other) -> Result<bool> {
            const auto& r = std::get<std::shared_ptr<ArrayLike>>(other);
            if (!l && !r) return true;
            if (!l || !r) return false;
            return ArrayLikeEqual(*l, *r);
          },
          [](const std::shared_ptr<MapLike>& l, const Scalar& other) -> Result<bool> {
            const auto& r = std::get<std::shared_ptr<MapLike>>(other);
            if (!l && !r) return true;
            if (!l || !r) return false;
            return MapLikeEqual(*l, *r);
          },
      },
      lhs, rhs);
}

}  // namespace

template <bool kValidate>
StructLikeSet<kValidate>::StructLikeSet(const StructType& type, size_t arena_initial_size)
    : arena_(arena_initial_size) {
  field_types_.reserve(type.fields().size());
  for (const auto& field : type.fields()) {
    field_types_.push_back(field.type());
  }
}

template <bool kValidate>
StructLikeSet<kValidate>::~StructLikeSet() = default;

template <bool kValidate>
std::string_view StructLikeSet<kValidate>::CopyToArena(std::string_view src) const {
  if (src.empty()) {
    return {};
  }
  auto buf = static_cast<char*>(arena_.allocate(src.size(), 1));
  std::memcpy(buf, src.data(), src.size());
  return {buf, src.size()};
}

template <bool kValidate>
Result<Scalar> StructLikeSet<kValidate>::DeepCopyScalar(const Scalar& scalar) const {
  return std::visit(
      Overloaded{
          [](std::monostate v) -> Result<Scalar> { return Scalar{v}; },
          [](bool v) -> Result<Scalar> { return Scalar{v}; },
          [](int32_t v) -> Result<Scalar> { return Scalar{v}; },
          [](int64_t v) -> Result<Scalar> { return Scalar{v}; },
          [](float v) -> Result<Scalar> { return Scalar{v}; },
          [](double v) -> Result<Scalar> { return Scalar{v}; },
          [this](std::string_view sv) -> Result<Scalar> {
            return Scalar{CopyToArena(sv)};
          },
          [](const Decimal& v) -> Result<Scalar> { return Scalar{v}; },
          [this](const std::shared_ptr<StructLike>& s) -> Result<Scalar> {
            ICEBERG_PRECHECK(s, "StructLike scalar must not be null");
            std::pmr::vector<Scalar> fields(&arena_);
            fields.resize(s->num_fields());
            for (size_t i = 0; i < s->num_fields(); ++i) {
              ICEBERG_ASSIGN_OR_RAISE(auto field, s->GetField(i));
              ICEBERG_ASSIGN_OR_RAISE(fields[i], DeepCopyScalar(field));
            }
            return Scalar{std::make_shared<ArenaStructLike>(std::move(fields))};
          },
          [this](const std::shared_ptr<ArrayLike>& a) -> Result<Scalar> {
            ICEBERG_PRECHECK(a, "ArrayLike scalar must not be null");
            std::pmr::vector<Scalar> elements(&arena_);
            elements.resize(a->size());
            for (size_t i = 0; i < a->size(); ++i) {
              ICEBERG_ASSIGN_OR_RAISE(auto element, a->GetElement(i));
              ICEBERG_ASSIGN_OR_RAISE(elements[i], DeepCopyScalar(element));
            }
            return Scalar{std::make_shared<ArenaArrayLike>(std::move(elements))};
          },
          [this](const std::shared_ptr<MapLike>& m) -> Result<Scalar> {
            ICEBERG_PRECHECK(m, "MapLike scalar must not be null");
            std::pmr::vector<Scalar> keys(&arena_);
            std::pmr::vector<Scalar> values(&arena_);
            keys.resize(m->size());
            values.resize(m->size());
            for (size_t i = 0; i < m->size(); ++i) {
              ICEBERG_ASSIGN_OR_RAISE(auto key, m->GetKey(i));
              ICEBERG_ASSIGN_OR_RAISE(auto value, m->GetValue(i));
              ICEBERG_ASSIGN_OR_RAISE(keys[i], DeepCopyScalar(key));
              ICEBERG_ASSIGN_OR_RAISE(values[i], DeepCopyScalar(value));
            }
            return Scalar{
                std::make_shared<ArenaMapLike>(std::move(keys), std::move(values))};
          },
      },
      scalar);
}

template <bool kValidate>
Result<std::unique_ptr<StructLike>> StructLikeSet<kValidate>::MakeArenaRow(
    const StructLike& row) const {
  std::pmr::vector<Scalar> fields(&arena_);
  fields.resize(field_types_.size());
  for (size_t i = 0; i < field_types_.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto scalar, row.GetField(i));
    ICEBERG_ASSIGN_OR_RAISE(fields[i], DeepCopyScalar(scalar));
  }
  return std::make_unique<ArenaStructLike>(std::move(fields));
}

template <bool kValidate>
Status StructLikeSet<kValidate>::Insert(const StructLike& row) {
  if constexpr (kValidate) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRowAgainstTypes(row, field_types_));
  }
  if (set_.contains(row)) {
    return {};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto arena_row, MakeArenaRow(row));
  set_.insert(std::move(arena_row));
  return {};
}

template <bool kValidate>
Result<bool> StructLikeSet<kValidate>::Contains(const StructLike& row) const {
  if constexpr (kValidate) {
    ICEBERG_RETURN_UNEXPECTED(ValidateRowAgainstTypes(row, field_types_));
  }
  return set_.find(row) != set_.end();
}

template <bool kValidate>
bool StructLikeSet<kValidate>::IsEmpty() const {
  return set_.empty();
}

template <bool kValidate>
size_t StructLikeSet<kValidate>::Size() const {
  return set_.size();
}

// --- KeyHash ---

template <bool kValidate>
size_t StructLikeSet<kValidate>::KeyHash::operator()(
    const std::unique_ptr<StructLike>& p) const noexcept {
  return HashStructLikeUnchecked(*p);
}

template <bool kValidate>
size_t StructLikeSet<kValidate>::KeyHash::operator()(const StructLike& s) const noexcept {
  return HashStructLikeUnchecked(s);
}

// --- KeyEqual ---

template <bool kValidate>
bool StructLikeSet<kValidate>::KeyEqual::operator()(
    const std::unique_ptr<StructLike>& lhs,
    const std::unique_ptr<StructLike>& rhs) const noexcept {
  return StructLikeEqualUnchecked(*lhs, *rhs);
}

template <bool kValidate>
bool StructLikeSet<kValidate>::KeyEqual::operator()(
    const StructLike& lhs, const std::unique_ptr<StructLike>& rhs) const noexcept {
  return StructLikeEqualUnchecked(lhs, *rhs);
}

template <bool kValidate>
bool StructLikeSet<kValidate>::KeyEqual::operator()(
    const std::unique_ptr<StructLike>& lhs, const StructLike& rhs) const noexcept {
  return StructLikeEqualUnchecked(*lhs, rhs);
}

template class ICEBERG_TEMPLATE_INSTANTIATION_EXPORT StructLikeSet<true>;
template class ICEBERG_TEMPLATE_INSTANTIATION_EXPORT StructLikeSet<false>;

}  // namespace iceberg
