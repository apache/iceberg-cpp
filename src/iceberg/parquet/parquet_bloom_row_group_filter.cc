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

#include <algorithm>
#include <array>
#include <cmath>
#include <limits>
#include <string_view>
#include <utility>
#include <vector>

#include <arrow/io/interfaces.h>
#include <parquet/bloom_filter.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_bloom_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/util/checked_cast.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {
namespace {

// True means a matching row may exist; false means the group can be skipped.
constexpr bool kRowsMightMatch = true;
constexpr bool kRowsCannotMatch = false;

// Arrow 25 rejects non-power-of-two bitsets even if they contain whole 32-byte
// blocks. It reports the same exception for malformed sizes. Inspect the compact
// header only on that error, after Arrow has validated the header and read the bitset.
Result<bool> HasWholeBloomBlocks(::arrow::io::RandomAccessFile& input, int64_t offset) {
  std::array<uint8_t, 2> prefix{};
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto count,
                                 input.ReadAt(offset, prefix.size(), prefix.data()));
  // parquet-go writes numBytes first: field 1, compact I32 (0x15). A positive
  // zigzag varint encodes size << 1, so its low six bits identify 32-byte alignment.
  return count == 2 && prefix[0] == 0x15 && (prefix[1] & 0x3f) == 0;
}

bool FindBytes(const ::parquet::BloomFilter& bloom, std::span<const uint8_t> bytes) {
  ::parquet::ByteArray value{static_cast<uint32_t>(bytes.size()), bytes.data()};
  return bloom.FindHash(bloom.Hash(value));
}

// Hash the file's physical representation, not Iceberg's single-value encoding.
// Java's BloomEvalVisitor.shouldRead uses the same physical-type dispatch.
bool MightContain(const ::parquet::ColumnDescriptor& column, const Literal& value,
                  const ::parquet::BloomFilter& bloom) {
  if (value.IsNull() || value.IsAboveMax() || value.IsBelowMin() || value.IsNaN()) {
    // Bloom hashes floating-point bits, whereas equality treats all NaN payloads
    // alike. Neither a NaN predicate nor a NaN literal can safely exclude rows.
    return kRowsMightMatch;
  }
  auto type = value.type()->type_id();
  if (type == TypeId::kDecimal &&
      (!column.logical_type()->is_decimal() ||
       internal::checked_cast<const DecimalType&>(*value.type()).scale() !=
           column.type_scale())) {
    // A pre-bound literal can compare equal at a different scale, but its
    // unscaled integer would hash to a different physical value.
    return kRowsMightMatch;
  }
  switch (column.physical_type()) {
    case ::parquet::Type::INT32: {
      int128_t number;
      switch (type) {
        case TypeId::kInt:
        case TypeId::kDate:
          number = std::get<int32_t>(value.value());
          break;
        case TypeId::kLong:
          number = std::get<int64_t>(value.value());
          break;
        case TypeId::kDecimal:
          number = std::get<Decimal>(value.value()).value();
          break;
        default:
          return kRowsMightMatch;
      }
      if (number < std::numeric_limits<int32_t>::min() ||
          number > std::numeric_limits<int32_t>::max()) {
        return kRowsCannotMatch;
      }
      return bloom.FindHash(bloom.Hash(static_cast<int32_t>(number)));
    }
    case ::parquet::Type::INT64: {
      int128_t number;
      switch (type) {
        case TypeId::kLong:
        case TypeId::kTime:
        case TypeId::kTimestamp:
        case TypeId::kTimestampTz:
        case TypeId::kTimestampNs:
        case TypeId::kTimestampTzNs:
          number = std::get<int64_t>(value.value());
          break;
        case TypeId::kDecimal:
          number = std::get<Decimal>(value.value()).value();
          break;
        default:
          return kRowsMightMatch;
      }
      if (number < std::numeric_limits<int64_t>::min() ||
          number > std::numeric_limits<int64_t>::max()) {
        return kRowsCannotMatch;
      }
      return bloom.FindHash(bloom.Hash(static_cast<int64_t>(number)));
    }
    case ::parquet::Type::FLOAT: {
      float number;
      if (type == TypeId::kFloat) {
        number = std::get<float>(value.value());
      } else if (type == TypeId::kDouble) {
        auto promoted = std::get<double>(value.value());
        if (std::isfinite(promoted) &&
            std::abs(promoted) > std::numeric_limits<float>::max()) {
          return kRowsCannotMatch;
        }
        number = static_cast<float>(promoted);
      } else {
        return kRowsMightMatch;
      }
      return bloom.FindHash(bloom.Hash(number));
    }
    case ::parquet::Type::DOUBLE:
      return type != TypeId::kDouble ||
             bloom.FindHash(bloom.Hash(std::get<double>(value.value())));
    case ::parquet::Type::BYTE_ARRAY:
    case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
      switch (type) {
        case TypeId::kString:
          return bloom.FindHash(
              bloom.Hash(std::string_view(std::get<std::string>(value.value()))));
        case TypeId::kBinary:
        case TypeId::kFixed:
          return FindBytes(bloom, std::get<std::vector<uint8_t>>(value.value()));
        case TypeId::kUuid:
          return FindBytes(bloom, std::get<Uuid>(value.value()).bytes());
        case TypeId::kDecimal: {
          // Iceberg's Parquet decimal representation is precision-sized,
          // two's-complement big endian, including sign extension. Use the
          // file's precision, since the table decimal may have been promoted.
          const auto& decimal = std::get<Decimal>(value.value());
          if (!decimal.FitsInPrecision(column.type_precision())) {
            return kRowsCannotMatch;
          }
          auto bytes = decimal.ToBigEndian();
          if (column.physical_type() == ::parquet::Type::BYTE_ARRAY) {
            // Unlike FIXED, binary metadata does not specify the stored width.
            // Include Java's precision-sized encoding and all other supported
            // sign-extended representations to avoid false negatives.
            for (size_t width = bytes.size(); width <= Decimal::kByteWidth; ++width) {
              if (FindBytes(bloom, bytes)) {
                return kRowsMightMatch;
              }
              bytes.insert(bytes.begin(), decimal.value() < 0 ? 0xff : 0x00);
            }
            return kRowsCannotMatch;
          }
          int width = column.type_length();
          if (std::cmp_less(width, bytes.size())) {
            return kRowsMightMatch;
          }
          bytes.insert(bytes.begin(), width - bytes.size(),
                       decimal.value() < 0 ? 0xff : 0x00);
          return FindBytes(bloom, bytes);
        }
        default:
          return kRowsMightMatch;
      }
    default:
      // Boolean and legacy INT96 have no Iceberg Bloom conversion here.
      return kRowsMightMatch;
  }
}

// After NOT has been pushed down, an IN containing NaN cannot exclude rows.
// Fold it once in the Bloom expression before visiting individual row groups.
class FoldNaNPredicates : public RewriteNot {
 public:
  using RewriteNot::Predicate;
  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<BoundPredicate>& pred) override {
    if (pred->op() == Expression::Operation::kIn &&
        pred->kind() == BoundPredicate::Kind::kSet) {
      const auto& values =
          internal::checked_cast<const BoundSetPredicate&>(*pred).literal_set();
      if (std::ranges::any_of(values, &Literal::IsNaN)) {
        return True::Instance();
      }
    }
    return pred;
  }
};

class BloomVisitor : public BoundVisitor<bool> {
 public:
  BloomVisitor(const ::parquet::arrow::SchemaManifest& manifest,
               const ::parquet::RowGroupMetaData& row_group,
               ::parquet::RowGroupBloomFilterReader* bloom_reader,
               ::arrow::io::RandomAccessFile* input,
               const std::unordered_map<int32_t, int>& columns)
      : manifest_(manifest),
        row_group_(row_group),
        bloom_reader_(bloom_reader),
        input_(input),
        columns_(columns) {}

  Result<bool> AlwaysTrue() override { return kRowsMightMatch; }
  Result<bool> AlwaysFalse() override { return kRowsCannotMatch; }
  // Make rewrites NOT. Negating an inclusive result would be unsafe.
  Result<bool> Not(bool) override { return kRowsMightMatch; }
  Result<bool> And(bool left, bool right) override { return left && right; }
  Result<bool> Or(bool left, bool right) override { return left || right; }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    if (value.IsNaN()) {
      return kRowsMightMatch;
    }
    auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    ICEBERG_ASSIGN_OR_RAISE(auto bloom, GetBloomFilter(ref));
    return !bloom || MightContain(*manifest_.descr->Column(columns_.at(ref->field_id())),
                                  value, *bloom);
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& values) override {
    auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    ICEBERG_ASSIGN_OR_RAISE(auto bloom, GetBloomFilter(ref));
    if (!bloom) {
      return kRowsMightMatch;
    }
    auto column = manifest_.descr->Column(columns_.at(ref->field_id()));
    return std::ranges::any_of(
        values, [&](const auto& value) { return MightContain(*column, value, *bloom); });
  }
  Result<bool> IsNull(const std::shared_ptr<Bound>&) override { return kRowsMightMatch; }
  Result<bool> NotNull(const std::shared_ptr<Bound>&) override { return kRowsMightMatch; }
  Result<bool> IsNaN(const std::shared_ptr<Bound>&) override { return kRowsMightMatch; }
  Result<bool> NotNaN(const std::shared_ptr<Bound>&) override { return kRowsMightMatch; }
  Result<bool> Lt(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> LtEq(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> Gt(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> GtEq(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> NotEq(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> StartsWith(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> NotStartsWith(const std::shared_ptr<Bound>&, const Literal&) override {
    return kRowsMightMatch;
  }
  Result<bool> NotIn(const std::shared_ptr<Bound>&,
                     const BoundSetPredicate::LiteralSet&) override {
    return kRowsMightMatch;
  }

 private:
  Result<const ::parquet::BloomFilter*> GetBloomFilter(
      const std::shared_ptr<BoundReference>& ref) {
    if (!ref || !ref->type()->is_primitive() ||
        MetadataColumns::IsMetadataColumn(ref->field_id())) {
      return nullptr;
    }
    auto index = columns_.find(ref->field_id());
    if (index == columns_.end()) {
      // Missing fields may have initial defaults; never infer all nulls here.
      return nullptr;
    }
    auto field = manifest_.column_index_to_field.find(index->second);
    if (field == manifest_.column_index_to_field.end() ||
        !ValidateParquetTypeCompatibility(*ref->type(), *field->second) ||
        manifest_.descr->Column(index->second)->max_repetition_level() > 0) {
      return nullptr;
    }
    auto physical_type = manifest_.descr->Column(index->second)->physical_type();
    if (physical_type == ::parquet::Type::BOOLEAN ||
        physical_type == ::parquet::Type::INT96) {
      return nullptr;
    }
    auto column = row_group_.ColumnChunk(index->second);
    auto offset = column->bloom_filter_offset();
    if (!offset || *offset <= 0) {
      return nullptr;
    }
    auto [entry, inserted] = cache_.try_emplace(ref->field_id());
    if (!inserted) {
      return entry->second.get();
    }
    if (bloom_reader_) {
      try {
        entry->second = bloom_reader_->GetColumnBloomFilter(index->second);
      } catch (const ::parquet::ParquetException& e) {
        if (input_ && std::string_view(e.what()) == "Given length of bitset is illegal" &&
            !column->crypto_metadata()) {
          ICEBERG_ASSIGN_OR_RAISE(auto whole_blocks,
                                  HasWholeBloomBlocks(*input_, *offset));
          if (whole_blocks) {
            return nullptr;
          }
        }
        throw;
      }
    }
    if (!entry->second) {
      return Invalid("Missing Parquet Bloom filter for field {}", ref->field_id());
    }
    return entry->second.get();
  }

  const ::parquet::arrow::SchemaManifest& manifest_;
  const ::parquet::RowGroupMetaData& row_group_;
  ::parquet::RowGroupBloomFilterReader* bloom_reader_;
  ::arrow::io::RandomAccessFile* input_;
  const std::unordered_map<int32_t, int>& columns_;
  std::unordered_map<int32_t, std::unique_ptr<::parquet::BloomFilter>> cache_;
};

}  // namespace

Result<std::unique_ptr<ParquetBloomRowGroupFilter>> ParquetBloomRowGroupFilter::Make(
    const std::shared_ptr<Expression>& filter,
    const ::parquet::SchemaDescriptor& file_schema) {
  auto result =
      std::unique_ptr<ParquetBloomRowGroupFilter>(new ParquetBloomRowGroupFilter());
  for (int i = 0; i < file_schema.num_columns(); ++i) {
    auto id = file_schema.Column(i)->schema_node()->field_id();
    if (id >= 0) {
      result->field_id_to_column_idx_.emplace(id, i);
    }
  }
  result->bound_ = True::Instance();
  if (filter) {
    ICEBERG_ASSIGN_OR_RAISE(auto rewritten, RewriteNot::Visit(filter));
    FoldNaNPredicates folder;
    ICEBERG_ASSIGN_OR_RAISE(result->bound_,
                            Visit<std::shared_ptr<Expression>>(rewritten, folder));
  }
  return result;
}

Result<bool> ParquetBloomRowGroupFilter::ShouldRead(
    const ::parquet::arrow::SchemaManifest& manifest,
    const ::parquet::RowGroupMetaData& row_group,
    ::parquet::RowGroupBloomFilterReader* bloom_reader,
    ::arrow::io::RandomAccessFile* input) const {
  if (row_group.num_rows() <= 0) {
    return kRowsCannotMatch;
  }
  try {
    BloomVisitor visitor(manifest, row_group, bloom_reader, input,
                         field_id_to_column_idx_);
    return VisitEvaluator(bound_, visitor);
  } catch (const ::parquet::ParquetException& e) {
    return Invalid("Cannot read Parquet Bloom filter: {}", e.what());
  }
}

}  // namespace iceberg::parquet
