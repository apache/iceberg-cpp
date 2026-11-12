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
#include <optional>
#include <type_traits>
#include <vector>

#include <arrow/io/memory.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/encoding.h>
#include <parquet/statistics.h>

#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/expression_visitor.h"
#include "iceberg/expression/rewrite_not.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_dictionary_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/parquet/parquet_value_util_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::parquet {
namespace {

bool IsDictionaryEncoding(::parquet::Encoding::type encoding) {
  return encoding == ::parquet::Encoding::PLAIN_DICTIONARY ||
         encoding == ::parquet::Encoding::RLE_DICTIONARY;
}

// A dictionary page alone is insufficient: later pages may have fallen back to
// plain encoding. Follow Java's ParquetUtil.hasNonDictionaryPages rules.
bool IsFullyDictionaryEncoded(const ::parquet::ColumnChunkMetaData& column) {
  const auto& stats = column.encoding_stats();
  if (!stats.empty()) {
    bool has_dictionary_data = false;
    for (const auto& stat : stats) {
      if (stat.page_type == ::parquet::PageType::DATA_PAGE ||
          stat.page_type == ::parquet::PageType::DATA_PAGE_V2) {
        if (!IsDictionaryEncoding(stat.encoding)) {
          return false;
        }
        has_dictionary_data |= stat.count > 0;
      }
    }
    return has_dictionary_data;
  }
  // With legacy encodings, PLAIN_DICTIONARY identifies dictionary data pages.
  // With RLE_DICTIONARY, PLAIN could describe either the dictionary or fallback
  // data, so encoding statistics are required.
  const auto& encodings = column.encodings();
  return std::ranges::find(encodings, ::parquet::Encoding::PLAIN_DICTIONARY) !=
             encodings.end() &&
         std::ranges::all_of(encodings, [](auto encoding) {
           return encoding == ::parquet::Encoding::PLAIN_DICTIONARY ||
                  encoding == ::parquet::Encoding::RLE ||
                  encoding == ::parquet::Encoding::BIT_PACKED;
         });
}

template <typename DType>
Result<std::vector<Literal>> DecodeDictionary(const ::parquet::DictionaryPage& page,
                                              const ::parquet::ColumnDescriptor& column,
                                              const PrimitiveType& type) {
  auto encoding = page.encoding() == ::parquet::Encoding::PLAIN_DICTIONARY
                      ? ::parquet::Encoding::PLAIN
                      : page.encoding();
  auto decoder = ::parquet::MakeTypedDecoder<DType>(encoding, &column);
  decoder->SetData(page.num_values(), page.data(), page.size());
  std::array<typename DType::c_type, 1024> buffer;
  std::vector<Literal> values;
  int remaining = page.num_values();
  while (remaining > 0) {
    const int count = std::min(remaining, static_cast<int>(buffer.size()));
    if (decoder->Decode(buffer.data(), count) != count) {
      return Invalid("Truncated Parquet dictionary page");
    }
    for (int i = 0; i < count; ++i) {
      auto converted = [&]() {
        if constexpr (std::is_same_v<DType, ::parquet::FLBAType>) {
          return ParquetValueToLiteral(buffer[i], type, column.type_length());
        } else {
          return ParquetValueToLiteral(buffer[i], type);
        }
      }();
      ICEBERG_ASSIGN_OR_RAISE(auto value, std::move(converted));
      values.push_back(std::move(value));
    }
    remaining -= count;
  }
  return values;
}

class DictionaryVisitor : public BoundVisitor<bool> {
 public:
  DictionaryVisitor(const ::parquet::arrow::SchemaManifest& manifest,
                    const ::parquet::RowGroupMetaData& row_group,
                    const std::unordered_map<int32_t, int>& columns,
                    const std::shared_ptr<::arrow::io::RandomAccessFile>& input,
                    const ::parquet::ReaderProperties& properties,
                    const ::parquet::ApplicationVersion& writer_version)
      : manifest_(manifest),
        row_group_(row_group),
        columns_(columns),
        input_(input),
        properties_(properties),
        writer_version_(writer_version) {}

  Result<bool> AlwaysTrue() override { return true; }
  Result<bool> AlwaysFalse() override { return false; }
  // Make rewrites NOT. Negating an inclusive result would be unsafe.
  Result<bool> Not(bool) override { return true; }
  Result<bool> And(bool left, bool right) override { return left && right; }
  Result<bool> Or(bool left, bool right) override { return left || right; }
  // Dictionaries contain only non-null values. A rewritten NOT(range) includes
  // IS_NULL, which stays inclusive here; footer statistics may rule it out.
  Result<bool> IsNull(const std::shared_ptr<Bound>&) override { return true; }
  Result<bool> NotNull(const std::shared_ptr<Bound>&) override { return true; }

  Result<bool> IsNaN(const std::shared_ptr<Bound>& expr) override {
    return AnyMatch(expr, [](const Literal& value) { return value.IsNaN(); });
  }
  Result<bool> NotNaN(const std::shared_ptr<Bound>& expr) override {
    return AnyMatch(expr, [](const Literal& value) { return !value.IsNaN(); }, true);
  }

  Result<bool> Lt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return (item <=> value) == std::partial_ordering::unordered || item < value;
    });
  }

  Result<bool> LtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return (item <=> value) == std::partial_ordering::unordered || item <= value;
    });
  }

  Result<bool> Gt(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return (item <=> value) == std::partial_ordering::unordered || item > value;
    });
  }

  Result<bool> GtEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return (item <=> value) == std::partial_ordering::unordered || item >= value;
    });
  }

  Result<bool> Eq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return (item <=> value) == std::partial_ordering::unordered || item == value;
    });
  }

  Result<bool> NotEq(const std::shared_ptr<Bound>& expr, const Literal& value) override {
    return AnyMatch(
        expr,
        [&](const Literal& item) {
          return (item <=> value) == std::partial_ordering::unordered || item != value;
        },
        true);
  }

  Result<bool> In(const std::shared_ptr<Bound>& expr,
                  const BoundSetPredicate::LiteralSet& values) override {
    return AnyMatch(expr, [&](const Literal& item) { return values.contains(item); });
  }

  Result<bool> NotIn(const std::shared_ptr<Bound>& expr,
                     const BoundSetPredicate::LiteralSet& values) override {
    return AnyMatch(
        expr, [&](const Literal& item) { return !values.contains(item); }, true);
  }

  Result<bool> StartsWith(const std::shared_ptr<Bound>& expr,
                          const Literal& value) override {
    return AnyMatch(expr, [&](const Literal& item) {
      return std::get<std::string>(item.value())
          .starts_with(std::get<std::string>(value.value()));
    });
  }

  Result<bool> NotStartsWith(const std::shared_ptr<Bound>& expr,
                             const Literal& value) override {
    return AnyMatch(
        expr,
        [&](const Literal& item) {
          return !std::get<std::string>(item.value())
                      .starts_with(std::get<std::string>(value.value()));
        },
        true);
  }

 private:
  template <typename Predicate>
  Result<bool> AnyMatch(const std::shared_ptr<Bound>& expr, Predicate predicate,
                        bool nulls_match = false) {
    auto ref = std::dynamic_pointer_cast<BoundReference>(expr);
    ICEBERG_ASSIGN_OR_RAISE(auto dictionary, GetDictionary(ref));
    if (!dictionary || (nulls_match && MayContainNull(*ref))) {
      return true;
    }
    return std::ranges::any_of(*dictionary, predicate);
  }

  bool MayContainNull(const BoundReference& ref) const {
    const auto column = row_group_.ColumnChunk(columns_.at(ref.field_id()));
    try {
      auto stats = column->statistics();
      return !stats || !stats->HasNullCount() || stats->null_count() != 0;
    } catch (const ::parquet::ParquetException&) {
      // Optional, unusable statistics cannot prove that nulls are absent.
      return true;
    }
  }

  Result<const std::vector<Literal>*> GetDictionary(
      const std::shared_ptr<BoundReference>& ref) {
    if (!ref || !ref->type()->is_primitive() ||
        ref->type()->type_id() == TypeId::kUnknown ||
        MetadataColumns::IsMetadataColumn(ref->field_id())) {
      return nullptr;
    }
    auto index = columns_.find(ref->field_id());
    if (index == columns_.end()) {
      // Missing fields may have initial defaults.
      return nullptr;
    }
    auto field = manifest_.column_index_to_field.find(index->second);
    if (field == manifest_.column_index_to_field.end() ||
        !ValidateParquetTypeCompatibility(*ref->type(), *field->second)) {
      return nullptr;
    }
    auto [cached, inserted] = dictionaries_.try_emplace(ref->field_id());
    if (inserted) {
      const auto column = row_group_.ColumnChunk(index->second);
      const auto* descriptor = manifest_.descr->Column(index->second);
      // A raw dictionary slice has no Parquet column decryption context. Leave
      // encrypted columns to the normal reader rather than interpreting ciphertext.
      if (descriptor->max_repetition_level() > 0 ||
          descriptor->physical_type() == ::parquet::Type::INT96 ||
          descriptor->physical_type() == ::parquet::Type::BOOLEAN ||
          column->crypto_metadata() || !IsFullyDictionaryEncoded(*column)) {
        return nullptr;
      }
      // GetColumnPageReader eagerly reads the entire column chunk by default.
      // Restrict planning I/O to the dictionary page, including its header.
      const int64_t start = column->dictionary_page_offset();
      const int64_t end = column->data_page_offset();
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto file_size, input_->GetSize());
      if (!column->has_dictionary_page() || start < 4 || end <= start ||
          end > file_size) {
        return Invalid("Invalid Parquet dictionary offsets [{}, {}) for field {}", start,
                       end, ref->field_id());
      }
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto data, input_->ReadAt(start, end - start));
      if (data->size() != end - start) {
        return Invalid("Truncated Parquet dictionary page for field {}", ref->field_id());
      }
      auto stream = std::make_shared<::arrow::io::BufferReader>(std::move(data));
      // Preserve Arrow's compatibility behavior for old compressed page headers.
      bool always_compressed = writer_version_.VersionLt(
          ::parquet::ApplicationVersion::PARQUET_CPP_10353_FIXED_VERSION());
      auto pages =
          ::parquet::PageReader::Open(stream, column->num_values(), column->compression(),
                                      properties_, *descriptor, always_compressed);
      auto page = pages->NextPage();
      if (!page || page->type() != ::parquet::PageType::DICTIONARY_PAGE) {
        return Invalid("Missing Parquet dictionary page for field {}", ref->field_id());
      }
      const auto& dictionary = static_cast<const ::parquet::DictionaryPage&>(*page);
      if (dictionary.num_values() < 0) {
        return Invalid("Negative Parquet dictionary size for field {}", ref->field_id());
      }
      const auto& type = static_cast<const PrimitiveType&>(*ref->type());
      auto decoded = [&]() -> Result<std::vector<Literal>> {
        switch (descriptor->physical_type()) {
          case ::parquet::Type::INT32:
            return DecodeDictionary<::parquet::Int32Type>(dictionary, *descriptor, type);
          case ::parquet::Type::INT64:
            return DecodeDictionary<::parquet::Int64Type>(dictionary, *descriptor, type);
          case ::parquet::Type::FLOAT:
            return DecodeDictionary<::parquet::FloatType>(dictionary, *descriptor, type);
          case ::parquet::Type::DOUBLE:
            return DecodeDictionary<::parquet::DoubleType>(dictionary, *descriptor, type);
          case ::parquet::Type::BYTE_ARRAY:
            return DecodeDictionary<::parquet::ByteArrayType>(dictionary, *descriptor,
                                                              type);
          case ::parquet::Type::FIXED_LEN_BYTE_ARRAY:
            return DecodeDictionary<::parquet::FLBAType>(dictionary, *descriptor, type);
          default:
            return NotSupported("Unsupported Parquet dictionary type");
        }
      }();
      ICEBERG_ASSIGN_OR_RAISE(auto values, std::move(decoded));
      cached->second = std::move(values);
    }
    return cached->second ? &*cached->second : nullptr;
  }

  const ::parquet::arrow::SchemaManifest& manifest_;
  const ::parquet::RowGroupMetaData& row_group_;
  const std::unordered_map<int32_t, int>& columns_;
  const std::shared_ptr<::arrow::io::RandomAccessFile>& input_;
  const ::parquet::ReaderProperties& properties_;
  const ::parquet::ApplicationVersion& writer_version_;
  std::unordered_map<int32_t, std::optional<std::vector<Literal>>> dictionaries_;
};

}  // namespace

Result<std::unique_ptr<ParquetDictionaryRowGroupFilter>>
ParquetDictionaryRowGroupFilter::Make(const std::shared_ptr<Expression>& filter,
                                      const ::parquet::SchemaDescriptor& file_schema) {
  auto result = std::unique_ptr<ParquetDictionaryRowGroupFilter>(
      new ParquetDictionaryRowGroupFilter());
  for (int i = 0; i < file_schema.num_columns(); ++i) {
    auto id = file_schema.Column(i)->schema_node()->field_id();
    if (id >= 0) {
      result->field_id_to_column_idx_.emplace(id, i);
    }
  }
  result->bound_ = True::Instance();
  if (filter) {
    ICEBERG_ASSIGN_OR_RAISE(result->bound_, RewriteNot::Visit(filter));
  }
  return result;
}

Result<bool> ParquetDictionaryRowGroupFilter::ShouldRead(
    const ::parquet::arrow::SchemaManifest& manifest,
    const ::parquet::FileMetaData& metadata, int row_group_index,
    const std::shared_ptr<::arrow::io::RandomAccessFile>& input,
    const ::parquet::ReaderProperties& properties) const {
  auto row_group = metadata.RowGroup(row_group_index);
  if (row_group->num_rows() <= 0) {
    return false;
  }
  try {
    DictionaryVisitor visitor(manifest, *row_group, field_id_to_column_idx_, input,
                              properties, metadata.writer_version());
    return Visit<bool>(bound_, visitor);
  } catch (const ::parquet::ParquetException& e) {
    return Invalid("Cannot read Parquet dictionary: {}", e.what());
  }
}

}  // namespace iceberg::parquet
