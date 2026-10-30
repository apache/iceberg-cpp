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

#include <bit>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/concatenate.h>
#include <arrow/c/bridge.h>
#include <arrow/compute/api.h>
#include <arrow/extension/parquet_variant.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/column_writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/schema.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow_c_data_util_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/name_mapping.h"
#include "iceberg/parquet/parquet_reader.h"
#include "iceberg/parquet/parquet_schema_util_internal.h"
#include "iceberg/parquet/parquet_variant_util_internal.h"
#include "iceberg/parquet/parquet_writer.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/std_io.h"
#include "iceberg/test/temp_file_test_base.h"
#include "iceberg/type.h"

#if ICEBERG_REQUIRE_ARROW_VARIANT
static_assert(ICEBERG_HAS_ARROW_VARIANT,
              "Variant tests require Arrow's variant APIs; they must not be skipped");
#endif

#if ICEBERG_HAS_ARROW_VARIANT
#  include <parquet/variant/builder.h>
#  include <parquet/variant/decoding.h>
#endif

namespace iceberg::parquet {

TEST(ParquetVariantSchemaTest, StorageChildrenHaveNoFieldIds) {
  Schema schema({SchemaField::MakeRequired(1, "payload", variant())});
  auto result = ToParquetSchema(schema);
  ASSERT_THAT(result, IsOk());
  auto group = static_cast<const ::parquet::schema::GroupNode*>(
      result.value()->schema_root().get());
  auto field = static_cast<const ::parquet::schema::GroupNode*>(group->field(0).get());
  EXPECT_TRUE(field->logical_type()->is_variant());
  EXPECT_EQ(field->field_id(), 1);
  ASSERT_EQ(field->field_count(), 2);
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(field->field(i)->field_id(), -1);
    EXPECT_EQ(field->field(i)->repetition(), ::parquet::Repetition::REQUIRED);
  }
}

#if !ICEBERG_HAS_ARROW_VARIANT

TEST(ParquetVariantTest, MissingArrowUtilitiesFailsBeforeCreatingFile) {
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "payload", variant())});
  auto io = std::make_shared<test::StdFileIO>();
  ParquetWriter writer;
  EXPECT_THAT(writer.Open({.path = "unused-variant.parquet", .schema = schema, .io = io}),
              HasErrorMessage("requires Arrow's variant validation"));
  EXPECT_FALSE(std::filesystem::exists("unused-variant.parquet"));
}

#else

namespace {

using ::parquet::variant::VariantArrayBuilder;
using ::parquet::variant::VariantPrimitiveType;
using ::parquet::variant::VariantValueView;

std::shared_ptr< ::arrow::Schema> ArrowSchemaFor(const Schema& schema) {
  ArrowSchema c_schema;
  arrow::RegisterAll();
  if (!ToArrowSchema(schema, &c_schema).has_value()) {
    return nullptr;
  }
  return ::arrow::ImportSchema(&c_schema).ValueOrDie();
}

std::shared_ptr< ::arrow::Array> CanonicalVariant(
    const std::shared_ptr< ::arrow::Array>& array,
    const std::shared_ptr< ::arrow::DataType>& type) {
  auto extension = std::static_pointer_cast< ::arrow::ExtensionArray>(array);
  auto storage_type =
      std::static_pointer_cast< ::arrow::ExtensionType>(type)->storage_type();
  auto storage = ::arrow::compute::Cast(extension->storage(), storage_type).ValueOrDie();
  return ::arrow::ExtensionType::WrapArray(type, storage.make_array());
}

// Compare logical objects and arrays by value, including integer widths and
// short-string versus long-string encodings, rather than by serialized bytes.
nlohmann::json SemanticValue(const VariantValueView& value) {
  return std::visit(
      [&](const auto& view) -> nlohmann::json {
        using View = std::decay_t<decltype(view)>;
        if constexpr (std::is_same_v<View, ::parquet::variant::VariantObjectView>) {
          auto object = nlohmann::json::object();
          for (const auto& field : view.fields()) {
            object[field.name] = SemanticValue(*view.GetField(field.name));
          }
          return object;
        } else if constexpr (std::is_same_v<View, ::parquet::variant::VariantArrayView>) {
          auto array = nlohmann::json::array();
          for (size_t i = 0; i < view.elements().size(); ++i) {
            array.push_back(SemanticValue(*view.GetElement(i)));
          }
          return array;
        } else if constexpr (std::is_same_v<View,
                                            ::parquet::variant::VariantShortStringView>) {
          return std::string(view.string());
        } else {
          auto type = view.type();
          if (type == VariantPrimitiveType::kNull) return nullptr;
          if (type == VariantPrimitiveType::kBooleanTrue) return true;
          if (type == VariantPrimitiveType::kBooleanFalse) return false;
          if (type >= VariantPrimitiveType::kInt8 &&
              type <= VariantPrimitiveType::kInt64) {
            auto payload = view.payload();
            uint64_t integer = 0;
            for (size_t i = 0; i < payload.size(); ++i) {
              integer |= static_cast<uint64_t>(static_cast<uint8_t>(payload[i]))
                         << (8 * i);
            }
            if (payload.size() < 8 && (static_cast<uint8_t>(payload.back()) & 0x80)) {
              integer |= ~uint64_t{0} << (payload.size() * 8);
            }
            return std::bit_cast<int64_t>(integer);
          }
          if (type == VariantPrimitiveType::kString) {
            // The primitive string payload starts with a four-byte length.
            return std::string(view.payload().substr(4));
          }
          return nlohmann::json::array(
              {static_cast<int>(type), std::string(view.payload())});
        }
      },
      value.data());
}

void ExpectVariantsEqual(const ::arrow::Array& expected, const ::arrow::Array& actual) {
  ASSERT_EQ(expected.length(), actual.length());
  const auto& expected_storage = static_cast<const ::arrow::StructArray&>(
      *static_cast<const ::arrow::ExtensionArray&>(expected).storage());
  const auto& actual_storage = static_cast<const ::arrow::StructArray&>(
      *static_cast<const ::arrow::ExtensionArray&>(actual).storage());
  const auto& expected_metadata =
      static_cast<const ::arrow::BinaryArray&>(*expected_storage.field(0));
  const auto& actual_metadata =
      static_cast<const ::arrow::BinaryArray&>(*actual_storage.field(0));
  const auto& expected_values =
      static_cast<const ::arrow::BinaryArray&>(*expected_storage.field(1));
  const auto& actual_values =
      static_cast<const ::arrow::BinaryArray&>(*actual_storage.field(1));
  for (int64_t i = 0; i < expected.length(); ++i) {
    ASSERT_EQ(expected.IsNull(i), actual.IsNull(i)) << i;
    if (expected.IsNull(i)) continue;
    auto em = ::parquet::variant::VariantMetadataView::Make(expected_metadata.GetView(i));
    auto am = ::parquet::variant::VariantMetadataView::Make(actual_metadata.GetView(i));
    EXPECT_EQ(
        SemanticValue(VariantValueView::MakeWithValidate(expected_values.GetView(i), em)),
        SemanticValue(VariantValueView::MakeWithValidate(actual_values.GetView(i), am)))
        << i;
  }
}

}  // namespace

class ParquetVariantTest : public TempFileTestBase {
 protected:
  std::shared_ptr<FileIO> io_ = std::make_shared<test::StdFileIO>();
  std::shared_ptr<Schema> schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "payload", variant())});

  std::shared_ptr< ::arrow::Array> Roundtrip(
      const std::shared_ptr< ::arrow::Array>& input,
      std::shared_ptr<Type> target = nullptr, int batches = 1, int32_t field_id = 1,
      std::shared_ptr<Schema> read_schema = nullptr) {
    auto path = CreateNewTempFilePathWithSuffix(".parquet");
    WriterOptions options{.path = path, .schema = schema_, .io = io_};
    options.properties.Set(WriterProperties::kParquetCompression,
                           std::string("uncompressed"));
    if (target) options.variant_shredding_types.emplace(field_id, std::move(target));
    ParquetWriter writer;
    auto opened = writer.Open(options);
    EXPECT_THAT(opened, IsOk());
    if (!opened) return nullptr;
    auto arrow_schema = ArrowSchemaFor(*schema_);
    for (int i = 0; i < batches; ++i) {
      auto batch = ::arrow::RecordBatch::Make(arrow_schema, input->length(), {input});
      ArrowArray c_array;
      EXPECT_TRUE(::arrow::ExportRecordBatch(*batch, &c_array).ok());
      EXPECT_THAT(writer.Write(&c_array), IsOk());
    }
    EXPECT_THAT(writer.Close(), IsOk());
    auto metrics = writer.metrics();
    EXPECT_THAT(metrics, IsOk());
    if (metrics) {
      EXPECT_FALSE(metrics->lower_bounds.contains(1));
      EXPECT_FALSE(metrics->upper_bounds.contains(1));
    }
    auto stream = arrow::OpenArrowInputStream(io_, path, std::nullopt).value();
    auto file = ::parquet::ParquetFileReader::Open(stream);
    const auto& descr = file->metadata()->schema();
    auto root =
        static_cast<const ::parquet::schema::GroupNode*>(descr->schema_root().get());
    if (schema_->fields()[0].type()->is_variant()) {
      EXPECT_TRUE(root->field(0)->logical_type()->is_variant());
    }
    for (int i = 0; i < descr->num_columns(); ++i) {
      auto node = descr->Column(i)->schema_node().get();
      for (auto parent = node->parent(); parent != nullptr; parent = parent->parent()) {
        if (parent->logical_type()->is_variant()) {
          EXPECT_EQ(node->field_id(), -1);
        }
      }
    }
    ::parquet::arrow::SchemaManifest manifest;
    auto properties = ::parquet::default_arrow_reader_properties();
    properties.set_arrow_extensions_enabled(true);
    EXPECT_TRUE(
        ::parquet::arrow::SchemaManifest::Make(descr, nullptr, properties, &manifest)
            .ok());
    auto projection = Project(*schema_, manifest);
    EXPECT_THAT(projection, IsOk());
    if (projection)
      EXPECT_EQ(SelectedColumnIndices(*projection).size(), descr->num_columns());
    file->Close();

    ParquetReader reader;
    if (!read_schema) read_schema = schema_;
    EXPECT_THAT(reader.Open({.path = path, .io = io_, .projection = read_schema}),
                IsOk());
    auto c_schema = reader.Schema().value();
    auto output_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
    EXPECT_TRUE(output_schema->Equals(*ArrowSchemaFor(*read_schema)));
    std::vector<std::shared_ptr< ::arrow::Array> > chunks;
    while (true) {
      auto next = reader.Next();
      EXPECT_THAT(next, IsOk());
      if (!next || !next->has_value()) break;
      auto c_array = next->value();
      auto batch = ::arrow::ImportRecordBatch(&c_array, output_schema).ValueOrDie();
      chunks.push_back(batch->column(0));
    }
    EXPECT_THAT(reader.Close(), IsOk());
    if (chunks.empty())
      return ::arrow::MakeArrayOfNull(arrow_schema->field(0)->type(), 0).ValueOrDie();
    return ::arrow::Concatenate(chunks).ValueOrDie();
  }
};

TEST_F(ParquetVariantTest, UnshreddedValues) {
  VariantArrayBuilder builder;
  builder.AppendNull();
  builder.AppendVariantNull();
  builder.AppendInt8(-42);
  builder.AppendString("hello");
  builder.AppendDate(12345);
  builder.AppendTimestampNanos(123456789, true);
  builder.AppendBinary(std::string("\0\xff", 2));
  builder.AppendDecimal16(::arrow::Decimal128(12345), 2);
  auto object = builder.StartObject();
  object.AppendString("name", "event");
  auto list = object.StartList("items");
  list.AppendBoolean(true);
  list.AppendVariantNull();
  list.AppendInt64(9);
  list.Finish();
  object.Finish();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto output = Roundtrip(input);
  ASSERT_NE(output, nullptr);
  ExpectVariantsEqual(*input, *output);
}

TEST_F(ParquetVariantTest, PrimitiveShreddingAndResidualValues) {
  VariantArrayBuilder builder;
  builder.AppendInt8(-42);
  builder.AppendInt64(100);
  builder.AppendString("residual");
  builder.AppendNull();
  builder.AppendVariantNull();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto output = Roundtrip(input, int64(), 2);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->length(), input->length() * 2);
  ExpectVariantsEqual(*input, *output->Slice(0, input->length()));
  ExpectVariantsEqual(*input, *output->Slice(input->length()));
}

TEST_F(ParquetVariantTest, ObjectAndListShredding) {
  VariantArrayBuilder builder;
  auto object = builder.StartObject();
  object.AppendInt8("count", 7);
  object.AppendString("other", "residual field");
  auto list = object.StartList("items");
  list.AppendInt32(3);
  list.AppendString("mixed");
  list.AppendVariantNull();
  list.Finish();
  object.Finish();
  auto missing = builder.StartObject();
  missing.AppendVariantNull("count");
  missing.Finish();
  builder.AppendInt64(11);
  builder.AppendNull();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto target = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeOptional(91, "count", int64()),
      SchemaField::MakeOptional(92, "items",
                                std::make_shared<ListType>(93, int64(), true))});
  auto output = Roundtrip(input, target);
  ASSERT_NE(output, nullptr);
  ExpectVariantsEqual(*input, *output);
}

TEST_F(ParquetVariantTest, ShreddingPreservesPrimitiveTypes) {
  VariantArrayBuilder builder;
  builder.AppendBoolean(true);
  builder.AppendBoolean(false);
  builder.AppendInt64(42);
  builder.AppendFloat(1.25F);
  builder.AppendDouble(1.25);
  builder.AppendString("true");
  builder.AppendString("12.34");
  builder.AppendDecimal16(::arrow::Decimal128(1234), 2);
  builder.AppendDecimal16(::arrow::Decimal128(1230), 2);
  builder.AppendTimestampMicros(123456, false);
  builder.AppendTimestampNanos(123456000, false);
  builder.AppendNull();
  builder.AppendVariantNull();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  for (const auto& target : std::vector<std::shared_ptr<Type> >{
           boolean(), int32(), int64(), float32(), float64(), decimal(30, 2),
           decimal(30, 1), timestamp_ns()}) {
    SCOPED_TRACE(target->ToString());
    auto output = Roundtrip(input, target);
    ASSERT_NE(output, nullptr);
    ExpectVariantsEqual(*input, *output);
  }
}

TEST_F(ParquetVariantTest, NestedShreddingPreservesPrimitiveTypes) {
  VariantArrayBuilder builder;
  auto object = builder.StartObject();
  object.AppendBoolean("count", true);
  auto list = object.StartList("items");
  list.AppendInt8(7);
  list.AppendBoolean(false);
  list.AppendDouble(2.0);
  list.Finish();
  object.Finish();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto target = std::make_shared<StructType>(std::vector<SchemaField>{
      SchemaField::MakeOptional(91, "count", int64()),
      SchemaField::MakeOptional(92, "items",
                                std::make_shared<ListType>(93, int64(), true))});
  auto output = Roundtrip(input, target);
  ASSERT_NE(output, nullptr);
  ExpectVariantsEqual(*input, *output);
}

TEST_F(ParquetVariantTest, EmptyAndAllNullBatches) {
  auto type = ArrowSchemaFor(*schema_)->field(0)->type();
  for (int64_t length : {0, 3}) {
    auto input = ::arrow::MakeArrayOfNull(type, length).ValueOrDie();
    auto output = Roundtrip(input, int64());
    ASSERT_NE(output, nullptr);
    ExpectVariantsEqual(*input, *output);
  }
}

TEST_F(ParquetVariantTest, MissingOptionalVariantUsesCompleteAnchor) {
  VariantArrayBuilder builder;
  builder.AppendInt64(1);
  builder.AppendNull();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto projected = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(5, "added_later", variant())});
  auto output = Roundtrip(input, int64(), 1, 1, projected);
  ASSERT_NE(output, nullptr);
  EXPECT_EQ(output->length(), 2);
  EXPECT_EQ(output->null_count(), 2);
}

TEST_F(ParquetVariantTest, NullStructParent) {
  VariantArrayBuilder builder;
  builder.AppendInt64(7);
  builder.AppendInt64(11);
  builder.AppendVariantNull();
  auto values =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload",
      std::make_shared<StructType>(
          std::vector<SchemaField>{SchemaField::MakeOptional(2, "nested", variant())}))});
  auto type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto bitmap = ::arrow::AllocateEmptyBitmap(3).ValueOrDie();
  bitmap->mutable_data()[0] = 0x05;
  auto input =
      ::arrow::StructArray::Make({values}, type->fields(), bitmap, 1).ValueOrDie();
  auto output = Roundtrip(input, int64(), 1, 2);
  ASSERT_NE(output, nullptr);
  EXPECT_TRUE(output->IsNull(1));
  const auto& child = static_cast<const ::arrow::StructArray&>(*output).field(0);
  ExpectVariantsEqual(*values->Slice(0, 1), *child->Slice(0, 1));
  ExpectVariantsEqual(*values->Slice(2, 1), *child->Slice(2, 1));
}

TEST_F(ParquetVariantTest, RequiredVariantRejectsSqlNull) {
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "payload", variant())});
  auto type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto input = ::arrow::MakeArrayOfNull(type, 1).ValueOrDie();
  auto path = CreateNewTempFilePathWithSuffix(".parquet");
  ParquetWriter writer;
  WriterProperties properties;
  properties.Set(WriterProperties::kParquetCompression, std::string("uncompressed"));
  ASSERT_THAT(writer.Open({.path = path,
                           .schema = schema_,
                           .io = io_,
                           .properties = std::move(properties)}),
              IsOk());
  auto batch = ::arrow::RecordBatch::Make(ArrowSchemaFor(*schema_), 1, {input});
  ArrowArray c_array;
  ASSERT_TRUE(::arrow::ExportRecordBatch(*batch, &c_array).ok());
  EXPECT_FALSE(writer.Write(&c_array).has_value());
  EXPECT_THAT(writer.Close(), IsOk());
}

TEST_F(ParquetVariantTest, RequiredVariantUnderNullStructParent) {
  VariantArrayBuilder builder;
  builder.AppendInt64(7);
  builder.AppendInt64(11);
  builder.AppendInt64(13);
  auto values =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload",
      std::make_shared<StructType>(
          std::vector<SchemaField>{SchemaField::MakeRequired(2, "nested", variant())}))});
  auto type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto bitmap = ::arrow::AllocateEmptyBitmap(3).ValueOrDie();
  bitmap->mutable_data()[0] = 0x05;
  auto input =
      ::arrow::StructArray::Make({values}, type->fields(), bitmap, 1).ValueOrDie();
  for (const auto& target : std::vector<std::shared_ptr<Type> >{nullptr, int64()}) {
    auto output = Roundtrip(input->Slice(1, 2), target, 1, 2);
    ASSERT_NE(output, nullptr);
    ASSERT_EQ(output->length(), 2);
    EXPECT_TRUE(output->IsNull(0));
    const auto& child = static_cast<const ::arrow::StructArray&>(*output).field(0);
    ExpectVariantsEqual(*values->Slice(2, 1), *child->Slice(1, 1));
  }
}

TEST_F(ParquetVariantTest, RequiredVariantInLargeListWithNullElements) {
  VariantArrayBuilder builder;
  builder.AppendInt64(7);
  builder.AppendInt64(11);
  builder.AppendInt64(13);
  auto values =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto element = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(3, "nested", variant())});
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload", std::make_shared<ListType>(2, element, true))});
  auto arrow_schema = ArrowSchemaFor(*schema_);
  auto bitmap = ::arrow::AllocateEmptyBitmap(3).ValueOrDie();
  bitmap->mutable_data()[0] = 0x02;
  auto entries =
      ::arrow::StructArray::Make(
          {values}, arrow_schema->field(0)->type()->field(0)->type()->fields(), bitmap, 2)
          .ValueOrDie();
  auto offsets =
      ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0, 2, 3]").ValueOrDie();
  auto input =
      ::arrow::ListArray::FromArrays(arrow_schema->field(0)->type(), *offsets, *entries)
          .ValueOrDie();
  auto path = CreateNewTempFilePathWithSuffix(".parquet");
  ParquetWriter writer;
  WriterOptions options{.path = path, .schema = schema_, .io = io_};
  options.properties.Set(WriterProperties::kParquetCompression,
                         std::string("uncompressed"));
  options.variant_shredding_types.emplace(3, int64());
  ASSERT_THAT(writer.Open(options), IsOk());
  auto batch = ::arrow::RecordBatch::Make(arrow_schema, 2, {input});
  ArrowArray c_array;
  ASSERT_TRUE(::arrow::ExportRecordBatch(*batch, &c_array).ok());
  ASSERT_THAT(writer.Write(&c_array), IsOk());
  ASSERT_THAT(writer.Close(), IsOk());

  ReaderOptions read_options{.path = path, .io = io_, .projection = schema_};
  read_options.properties.Set(ReaderProperties::kArrowUseLargeList, true);
  read_options.properties.Set(ReaderProperties::kBatchSize, int64_t{1});
  ParquetReader reader;
  ASSERT_THAT(reader.Open(read_options), IsOk());
  auto c_schema = reader.Schema().value();
  auto output_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  ASSERT_EQ(output_schema->field(0)->type()->id(), ::arrow::Type::LARGE_LIST);
  for (int i = 0; i < 2; ++i) {
    auto next = reader.Next();
    ASSERT_THAT(next, IsOk());
    ASSERT_TRUE(next->has_value());
    auto output_array = next->value();
    auto output = ::arrow::ImportRecordBatch(&output_array, output_schema).ValueOrDie();
    auto list = std::static_pointer_cast< ::arrow::LargeListArray>(output->column(0));
    auto elements = std::static_pointer_cast< ::arrow::StructArray>(list->value_slice(0));
    ASSERT_EQ(elements->length(), i == 0 ? 2 : 1);
    EXPECT_TRUE(elements->IsNull(0));
    if (i == 0) {
      ExpectVariantsEqual(*values->Slice(1, 1), *elements->field(0)->Slice(1, 1));
    }
  }
  EXPECT_FALSE(reader.Next().value().has_value());
  EXPECT_THAT(reader.Close(), IsOk());
}

TEST_F(ParquetVariantTest, RequiredVariantInMapWithNullValues) {
  VariantArrayBuilder builder;
  builder.AppendInt64(7);
  builder.AppendInt64(11);
  auto values =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  auto item = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(4, "nested", variant())});
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload",
      std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                SchemaField::MakeOptional(3, "value", item)))});
  auto map_type = std::static_pointer_cast< ::arrow::MapType>(
      ArrowSchemaFor(*schema_)->field(0)->type());
  auto bitmap = ::arrow::AllocateEmptyBitmap(2).ValueOrDie();
  bitmap->mutable_data()[0] = 0x02;
  auto items =
      ::arrow::StructArray::Make({values}, map_type->item_type()->fields(), bitmap, 1)
          .ValueOrDie();
  auto offsets =
      ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0, 2]").ValueOrDie();
  auto keys =
      ::arrow::json::ArrayFromJSONString(::arrow::utf8(), "[\"a\", \"b\"]").ValueOrDie();
  auto input = ::arrow::MapArray::FromArrays(map_type, offsets, keys, items).ValueOrDie();
  auto output = Roundtrip(input, int64(), 1, 4);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->length(), 1);
  auto output_items = std::static_pointer_cast< ::arrow::StructArray>(
      static_cast<const ::arrow::MapArray&>(*output).items());
  EXPECT_TRUE(output_items->IsNull(0));
  ExpectVariantsEqual(*values->Slice(1, 1), *output_items->field(0)->Slice(1, 1));
}

TEST_F(ParquetVariantTest, NestedVariantsAndSlicedArrays) {
  VariantArrayBuilder builder;
  builder.AppendInt8(7);
  builder.AppendString("residual");
  builder.AppendNull();
  builder.AppendVariantNull();
  auto canonical_type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto values = CanonicalVariant(builder.Finish(), canonical_type);

  // A sliced struct retains an offset into its unsliced child arrays.
  auto structure = std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeOptional(2, "nested", variant())});
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "payload", structure)});
  auto structure_type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto input_struct =
      ::arrow::StructArray::Make({values}, structure_type->fields()).ValueOrDie();
  auto output_struct = Roundtrip(input_struct->Slice(1, 3), int64(), 1, 2);
  ASSERT_NE(output_struct, nullptr);
  ExpectVariantsEqual(*values->Slice(1, 3),
                      *static_cast<const ::arrow::StructArray&>(*output_struct).field(0));

  auto offsets =
      ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0, 2, 2, 4]").ValueOrDie();
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload", std::make_shared<ListType>(2, variant(), true))});
  auto list_type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto input_list =
      ::arrow::ListArray::FromArrays(list_type, *offsets, *values).ValueOrDie();
  auto output_list = Roundtrip(input_list, int64(), 1, 2);
  ASSERT_NE(output_list, nullptr);
  ExpectVariantsEqual(*values,
                      *static_cast<const ::arrow::ListArray&>(*output_list).values());

  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{SchemaField::MakeOptional(
      1, "payload",
      std::make_shared<MapType>(SchemaField::MakeRequired(2, "key", string()),
                                SchemaField::MakeOptional(3, "value", variant())))});
  auto keys =
      ::arrow::json::ArrayFromJSONString(::arrow::utf8(), "[\"a\",\"b\",\"c\",\"d\"]")
          .ValueOrDie();
  auto map_type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto input_map =
      ::arrow::MapArray::FromArrays(map_type, offsets, keys, values).ValueOrDie();
  auto output_map = Roundtrip(input_map, int64(), 1, 3);
  ASSERT_NE(output_map, nullptr);
  ExpectVariantsEqual(*values,
                      *static_cast<const ::arrow::MapArray&>(*output_map).items());
}

TEST_F(ParquetVariantTest, CorruptValuesReturnStatus) {
  auto type = ArrowSchemaFor(*schema_)->field(0)->type();
  auto metadata =
      ::arrow::json::ArrayFromJSONString(::arrow::binary(), "[\"invalid\"]").ValueOrDie();
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::binary(), "[\"invalid\"]").ValueOrDie();
  auto storage_type = static_cast<const ::arrow::ExtensionType&>(*type).storage_type();
  auto storage =
      ::arrow::StructArray::Make({metadata, values}, storage_type->fields()).ValueOrDie();
  auto array = ::arrow::ExtensionType::WrapArray(type, storage);
  ParquetWriter writer;
  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ASSERT_THAT(writer.Open({.path = CreateNewTempFilePathWithSuffix(".parquet"),
                           .schema = schema_,
                           .io = io_,
                           .properties = std::move(writer_properties)}),
              IsOk());
  ArrowArray c_array;
  auto batch = ::arrow::RecordBatch::Make(ArrowSchemaFor(*schema_), 1, {array});
  ASSERT_TRUE(::arrow::ExportRecordBatch(*batch, &c_array).ok());
  EXPECT_FALSE(writer.Write(&c_array).has_value());
  EXPECT_EQ(c_array.release, nullptr);
  EXPECT_THAT(writer.Close(), IsOk());

  // Write a malformed file directly through Arrow with validation disabled,
  // then check that Iceberg's reader validates it and returns a Status.
  auto path = CreateNewTempFilePathWithSuffix(".parquet");
  auto stream = arrow::OpenArrowOutputStream(io_, path).value();
  auto properties = ::parquet::ArrowWriterProperties::Builder()
                        .set_variant_validation_enabled(false)
                        ->build();
  auto unchecked = ::parquet::arrow::FileWriter::Open(
                       *batch->schema(), ::arrow::default_memory_pool(), stream,
                       ::parquet::default_writer_properties(), properties)
                       .ValueOrDie();
  ASSERT_TRUE(unchecked->WriteRecordBatch(*batch).ok());
  ASSERT_TRUE(unchecked->Close().ok());
  ASSERT_TRUE(stream->Close().ok());
  ParquetReader reader;
  ASSERT_THAT(reader.Open({.path = path, .io = io_, .projection = schema_}), IsOk());
  EXPECT_FALSE(reader.Next().has_value());
  EXPECT_THAT(reader.Close(), IsOk());
}

TEST_F(ParquetVariantTest, ReadTypedOnlyExternalLayout) {
  VariantArrayBuilder builder;
  builder.AppendInt64(42);
  builder.AppendInt64(-7);
  builder.AppendNull();
  auto input =
      CanonicalVariant(builder.Finish(), ArrowSchemaFor(*schema_)->field(0)->type());
  const auto& storage = static_cast<const ::arrow::StructArray&>(
      *static_cast<const ::arrow::ExtensionArray&>(*input).storage());
  const auto& metadata = static_cast<const ::arrow::BinaryArray&>(*storage.field(0));
  for (int32_t field_id : {1, -1}) {
    SCOPED_TRACE(field_id);
    auto path = CreateNewTempFilePathWithSuffix(".parquet");
    auto stream = arrow::OpenArrowOutputStream(io_, path).value();
    auto metadata_node = ::parquet::schema::PrimitiveNode::Make(
        "metadata", ::parquet::Repetition::REQUIRED, ::parquet::Type::BYTE_ARRAY);
    auto typed_node = ::parquet::schema::PrimitiveNode::Make(
        "typed_value", ::parquet::Repetition::OPTIONAL, ::parquet::Type::INT64);
    auto variant_node = ::parquet::schema::GroupNode::Make(
        "payload", ::parquet::Repetition::OPTIONAL, {metadata_node, typed_node},
        ::parquet::LogicalType::Variant(), field_id);
    auto root = ::parquet::schema::GroupNode::Make(
        "schema", ::parquet::Repetition::REQUIRED, {variant_node});
    auto writer = ::parquet::ParquetFileWriter::Open(
        stream, std::static_pointer_cast< ::parquet::schema::GroupNode>(root));
    auto row_group = writer->AppendRowGroup();
    int16_t metadata_defs[] = {1, 1, 0};
    int16_t typed_defs[] = {2, 2, 0};
    ::parquet::ByteArray encoded_metadata[] = {
        ::parquet::ByteArray(
            static_cast<uint32_t>(metadata.GetView(0).size()),
            reinterpret_cast<const uint8_t*>(metadata.GetView(0).data())),
        ::parquet::ByteArray(
            static_cast<uint32_t>(metadata.GetView(1).size()),
            reinterpret_cast<const uint8_t*>(metadata.GetView(1).data()))};
    int64_t integers[] = {42, -7};
    auto metadata_writer =
        static_cast< ::parquet::ByteArrayWriter*>(row_group->NextColumn());
    EXPECT_EQ(metadata_writer->WriteBatch(3, metadata_defs, nullptr, encoded_metadata),
              2);
    metadata_writer->Close();
    auto typed_writer = static_cast< ::parquet::Int64Writer*>(row_group->NextColumn());
    EXPECT_EQ(typed_writer->WriteBatch(3, typed_defs, nullptr, integers), 2);
    typed_writer->Close();
    row_group->Close();
    writer->Close();
    ASSERT_TRUE(stream->Close().ok());

    ParquetReader reader;
    auto mapping = CreateMapping(*schema_).value();
    ASSERT_THAT(reader.Open({.path = path,
                             .io = io_,
                             .projection = schema_,
                             .name_mapping = std::move(mapping)}),
                IsOk());
    auto next = reader.Next();
    ASSERT_THAT(next, IsOk());
    ASSERT_TRUE(next->has_value());
    auto c_array = next->value();
    auto batch =
        ::arrow::ImportRecordBatch(&c_array, ArrowSchemaFor(*schema_)).ValueOrDie();
    ExpectVariantsEqual(*input, *batch->column(0));
    EXPECT_THAT(reader.Close(), IsOk());
  }
}

TEST_F(ParquetVariantTest, BatchFilteringPreservesExtensionAndNulls) {
  VariantArrayBuilder builder;
  builder.AppendInt64(42);
  builder.AppendNull();
  builder.AppendVariantNull();
  auto arrow_schema = ArrowSchemaFor(*schema_);
  auto input = CanonicalVariant(builder.Finish(), arrow_schema->field(0)->type());
  int32_t rows[] = {2, 0, 1};
  for (auto function : {ProjectionContext::ProjectBatchFunction{nullptr},
                        ProjectionContext::ResolveProjectBatchFunction()}) {
    auto projection = ProjectionContext::Make(*schema_, *schema_, function);
    ASSERT_THAT(projection, IsOk());
    auto batch = ::arrow::RecordBatch::Make(arrow_schema, 3, {input});
    ArrowArray c_array;
    ASSERT_TRUE(::arrow::ExportRecordBatch(*batch, &c_array).ok());
    auto projected = ProjectBatch(&c_array, rows, *projection);
    ASSERT_THAT(projected, IsOk());
    auto output_c_array = projected.value();
    auto output = ::arrow::ImportRecordBatch(&output_c_array, arrow_schema).ValueOrDie();
    ExpectVariantsEqual(*input->Slice(2, 1), *output->column(0)->Slice(0, 1));
    ExpectVariantsEqual(*input->Slice(0, 2), *output->column(0)->Slice(1, 2));
  }
}

TEST_F(ParquetVariantTest, InvalidTargetsFailBeforeCreatingFile) {
  for (auto targets :
       {std::unordered_map<int32_t, std::shared_ptr<Type> >{{999, int64()}},
        std::unordered_map<int32_t, std::shared_ptr<Type> >{{1, nullptr}},
        std::unordered_map<int32_t, std::shared_ptr<Type> >{{1, variant()}},
        std::unordered_map<int32_t, std::shared_ptr<Type> >{{1, unknown()}}}) {
    auto path = CreateNewTempFilePathWithSuffix(".parquet");
    ParquetWriter writer;
    EXPECT_THAT(writer.Open({.path = path,
                             .schema = schema_,
                             .io = io_,
                             .variant_shredding_types = targets}),
                IsError(ErrorKind::kInvalidArgument));
    EXPECT_FALSE(std::filesystem::exists(path));
  }
}

#endif

}  // namespace iceberg::parquet
