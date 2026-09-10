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

#include <array>
#include <format>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/builder_binary.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/extension_type.h>
#include <arrow/filesystem/mockfs.h>
#include <arrow/json/from_string.h>
#include <arrow/type.h>
#include <arrow/util/logging.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/type.h"
#include "iceberg/update/update_schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

struct DefaultValueEndToEndParam {
  std::string name;
  FileFormatType format;
  std::shared_ptr<Type> type;
  Literal default_value;
  // Builds the two-row column the added field is expected to read back as. A factory
  // rather than JSON so extension types (uuid) can be constructed explicitly.
  std::function<std::shared_ptr<::arrow::Array>()> expected_column;
  // Avro reads through one of two backends depending on this; unused for Parquet.
  bool avro_skip_datum = true;
};

class DefaultValueEndToEndTest
    : public UpdateTestBase,
      public ::testing::WithParamInterface<DefaultValueEndToEndParam> {
 protected:
  static void SetUpTestSuite() {
    parquet::RegisterAll();
    avro::RegisterAll();
  }

  std::string MetadataResource() const override {
    return "TableMetadataV3ValidMinimal.json";
  }
};

TEST_P(DefaultValueEndToEndTest, WriteEvolveReadFillsInitialDefault) {
  const auto& param = GetParam();
  ICEBERG_UNWRAP_OR_FAIL(auto original_schema, table_->schema());

  ArrowSchema arrow_c_schema;
  ASSERT_THAT(ToArrowSchema(*original_schema, &arrow_c_schema), IsOk());
  auto arrow_type = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();
  auto array = ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_type->fields()),
                                                  R"([[1, 2, 3],[4, 5, 6]])")
                   .ValueOrDie();

  // Arrow's mock filesystem does not create directories on demand.
  auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
      static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
  ASSERT_TRUE(arrow_fs != nullptr);
  ASSERT_TRUE(arrow_fs->CreateDir(table_location_ + "/data").ok());

  const std::string data_file =
      std::format("{}/data/evolve-default-{}", table_location_, param.name);
  WriterProperties writer_properties;
  writer_properties.Set(WriterProperties::kParquetCompression,
                        std::string("uncompressed"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      WriterFactoryRegistry::Open(param.format, {.path = data_file,
                                                 .schema = original_schema,
                                                 .io = file_io_,
                                                 .properties = writer_properties}));
  ArrowArray exported;
  ASSERT_TRUE(::arrow::ExportArray(*array, &exported).ok());
  ASSERT_THAT(writer->Write(&exported), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto update, table_->NewUpdateSchema());
  update->AddColumn("added", param.type, "Added after the data was written",
                    param.default_value);
  ASSERT_THAT(update->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto reloaded, catalog_->LoadTable(table_ident_));
  ICEBERG_UNWRAP_OR_FAIL(auto evolved_schema, reloaded->schema());
  ICEBERG_UNWRAP_OR_FAIL(auto added_field, evolved_schema->FindFieldByName("added"));
  ASSERT_TRUE(added_field.has_value());
  // AddColumn sets both defaults, so evolution must carry both values through the commit
  // and the metadata round trip.
  ASSERT_NE(added_field->get().initial_default(), nullptr);
  ASSERT_NE(added_field->get().write_default(), nullptr);
  EXPECT_EQ(*added_field->get().initial_default(), param.default_value);
  EXPECT_EQ(*added_field->get().write_default(), param.default_value);

  ReaderProperties reader_properties;
  reader_properties.Set(ReaderProperties::kAvroSkipDatum, param.avro_skip_datum);
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(param.format, {.path = data_file,
                                                 .io = file_io_,
                                                 .projection = evolved_schema,
                                                 .properties = reader_properties}));

  // The reader reports the evolved schema, including the added column.
  ICEBERG_UNWRAP_OR_FAIL(auto reported_c_schema, reader->Schema());
  auto reported_schema = ::arrow::ImportSchema(&reported_c_schema).ValueOrDie();
  ASSERT_EQ(reported_schema->num_fields(), 4);
  EXPECT_EQ(reported_schema->field(3)->name(), "added");
  ICEBERG_UNWRAP_OR_FAIL(auto batch, reader->Next());
  ASSERT_TRUE(batch.has_value());

  ArrowSchema read_c_schema;
  ASSERT_THAT(ToArrowSchema(*evolved_schema, &read_c_schema), IsOk());
  auto read_type = ::arrow::ImportType(&read_c_schema).ValueOrDie();
  auto actual = ::arrow::ImportArray(&batch.value(), read_type).ValueOrDie();
  auto actual_struct = std::static_pointer_cast<::arrow::StructArray>(actual);
  ASSERT_EQ(actual_struct->length(), 2);

  // The pre-existing columns are unchanged.
  auto expected_existing = ::arrow::json::ArrayFromJSONString(
                               ::arrow::struct_({read_type->field(0), read_type->field(1),
                                                 read_type->field(2)}),
                               R"([[1, 2, 3],[4, 5, 6]])")
                               .ValueOrDie();
  auto actual_existing =
      ::arrow::StructArray::Make(
          {actual_struct->field(0), actual_struct->field(1), actual_struct->field(2)},
          {read_type->field(0), read_type->field(1), read_type->field(2)})
          .ValueOrDie();
  ASSERT_TRUE(actual_existing->Equals(*expected_existing))
      << "actual: " << actual_existing->ToString()
      << "\nexpected: " << expected_existing->ToString();

  // The added column materializes its initial-default for every row.
  auto expected_added = param.expected_column();
  auto actual_added = actual_struct->field(3);
  ASSERT_TRUE(actual_added->Equals(*expected_added))
      << "actual: " << actual_added->ToString()
      << "\nexpected: " << expected_added->ToString();

  // The file held exactly those two rows.
  ICEBERG_UNWRAP_OR_FAIL(auto next_batch, reader->Next());
  ASSERT_FALSE(next_batch.has_value());
}

namespace {

// Two-row column of `json` values at `type`, for the simple cases.
std::function<std::shared_ptr<::arrow::Array>()> Rows(
    std::shared_ptr<::arrow::DataType> type, std::string json) {
  return [type = std::move(type), json = std::move(json)]() {
    return ::arrow::json::ArrayFromJSONString(type, json).ValueOrDie();
  };
}

constexpr std::array<uint8_t, 16> kUuidBytes = {0xF7, 0x9C, 0x3E, 0x09, 0x67, 0x7C,
                                                0x4B, 0xBD, 0x90, 0x38, 0x1E, 0x7F,
                                                0x1A, 0x0C, 0x8B, 0x2D};

std::shared_ptr<::arrow::Array> UuidRows() {
  // An Iceberg uuid surfaces in Arrow as the `arrow.uuid` extension type over
  // fixed_size_binary(16), so the storage array is built then wrapped.
  ::arrow::FixedSizeBinaryBuilder builder(::arrow::fixed_size_binary(Uuid::kLength));
  ARROW_CHECK_OK(builder.Append(kUuidBytes.data()));
  ARROW_CHECK_OK(builder.Append(kUuidBytes.data()));
  return ::arrow::ExtensionType::WrapArray(::arrow::extension::uuid(),
                                           builder.Finish().ValueOrDie());
}

}  // namespace

INSTANTIATE_TEST_SUITE_P(
    FormatsAndTypes, DefaultValueEndToEndTest,
    ::testing::Values(
        DefaultValueEndToEndParam{"parquet_long", FileFormatType::kParquet, int64(),
                                  Literal::Long(42), Rows(::arrow::int64(), "[42, 42]")},
        DefaultValueEndToEndParam{"avro_long", FileFormatType::kAvro, int64(),
                                  Literal::Long(42), Rows(::arrow::int64(), "[42, 42]")},
        DefaultValueEndToEndParam{"parquet_string", FileFormatType::kParquet, string(),
                                  Literal::String("iceberg"),
                                  Rows(::arrow::utf8(), R"(["iceberg", "iceberg"])")},
        DefaultValueEndToEndParam{"avro_string", FileFormatType::kAvro, string(),
                                  Literal::String("iceberg"),
                                  Rows(::arrow::utf8(), R"(["iceberg", "iceberg"])")},
        DefaultValueEndToEndParam{
            "parquet_decimal", FileFormatType::kParquet, decimal(9, 2),
            Literal::Decimal(12345, 9, 2),
            Rows(::arrow::decimal128(9, 2), R"(["123.45", "123.45"])")},
        DefaultValueEndToEndParam{
            "avro_decimal", FileFormatType::kAvro, decimal(9, 2),
            Literal::Decimal(12345, 9, 2),
            Rows(::arrow::decimal128(9, 2), R"(["123.45", "123.45"])")},
        DefaultValueEndToEndParam{"parquet_timestamp", FileFormatType::kParquet,
                                  timestamp(), Literal::Timestamp(1672531200000000),
                                  Rows(::arrow::timestamp(::arrow::TimeUnit::MICRO),
                                       "[1672531200000000, 1672531200000000]")},
        DefaultValueEndToEndParam{"avro_timestamp", FileFormatType::kAvro, timestamp(),
                                  Literal::Timestamp(1672531200000000),
                                  Rows(::arrow::timestamp(::arrow::TimeUnit::MICRO),
                                       "[1672531200000000, 1672531200000000]")},
        DefaultValueEndToEndParam{"parquet_uuid", FileFormatType::kParquet, uuid(),
                                  Literal::UUID(Uuid::FromBytes(kUuidBytes).value()),
                                  UuidRows},
        DefaultValueEndToEndParam{"avro_uuid", FileFormatType::kAvro, uuid(),
                                  Literal::UUID(Uuid::FromBytes(kUuidBytes).value()),
                                  UuidRows},
        // The Avro reader has two backends; the cases above use the default
        // (skip-datum), so cover the GenericDatum path too.
        DefaultValueEndToEndParam{"avro_long_generic_datum", FileFormatType::kAvro,
                                  int64(), Literal::Long(42),
                                  Rows(::arrow::int64(), "[42, 42]"),
                                  /*avro_skip_datum=*/false},
        DefaultValueEndToEndParam{"avro_uuid_generic_datum", FileFormatType::kAvro,
                                  uuid(),
                                  Literal::UUID(Uuid::FromBytes(kUuidBytes).value()),
                                  UuidRows, /*avro_skip_datum=*/false}),
    [](const ::testing::TestParamInfo<DefaultValueEndToEndParam>& info) {
      return info.param.name;
    });

}  // namespace

}  // namespace iceberg
