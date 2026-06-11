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

#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/filesystem/filesystem.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_metrics_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/metrics_test_base.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::test {

class ParquetMetricsTest : public MetricsTestBase, public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    MetricsTestBase::SetUp();
    temp_parquet_file_ = "parquet_metrics_test.parquet";
    writer_properties_ = WriterProperties::FromMap({
        {WriterProperties::kParquetCompression.key(), "uncompressed"},
        {WriterProperties::kParquetMaxRowGroupRows.key(), "100"},
    });
  }

  Result<Metrics> GetMetrics(std::shared_ptr<Schema> schema,
                             std::shared_ptr<::arrow::Array> records) override {
    return GetMetrics(schema, MetricsConfig::Default(), records);
  }

  Result<Metrics> GetMetrics(std::shared_ptr<Schema> schema,
                             std::shared_ptr<MetricsConfig> config,
                             std::shared_ptr<::arrow::Array> records) override {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, WriterFactoryRegistry::Open(FileFormatType::kParquet,
                                                 {.path = temp_parquet_file_,
                                                  .schema = schema,
                                                  .io = file_io_,
                                                  .metadata = {},
                                                  .metrics_config = config,
                                                  .properties = writer_properties_}));
    ArrowArray arr;
    ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportArray(*records, &arr));
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&arr));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->metrics();
  }

  std::string CreateOutputFile() override { return temp_parquet_file_; }

  Result<int> GetSplitCount() override {
    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto infile = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
    auto metadata = ::parquet::ReadMetaData(infile);
    return metadata->num_row_groups();
  }

  Result<Metrics> GetMetricsWithFieldMetrics(
      std::shared_ptr<Schema> schema, std::shared_ptr<MetricsConfig> config,
      std::shared_ptr<::arrow::Array> records,
      const std::unordered_map<int32_t, FieldMetrics>& field_metrics) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, WriterFactoryRegistry::Open(FileFormatType::kParquet,
                                                 {.path = temp_parquet_file_,
                                                  .schema = schema,
                                                  .io = file_io_,
                                                  .metadata = {},
                                                  .metrics_config = config,
                                                  .properties = writer_properties_}));
    ArrowArray arr;
    ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportArray(*records, &arr));
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&arr));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());

    auto io = internal::checked_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
    auto infile = io.fs()->OpenInputFile(temp_parquet_file_).ValueOrDie();
    auto metadata = ::parquet::ReadMetaData(infile);
    return parquet::ParquetMetrics::GetMetrics(*schema, *metadata->schema(), *config,
                                               *metadata, field_metrics);
  }

  bool SupportsSmallRowGroups() const override { return true; }
  bool ReportsNanCounts() const override { return true; }

 private:
  std::string temp_parquet_file_;
  WriterProperties writer_properties_;
};

DEFINE_METRICS_TESTS(ParquetMetricsTest);

TEST_F(ParquetMetricsTest, FieldMetricsOverrideFooterStats) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "floatCol", float32()),
      SchemaField::MakeOptional(2, "strCol", string()),
  });
  auto arrow_schema = ::arrow::schema({
      ::arrow::field("floatCol", ::arrow::float32(), true),
      ::arrow::field("strCol", ::arrow::utf8(), true),
  });
  auto records = CreateRecordArrays(arrow_schema, R"([
    {"floatCol": 1.0, "strCol": "footer-value"}
  ])");
  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "truncate(3)"}};
  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));

  std::unordered_map<int32_t, FieldMetrics> field_metrics{
      {1, FieldMetrics{.field_id = 1,
                       .value_count = 10,
                       .null_value_count = 2,
                       .lower_bound = Literal::Float(5.0F),
                       .upper_bound = Literal::Float(9.0F)}},
      {2, FieldMetrics{.field_id = 2,
                       .value_count = 10,
                       .null_value_count = 1,
                       .lower_bound = Literal::String("abcdef"),
                       .upper_bound = Literal::String("abcxyz")}},
  };

  ICEBERG_UNWRAP_OR_FAIL(
      auto metrics, GetMetricsWithFieldMetrics(schema, config, records, field_metrics));

  AssertCounts(1, 10, 2, metrics);
  AssertBounds<float>(1, float32(), 5.0F, 9.0F, metrics);
  AssertCounts(2, 10, 1, metrics);
  AssertBounds<std::string>(2, string(), std::string("abc"), std::string("abd"), metrics);
}

TEST_F(ParquetMetricsTest, UnrepresentableTruncatedUpperBoundIsOmitted) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "binCol", binary()),
  });
  auto arrow_schema = ::arrow::schema({
      ::arrow::field("binCol", ::arrow::binary(), false),
  });

  ::arrow::BinaryBuilder builder;
  std::vector<uint8_t> data = {0xFF, 0xFF, 0x01};
  ASSERT_TRUE(builder.Append(data.data(), data.size()).ok());
  auto array = builder.Finish().ValueOrDie();
  auto records = ::arrow::StructArray::Make({array}, arrow_schema->fields()).ValueOrDie();
  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "truncate(2)"}};
  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  AssertCounts(1, 1, 0, metrics);
  AssertBounds<std::vector<uint8_t>>(1, binary(), std::vector<uint8_t>({0xFF, 0xFF}),
                                     std::nullopt, metrics);
}

}  // namespace iceberg::test
