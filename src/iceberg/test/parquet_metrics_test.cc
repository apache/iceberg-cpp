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

#include <arrow/c/bridge.h>
#include <gtest/gtest.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/test/metrics_test_base.h"
#include "iceberg/util/checked_cast.h"

namespace iceberg::test {

class ParquetMetricsTest : public MetricsTestBase, public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    MetricsTestBase::SetUp();
    temp_parquet_file_ = "parquet_metrics_test.parquet";
    writer_properties_ = WriterProperties::FromMap(
        {{WriterProperties::kParquetCompression.key(), "uncompressed"}});
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

  bool SupportsSmallRowGroups() const override { return false; }

 private:
  std::string temp_parquet_file_;
  WriterProperties writer_properties_;
};

DEFINE_METRICS_TESTS(ParquetMetricsTest);

}  // namespace iceberg::test
