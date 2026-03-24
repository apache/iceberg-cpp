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

#pragma once

#include <memory>
#include <optional>
#include <string>

#include <arrow/array.h>

#include "iceberg/file_io.h"
#include "iceberg/metrics.h"
#include "iceberg/metrics_config.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"

namespace iceberg::test {

/// \brief Base test class for metrics testing, similar to Java's TestMetrics
///
/// This class provides common test infrastructure and helper methods for testing
/// metrics collection across different file formats (Parquet, Avro, ORC).
class MetricsTestBase {
 protected:
  virtual void SetUp();

  /// \brief Get metrics for the given schema and records
  virtual Result<Metrics> GetMetrics(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<::arrow::Array> records) = 0;

  /// \brief Get metrics with custom MetricsConfig
  virtual Result<Metrics> GetMetrics(std::shared_ptr<Schema> schema,
                                     std::shared_ptr<MetricsConfig> config,
                                     std::shared_ptr<::arrow::Array> records) = 0;

  /// \brief Create an output file for testing
  virtual std::string CreateOutputFile() = 0;

  /// \brief Get the number of row groups/splits in a file
  virtual Result<int> GetSplitCount() = 0;

  /// \brief Whether the format supports small row groups for testing
  virtual bool SupportsSmallRowGroups() const { return false; }

  // Helper methods for assertions
  void AssertCounts(int field_id, std::optional<int64_t> expected_value_count,
                    std::optional<int64_t> expected_null_count, const Metrics& metrics);

  void AssertCounts(int field_id, std::optional<int64_t> expected_value_count,
                    std::optional<int64_t> expected_null_count,
                    std::optional<int64_t> expected_nan_count, const Metrics& metrics);

  template <typename T>
  void AssertBounds(int field_id, std::shared_ptr<PrimitiveType> type,
                    std::optional<T> expected_lower, std::optional<T> expected_upper,
                    const Metrics& metrics);

  // Helper methods for creating test data
  std::shared_ptr<::arrow::Array> CreateRecordArrays(
      const std::shared_ptr<::arrow::Schema>& arrow_schema, const std::string& json_data);

  // Common test schemas
  static std::shared_ptr<Schema> SimpleSchema();
  static std::shared_ptr<Schema> NestedSchema();
  static std::shared_ptr<Schema> FloatDoubleSchema();

  // Test case methods - subclasses should call these from TEST_F macros
  void MetricsForRepeatedValues();
  void MetricsForTopLevelFields();
  void MetricsForDecimals();
  void MetricsForNestedStructFields();
  void MetricsModeForNestedStructFields();
  void MetricsForListAndMapElements();
  void MetricsForNullColumns();
  void MetricsForNaNColumns();
  void ColumnBoundsWithNaNValueAtFront();
  void ColumnBoundsWithNaNValueInMiddle();
  void ColumnBoundsWithNaNValueAtEnd();
  void MetricsForTopLevelWithMultipleRowGroup();
  void MetricsForNestedStructFieldsWithMultipleRowGroup();
  void NoneMetricsMode();
  void CountsMetricsMode();
  void FullMetricsMode();
  void TruncateStringMetricsMode();
  void TruncateBinaryMetricsMode();

 private:
  Result<std::shared_ptr<::arrow::Array>> BuildSimpleRecords(
      std::shared_ptr<::arrow::Schema> arrow_schema, int32_t count = 1);
  Result<std::shared_ptr<::arrow::Array>> BuildNestedRecords(int32_t count = 1);

 protected:
  std::shared_ptr<FileIO> file_io_;
  std::string temp_dir_;
  std::string path_;
};

#define DEFINE_METRICS_TEST_CASE(TestClass, Case) \
  TEST_F(TestClass, Case) { Case(); }

#define DEFINE_METRICS_TESTS(TestClass)                                       \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForRepeatedValues)               \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForTopLevelFields)               \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForNestedStructFields)           \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForNullColumns)                  \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForNaNColumns)                   \
  DEFINE_METRICS_TEST_CASE(TestClass, ColumnBoundsWithNaNValueAtFront)        \
  DEFINE_METRICS_TEST_CASE(TestClass, ColumnBoundsWithNaNValueInMiddle)       \
  DEFINE_METRICS_TEST_CASE(TestClass, ColumnBoundsWithNaNValueAtEnd)          \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForDecimals)                     \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForListAndMapElements)           \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsModeForNestedStructFields)       \
  DEFINE_METRICS_TEST_CASE(TestClass, NoneMetricsMode)                        \
  DEFINE_METRICS_TEST_CASE(TestClass, CountsMetricsMode)                      \
  DEFINE_METRICS_TEST_CASE(TestClass, FullMetricsMode)                        \
  DEFINE_METRICS_TEST_CASE(TestClass, TruncateStringMetricsMode)              \
  DEFINE_METRICS_TEST_CASE(TestClass, TruncateBinaryMetricsMode)              \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForTopLevelWithMultipleRowGroup) \
  DEFINE_METRICS_TEST_CASE(TestClass, MetricsForNestedStructFieldsWithMultipleRowGroup)

}  // namespace iceberg::test
