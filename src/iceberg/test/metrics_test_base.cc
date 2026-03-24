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

#include "iceberg/test/metrics_test_base.h"

#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/decimal.h"

namespace iceberg::test {

void MetricsTestBase::SetUp() {
  file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
  temp_dir_ = "metrics_test";
}

void MetricsTestBase::AssertCounts(int field_id,
                                   std::optional<int64_t> expected_value_count,
                                   std::optional<int64_t> expected_null_count,
                                   const Metrics& metrics) {
  if (expected_value_count.has_value()) {
    ASSERT_TRUE(metrics.value_counts.contains(field_id))
        << "Field " << field_id << " should have value count";
    EXPECT_EQ(metrics.value_counts.at(field_id), expected_value_count.value())
        << "Field " << field_id << " value count mismatch";
  } else {
    EXPECT_FALSE(metrics.value_counts.contains(field_id))
        << "Field " << field_id << " should not have value count";
  }

  if (expected_null_count.has_value()) {
    ASSERT_TRUE(metrics.null_value_counts.contains(field_id))
        << "Field " << field_id << " should have null count";
    EXPECT_EQ(metrics.null_value_counts.at(field_id), expected_null_count.value())
        << "Field " << field_id << " null count mismatch";
  } else {
    EXPECT_FALSE(metrics.null_value_counts.contains(field_id))
        << "Field " << field_id << " should not have null count";
  }
}

void MetricsTestBase::AssertCounts(int field_id,
                                   std::optional<int64_t> expected_value_count,
                                   std::optional<int64_t> expected_null_count,
                                   std::optional<int64_t> expected_nan_count,
                                   const Metrics& metrics) {
  AssertCounts(field_id, expected_value_count, expected_null_count, metrics);

  if (expected_nan_count.has_value()) {
    ASSERT_TRUE(metrics.nan_value_counts.contains(field_id))
        << "Field " << field_id << " should have NaN count";
    EXPECT_EQ(metrics.nan_value_counts.at(field_id), expected_nan_count.value())
        << "Field " << field_id << " NaN count mismatch";
  } else {
    EXPECT_FALSE(metrics.nan_value_counts.contains(field_id))
        << "Field " << field_id << " should not have NaN count";
  }
}

template <typename T>
void MetricsTestBase::AssertBounds(int field_id, std::shared_ptr<PrimitiveType> type,
                                   std::optional<T> expected_lower,
                                   std::optional<T> expected_upper,
                                   const Metrics& metrics) {
  if (expected_lower.has_value()) {
    ASSERT_TRUE(metrics.lower_bounds.contains(field_id))
        << "Field " << field_id << " should have lower bound";
    const auto& literal = metrics.lower_bounds.at(field_id);
    ASSERT_FALSE(literal.IsNull())
        << "Field " << field_id << " lower bound literal should not be null";
    EXPECT_EQ(std::get<T>(literal.value()), expected_lower.value())
        << "Field " << field_id << " lower bound mismatch";
  } else {
    EXPECT_FALSE(metrics.lower_bounds.contains(field_id));
  }

  if (expected_upper.has_value()) {
    ASSERT_TRUE(metrics.upper_bounds.contains(field_id))
        << "Field " << field_id << " should have upper bound";
    const auto& literal = metrics.upper_bounds.at(field_id);
    ASSERT_FALSE(literal.IsNull())
        << "Field " << field_id << " upper bound literal should not be null";
    EXPECT_EQ(std::get<T>(literal.value()), expected_upper.value())
        << "Field " << field_id << " upper bound mismatch";
  } else {
    EXPECT_FALSE(metrics.upper_bounds.contains(field_id));
  }
}

// Explicit template instantiations for common types
template void MetricsTestBase::AssertBounds<bool>(int, std::shared_ptr<PrimitiveType>,
                                                  std::optional<bool>,
                                                  std::optional<bool>, const Metrics&);
template void MetricsTestBase::AssertBounds<int32_t>(int, std::shared_ptr<PrimitiveType>,
                                                     std::optional<int32_t>,
                                                     std::optional<int32_t>,
                                                     const Metrics&);
template void MetricsTestBase::AssertBounds<int64_t>(int, std::shared_ptr<PrimitiveType>,
                                                     std::optional<int64_t>,
                                                     std::optional<int64_t>,
                                                     const Metrics&);
template void MetricsTestBase::AssertBounds<float>(int, std::shared_ptr<PrimitiveType>,
                                                   std::optional<float>,
                                                   std::optional<float>, const Metrics&);
template void MetricsTestBase::AssertBounds<double>(int, std::shared_ptr<PrimitiveType>,
                                                    std::optional<double>,
                                                    std::optional<double>,
                                                    const Metrics&);
template void MetricsTestBase::AssertBounds<std::string>(int,
                                                         std::shared_ptr<PrimitiveType>,
                                                         std::optional<std::string>,
                                                         std::optional<std::string>,
                                                         const Metrics&);
template void MetricsTestBase::AssertBounds<std::vector<uint8_t>>(
    int, std::shared_ptr<PrimitiveType>, std::optional<std::vector<uint8_t>>,
    std::optional<std::vector<uint8_t>>, const Metrics&);

template void MetricsTestBase::AssertBounds<Decimal>(int, std::shared_ptr<PrimitiveType>,
                                                     std::optional<Decimal>,
                                                     std::optional<Decimal>,
                                                     const Metrics&);

std::shared_ptr<::arrow::Array> MetricsTestBase::CreateRecordArrays(
    const std::shared_ptr<::arrow::Schema>& arrow_schema, const std::string& json_data) {
  auto struct_type = ::arrow::struct_(arrow_schema->fields());
  return ::arrow::json::ArrayFromJSONString(struct_type, json_data).ValueOrDie();
}

std::shared_ptr<Schema> MetricsTestBase::SimpleSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "booleanCol", boolean()),
      SchemaField::MakeRequired(2, "intCol", int32()),
      SchemaField::MakeOptional(3, "longCol", int64()),
      SchemaField::MakeRequired(4, "floatCol", float32()),
      SchemaField::MakeOptional(5, "doubleCol", float64()),
      SchemaField::MakeOptional(6, "decimalCol", decimal(10, 2)),
      SchemaField::MakeRequired(7, "stringCol", string()),
      SchemaField::MakeOptional(8, "dateCol", date()),
      SchemaField::MakeRequired(9, "timeCol", time()),
      SchemaField::MakeRequired(10, "timestampColAboveEpoch", timestamp()),
      SchemaField::MakeRequired(11, "fixedCol", fixed(4)),
      SchemaField::MakeRequired(12, "binaryCol", binary()),
      SchemaField::MakeRequired(13, "timestampColBelowEpoch", timestamp()),
  });
}

std::shared_ptr<Schema> MetricsTestBase::NestedSchema() {
  auto leaf_struct = struct_({
      SchemaField::MakeOptional(5, "leafLongCol", int64()),
      SchemaField::MakeOptional(6, "leafBinaryCol", binary()),
  });

  auto nested_struct = struct_({
      SchemaField::MakeRequired(3, "longCol", int64()),
      SchemaField::MakeRequired(4, "leafStructCol", leaf_struct),
      SchemaField::MakeRequired(7, "doubleCol", float64()),
  });

  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "intCol", int32()),
      SchemaField::MakeRequired(2, "nestedStructCol", nested_struct),
  });
}

std::shared_ptr<Schema> MetricsTestBase::FloatDoubleSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "floatCol", float32()),
      SchemaField::MakeOptional(2, "doubleCol", float64()),
  });
}

Result<std::shared_ptr<::arrow::Schema>> ToArrowSchema(std::shared_ptr<Schema> schema) {
  ArrowSchema c_schema;
  ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema, &c_schema));
  std::shared_ptr<::arrow::Schema> arrow_schema;
  ICEBERG_ARROW_ASSIGN_OR_RETURN(arrow_schema, ::arrow::ImportSchema(&c_schema));
  return arrow_schema;
}

void MetricsTestBase::MetricsForRepeatedValues() {
  auto schema = SimpleSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto arrow_schema, ToArrowSchema(schema));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildSimpleRecords(arrow_schema, 2));
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 2);

  AssertCounts(1, 2, 0, metrics);
  AssertCounts(2, 2, 0, metrics);
  AssertCounts(3, 2, 1, metrics);
  // TODO() Assert NaN metrics
  AssertCounts(4, 2, 0, metrics);  // floatCol has 2 NaN values
  AssertCounts(5, 2, 0, metrics);
  AssertCounts(6, 2, 1, metrics);
  AssertCounts(7, 2, 0, metrics);
  AssertCounts(8, 2, 0, metrics);
  AssertCounts(9, 2, 0, metrics);
  AssertCounts(10, 2, 0, metrics);
  AssertCounts(11, 2, 0, metrics);
  AssertCounts(12, 2, 0, metrics);
  AssertCounts(13, 2, 0, metrics);
}

void MetricsTestBase::MetricsForTopLevelFields() {
  auto schema = SimpleSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto arrow_schema, ToArrowSchema(schema));

  auto records = CreateRecordArrays(arrow_schema, R"([
    {"booleanCol": true, "intCol": 3, "longCol": 5, "floatCol": 2.0, "doubleCol": 2.0,
     "decimalCol": "3.50", "stringCol": "AAA", "dateCol": 1500, "timeCol": 2000,
     "timestampColAboveEpoch": 0, "fixedCol": "abcd", "binaryCol": "S", "timestampColBelowEpoch": -1900300},
    {"booleanCol": false, "intCol": -2147483648, "longCol": null, "floatCol": 1.0, "doubleCol": null,
     "decimalCol": null, "stringCol": "ZZZ", "dateCol": null, "timeCol": 3000,
     "timestampColAboveEpoch": 900, "fixedCol": "abcd", "binaryCol": "W", "timestampColBelowEpoch": -7000}
   ])");

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 2);

  AssertCounts(1, 2, 0, metrics);
  AssertBounds<bool>(1, boolean(), false, true, metrics);
  AssertCounts(2, 2, 0, metrics);
  AssertBounds<int32_t>(2, int32(), std::numeric_limits<int32_t>::min(), 3, metrics);
  AssertCounts(3, 2, 1, metrics);
  AssertBounds<int64_t>(3, int64(), 5, 5, metrics);
  AssertCounts(4, 2, 0, metrics);
  AssertBounds<float>(4, float32(), 1.0F, 2.0F, metrics);
  AssertCounts(5, 2, 1, metrics);
  AssertBounds<double>(5, float64(), 2.0, 2.0, metrics);
  AssertCounts(6, 2L, 1L, metrics);
  AssertBounds<Decimal>(6, std::make_shared<DecimalType>(10, 2), Decimal(350),
                        Decimal(350), metrics);
  AssertCounts(7, 2, 0, metrics);
  AssertBounds<std::string>(7, string(), std::string("AAA"), std::string("ZZZ"), metrics);

  AssertCounts(8, 2, 1, metrics);
  AssertBounds<int32_t>(8, date(), 1500, 1500, metrics);

  AssertCounts(9, 2, 0, metrics);
  AssertBounds<int64_t>(9, time(), 2000, 3000, metrics);

  AssertCounts(10, 2, 0, metrics);
  AssertBounds<int64_t>(10, timestamp(), 0, 900, metrics);

  AssertCounts(11, 2, 0, metrics);
  std::vector<uint8_t> fixed_val = {'a', 'b', 'c', 'd'};
  AssertBounds<std::vector<uint8_t>>(11, fixed(4), fixed_val, fixed_val, metrics);

  AssertCounts(12, 2, 0, metrics);
  AssertBounds<std::vector<uint8_t>>(12, binary(), std::vector<uint8_t>{'S'},
                                     std::vector<uint8_t>{'W'}, metrics);

  AssertCounts(13, 2, 0, metrics);
  AssertBounds<int64_t>(13, timestamp(), -1900300, -7000, metrics);
}

void MetricsTestBase::MetricsForDecimals() {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "decimalAsInt32", decimal(4, 2)),
      SchemaField::MakeRequired(2, "decimalAsInt64", decimal(14, 2)),
      SchemaField::MakeRequired(3, "decimalAsFixed", decimal(22, 2)),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("decimalAsInt32", ::arrow::decimal128(4, 2), false),
      ::arrow::field("decimalAsInt64", ::arrow::decimal128(14, 2), false),
      ::arrow::field("decimalAsFixed", ::arrow::decimal128(22, 2), false),
  });

  // Create decimal values
  ::arrow::Decimal128Builder builder1(::arrow::decimal128(4, 2));
  ::arrow::Decimal128Builder builder2(::arrow::decimal128(14, 2));
  ::arrow::Decimal128Builder builder3(::arrow::decimal128(22, 2));

  // 2.55, 4.75, 5.80
  ASSERT_TRUE(builder1.Append(::arrow::Decimal128("255")).ok());  // 2.55 with scale 2
  ASSERT_TRUE(builder2.Append(::arrow::Decimal128("475")).ok());  // 4.75 with scale 2
  ASSERT_TRUE(builder3.Append(::arrow::Decimal128("580")).ok());  // 5.80 with scale 2

  auto array1 = builder1.Finish().ValueOrDie();
  auto array2 = builder2.Finish().ValueOrDie();
  auto array3 = builder3.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {array1, array2, array3};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  AssertCounts(1, 1, 0, metrics);
  // For decimals, bounds exist but we just check they're present
  EXPECT_TRUE(metrics.lower_bounds.contains(1));
  EXPECT_TRUE(metrics.upper_bounds.contains(1));

  AssertCounts(2, 1, 0, metrics);
  EXPECT_TRUE(metrics.lower_bounds.contains(2));
  EXPECT_TRUE(metrics.upper_bounds.contains(2));

  AssertCounts(3, 1, 0, metrics);
  EXPECT_TRUE(metrics.lower_bounds.contains(3));
  EXPECT_TRUE(metrics.upper_bounds.contains(3));
}

void MetricsTestBase::MetricsForNestedStructFields() {
  auto schema = NestedSchema();

  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords());
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  AssertCounts(1, 1, 0, metrics);
  AssertBounds<int32_t>(1, int32(), std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::min(), metrics);

  AssertCounts(3, 1, 0, metrics);
  AssertBounds<int64_t>(3, int64(), 100, 100, metrics);

  AssertCounts(5, 1, 0, metrics);
  AssertBounds<int64_t>(5, int64(), 20, 20, metrics);

  AssertCounts(6, 1L, 0L, metrics);
  AssertBounds<std::vector<uint8_t>>(6, binary(), std::vector<uint8_t>{'A'},
                                     std::vector<uint8_t>{'A'}, metrics);

  // TODO() Assert NaN metrics
  AssertCounts(7, 1L, 0L, metrics);
  AssertBounds<double>(7, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::MetricsModeForNestedStructFields() {
  auto schema = NestedSchema();

  // Create MetricsConfig with custom column modes
  // Default mode is None, but nestedStructCol.longCol should be Full
  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "none"},
      {"write.metadata.metrics.column.nestedStructCol.longCol", "full"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords());
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // Only field 3 (nestedStructCol.longCol) should have bounds
  EXPECT_EQ(metrics.lower_bounds.size(), 1);
  EXPECT_EQ(metrics.upper_bounds.size(), 1);
  AssertBounds<int64_t>(3, int64(), 100, 100, metrics);
}

void MetricsTestBase::MetricsForListAndMapElements() {
  // Create struct type for map values
  auto leaf_struct = struct_({
      SchemaField::MakeRequired(1, "leafIntCol", int32()),
      SchemaField::MakeOptional(2, "leafStringCol", string()),
  });

  // Create list and map types using constructors directly
  auto list_type = list(SchemaField::MakeRequired(4, "element", int32()));
  auto map_type = map(SchemaField::MakeRequired(6, "key", string()),
                      SchemaField::MakeRequired(7, "value", leaf_struct));

  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(3, "intListCol", list_type),
      SchemaField::MakeOptional(5, "mapCol", map_type),
  });

  // Create Arrow schema
  auto arrow_leaf_struct = ::arrow::struct_({
      ::arrow::field("leafIntCol", ::arrow::int32(), false),
      ::arrow::field("leafStringCol", ::arrow::utf8(), true),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("intListCol",
                     ::arrow::list(::arrow::field("element", ::arrow::int32(), false)),
                     true),
      ::arrow::field("mapCol", ::arrow::map(::arrow::utf8(), arrow_leaf_struct), true),
  });

  // Create list: [10, 11, 12]
  ::arrow::Int32Builder int_builder;
  ASSERT_TRUE(int_builder.Append(10).ok());
  ASSERT_TRUE(int_builder.Append(11).ok());
  ASSERT_TRUE(int_builder.Append(12).ok());
  auto int_array = int_builder.Finish().ValueOrDie();

  ::arrow::ListBuilder list_builder(::arrow::default_memory_pool(),
                                    std::make_shared<::arrow::Int32Builder>());
  ASSERT_TRUE(list_builder.Append().ok());
  auto list_value_builder =
      static_cast<::arrow::Int32Builder*>(list_builder.value_builder());
  ASSERT_TRUE(list_value_builder->Append(10).ok());
  ASSERT_TRUE(list_value_builder->Append(11).ok());
  ASSERT_TRUE(list_value_builder->Append(12).ok());
  auto list_array = list_builder.Finish().ValueOrDie();

  // Create map: {"4" -> {leafIntCol: 1, leafStringCol: "BBB"}}
  // MapArray needs offsets, keys, and items (struct values)
  ::arrow::Int32Builder offset_builder;
  ASSERT_TRUE(offset_builder.Append(0).ok());  // Start offset
  ASSERT_TRUE(offset_builder.Append(1).ok());  // End offset (1 entry)
  auto offsets = offset_builder.Finish().ValueOrDie();

  ::arrow::StringBuilder key_builder;
  ASSERT_TRUE(key_builder.Append("4").ok());
  auto keys = key_builder.Finish().ValueOrDie();

  ::arrow::Int32Builder struct_int_builder;
  ::arrow::StringBuilder struct_str_builder;
  ASSERT_TRUE(struct_int_builder.Append(1).ok());
  ASSERT_TRUE(struct_str_builder.Append("BBB").ok());
  auto struct_int_array = struct_int_builder.Finish().ValueOrDie();
  auto struct_str_array = struct_str_builder.Finish().ValueOrDie();
  auto items =
      ::arrow::StructArray::Make({struct_int_array, struct_str_array},
                                 {::arrow::field("leafIntCol", ::arrow::int32(), false),
                                  ::arrow::field("leafStringCol", ::arrow::utf8(), true)})
          .ValueOrDie();

  auto map_array = ::arrow::MapArray::FromArrays(offsets, keys, items).ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {list_array, map_array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // For list and map elements, metrics should not be collected
  // Field IDs: 1 (leafIntCol), 2 (leafStringCol), 4 (list element), 6 (map key), 7 (map
  // value)
  AssertCounts(1, std::nullopt, std::nullopt, metrics);
  AssertCounts(2, std::nullopt, std::nullopt, metrics);
  AssertCounts(4, std::nullopt, std::nullopt, metrics);
  AssertCounts(6, std::nullopt, std::nullopt, metrics);

  AssertBounds<int32_t>(1, int32(), std::nullopt, std::nullopt, metrics);
  AssertBounds<std::string>(2, string(), std::nullopt, std::nullopt, metrics);
  AssertBounds<int32_t>(4, int32(), std::nullopt, std::nullopt, metrics);
  AssertBounds<std::string>(6, string(), std::nullopt, std::nullopt, metrics);
  ASSERT_FALSE(metrics.lower_bounds.contains(7));
  ASSERT_FALSE(metrics.upper_bounds.contains(7));
}

void MetricsTestBase::MetricsForNullColumns() {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "intCol", int32()),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("intCol", ::arrow::int32(), true),
  });

  auto records = CreateRecordArrays(arrow_schema, R"([
    {"intCol": null},
    {"intCol": null}
  ])");

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 2);
  AssertCounts(1, 2, 2, metrics);
  AssertBounds<int32_t>(1, int32(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::MetricsForNaNColumns() {
  auto schema = FloatDoubleSchema();

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("floatCol", ::arrow::float32(), true),
      ::arrow::field("doubleCol", ::arrow::float64(), true),
  });

  ::arrow::FloatBuilder float_builder;
  ::arrow::DoubleBuilder double_builder;

  ASSERT_TRUE(float_builder.Append(std::numeric_limits<float>::quiet_NaN()).ok());
  ASSERT_TRUE(double_builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());
  ASSERT_TRUE(float_builder.Append(std::numeric_limits<float>::quiet_NaN()).ok());
  ASSERT_TRUE(double_builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());

  auto float_array = float_builder.Finish().ValueOrDie();
  auto double_array = double_builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {float_array, double_array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 2);
  // TODO() Assert NaN metrics
  AssertCounts(1, 2, 0, metrics);
  AssertCounts(2, 2, 0, metrics);

  // When all values are NaN, bounds should not be set
  AssertBounds<float>(1, float32(), std::nullopt, std::nullopt, metrics);
  AssertBounds<double>(2, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::ColumnBoundsWithNaNValueAtFront() {
  auto schema = FloatDoubleSchema();

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("floatCol", ::arrow::float32(), true),
      ::arrow::field("doubleCol", ::arrow::float64(), true),
  });

  ::arrow::FloatBuilder float_builder;
  ::arrow::DoubleBuilder double_builder;

  // NaN, 1.2, 5.6
  ASSERT_TRUE(float_builder.Append(std::numeric_limits<float>::quiet_NaN()).ok());
  ASSERT_TRUE(double_builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());
  ASSERT_TRUE(float_builder.Append(1.2F).ok());
  ASSERT_TRUE(double_builder.Append(3.4).ok());
  ASSERT_TRUE(float_builder.Append(5.6F).ok());
  ASSERT_TRUE(double_builder.Append(7.8).ok());

  auto float_array = float_builder.Finish().ValueOrDie();
  auto double_array = double_builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {float_array, double_array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 3);
  // TODO() Assert NaN metrics
  AssertCounts(1, 3, 0, metrics);
  AssertCounts(2, 3, 0, metrics);

  // Bounds should be computed from non-NaN values
  if (metrics.lower_bounds.contains(1)) {
    AssertBounds<float>(1, float32(), 1.2F, 5.6F, metrics);
    AssertBounds<double>(2, float64(), 3.4, 7.8, metrics);
  }
}

void MetricsTestBase::ColumnBoundsWithNaNValueInMiddle() {
  auto schema = FloatDoubleSchema();

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("floatCol", ::arrow::float32(), true),
      ::arrow::field("doubleCol", ::arrow::float64(), true),
  });

  ::arrow::FloatBuilder float_builder;
  ::arrow::DoubleBuilder double_builder;

  // 1.2, NaN, 5.6
  ASSERT_TRUE(float_builder.Append(1.2F).ok());
  ASSERT_TRUE(double_builder.Append(3.4).ok());
  ASSERT_TRUE(float_builder.Append(std::numeric_limits<float>::quiet_NaN()).ok());
  ASSERT_TRUE(double_builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());
  ASSERT_TRUE(float_builder.Append(5.6F).ok());
  ASSERT_TRUE(double_builder.Append(7.8).ok());

  auto float_array = float_builder.Finish().ValueOrDie();
  auto double_array = double_builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {float_array, double_array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 3);
  AssertCounts(1, 3, 0, metrics);
  AssertCounts(2, 3, 0, metrics);

  if (metrics.lower_bounds.contains(1)) {
    AssertBounds<float>(1, float32(), 1.2F, 5.6F, metrics);
    AssertBounds<double>(2, float64(), 3.4, 7.8, metrics);
  }
}

void MetricsTestBase::ColumnBoundsWithNaNValueAtEnd() {
  auto schema = FloatDoubleSchema();

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("floatCol", ::arrow::float32(), true),
      ::arrow::field("doubleCol", ::arrow::float64(), true),
  });

  ::arrow::FloatBuilder float_builder;
  ::arrow::DoubleBuilder double_builder;

  // 1.2, 5.6, NaN
  ASSERT_TRUE(float_builder.Append(1.2F).ok());
  ASSERT_TRUE(double_builder.Append(3.4).ok());
  ASSERT_TRUE(float_builder.Append(5.6F).ok());
  ASSERT_TRUE(double_builder.Append(7.8).ok());
  ASSERT_TRUE(float_builder.Append(std::numeric_limits<float>::quiet_NaN()).ok());
  ASSERT_TRUE(double_builder.Append(std::numeric_limits<double>::quiet_NaN()).ok());

  auto float_array = float_builder.Finish().ValueOrDie();
  auto double_array = double_builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {float_array, double_array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 3);
  AssertCounts(1, 3, 0, metrics);
  AssertCounts(2, 3, 0, metrics);

  if (metrics.lower_bounds.contains(1)) {
    AssertBounds<float>(1, float32(), 1.2F, 5.6F, metrics);
    AssertBounds<double>(2, float64(), 3.4, 7.8, metrics);
  }
}

void MetricsTestBase::MetricsForTopLevelWithMultipleRowGroup() {
  auto schema = SimpleSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto arrow_schema, ToArrowSchema(schema));

  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildSimpleRecords(arrow_schema, 201));
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  if (SupportsSmallRowGroups()) {
    ICEBERG_UNWRAP_OR_FAIL(auto split_count, GetSplitCount());
    EXPECT_EQ(split_count, 3);
  }

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 201);

  // Verify metrics are collected for top-level fields
  AssertCounts(1, 201, 0, metrics);
  AssertBounds<bool>(1, boolean(), false, true, metrics);
  AssertCounts(2, 201, 0, metrics);
  AssertBounds<int32_t>(2, int32(), 3, 203, metrics);
  AssertCounts(3, 201, 1, metrics);
  AssertBounds<int64_t>(3, int64(), 1, 200, metrics);
  AssertCounts(4, 201, 0, metrics);
  AssertBounds<float>(4, float32(), 2.0F, 201.0F, metrics);
  AssertCounts(5, 201, 0, metrics);
  AssertBounds<double>(5, float64(), 2.0, 201.0, metrics);
  AssertCounts(6, 201L, 1L, metrics);
  AssertBounds<Decimal>(6, std::make_shared<DecimalType>(10, 2), Decimal(101),
                        Decimal(300), metrics);
}

void MetricsTestBase::MetricsForNestedStructFieldsWithMultipleRowGroup() {
  auto schema = NestedSchema();
  ICEBERG_UNWRAP_OR_FAIL(auto arrow_schema, ToArrowSchema(schema));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords(201));
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, records));

  if (SupportsSmallRowGroups()) {
    ICEBERG_UNWRAP_OR_FAIL(auto split_count, GetSplitCount());
    EXPECT_EQ(split_count, 3);
  }
  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 201);

  // Verify metrics for top-level field
  AssertCounts(1, 201, 0, metrics);
  AssertBounds<int32_t>(1, int32(), std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::min() + 200, metrics);

  // Verify metrics for nested struct fields
  AssertCounts(3, 201, 0, metrics);
  AssertBounds<int64_t>(3, int64(), 100, 100 + 200, metrics);

  AssertCounts(5, 201, 0, metrics);
  AssertBounds<int64_t>(5, int64(), 20, 20 + 200, metrics);

  AssertCounts(6, 201, 0L, metrics);
  AssertBounds<std::vector<uint8_t>>(6, binary(), std::vector<uint8_t>{'A'},
                                     std::vector<uint8_t>{'A'}, metrics);

  AssertCounts(7, 201, 0L, metrics);
  AssertBounds<double>(7, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::NoneMetricsMode() {
  auto schema = NestedSchema();

  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "none"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords());
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // In None mode, column_sizes should be empty
  EXPECT_TRUE(metrics.column_sizes.empty());

  // All counts should be null
  AssertCounts(1, std::nullopt, std::nullopt, metrics);
  AssertBounds<int32_t>(1, int32(), std::nullopt, std::nullopt, metrics);
  AssertCounts(3, std::nullopt, std::nullopt, metrics);
  AssertBounds<int64_t>(3, int64(), std::nullopt, std::nullopt, metrics);
  AssertCounts(5, std::nullopt, std::nullopt, metrics);
  AssertBounds<int64_t>(5, int64(), std::nullopt, std::nullopt, metrics);
  AssertCounts(6, std::nullopt, std::nullopt, metrics);
  AssertBounds<std::string>(6, binary(), std::nullopt, std::nullopt, metrics);
  AssertCounts(7, std::nullopt, std::nullopt, metrics);
  AssertBounds<double>(7, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::CountsMetricsMode() {
  auto schema = NestedSchema();

  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "counts"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords());
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // In Counts mode, column_sizes should not be empty
  EXPECT_FALSE(metrics.column_sizes.empty());

  // Counts should be present but bounds should be null
  AssertCounts(1, 1, 0, metrics);
  AssertBounds<int32_t>(1, int32(), std::nullopt, std::nullopt, metrics);
  AssertCounts(3, 1, 0, metrics);
  AssertBounds<int64_t>(3, int64(), std::nullopt, std::nullopt, metrics);
  AssertCounts(5, 1, 0, metrics);
  AssertBounds<int64_t>(5, int64(), std::nullopt, std::nullopt, metrics);
  AssertCounts(6, 1, 0, metrics);
  AssertBounds<std::string>(6, binary(), std::nullopt, std::nullopt, metrics);
  AssertCounts(7, 1, 0, metrics);
  AssertBounds<double>(7, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::FullMetricsMode() {
  auto schema = NestedSchema();

  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "full"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto records, BuildNestedRecords());
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // In Full mode, column_sizes should not be empty
  EXPECT_FALSE(metrics.column_sizes.empty());

  // Both counts and bounds should be present
  AssertCounts(1, 1, 0, metrics);
  AssertBounds<int32_t>(1, int32(), std::numeric_limits<int32_t>::min(),
                        std::numeric_limits<int32_t>::min(), metrics);
  AssertCounts(3, 1, 0, metrics);
  AssertBounds<int64_t>(3, int64(), 100, 100, metrics);
  AssertCounts(5, 1, 0, metrics);
  AssertBounds<int64_t>(5, int64(), 20, 20, metrics);
  AssertCounts(6, 1, 0, metrics);
  AssertBounds<std::vector<uint8_t>>(6, binary(), std::vector<uint8_t>{'A'},
                                     std::vector<uint8_t>{'A'}, metrics);
  AssertCounts(7, 1, 0, metrics);
  AssertBounds<double>(7, float64(), std::nullopt, std::nullopt, metrics);
}

void MetricsTestBase::TruncateStringMetricsMode() {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "str_to_truncate", string()),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("str_to_truncate", ::arrow::utf8(), false),
  });

  auto records = CreateRecordArrays(arrow_schema, R"([
    {"str_to_truncate": "Lorem ipsum dolor sit amet"}
  ])");

  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "truncate(10)"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // Column sizes should not be empty
  EXPECT_FALSE(metrics.column_sizes.empty());

  AssertCounts(1, 1, 0, metrics);

  // Bounds should be truncated to 10 characters
  // Lower bound: "Lorem ipsu" (first 10 chars)
  // Upper bound: "Lorem ipsv" (first 10 chars with last char incremented)
  std::string expected_lower = "Lorem ipsu";
  std::string expected_upper = "Lorem ipsv";
  AssertBounds<std::string>(1, string(), expected_lower, expected_upper, metrics);
}

void MetricsTestBase::TruncateBinaryMetricsMode() {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "bin_to_truncate", binary()),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("bin_to_truncate", ::arrow::binary(), false),
  });

  // Create binary data: {0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9, 0x10, 0xA, 0xB}
  ::arrow::BinaryBuilder builder;
  std::vector<uint8_t> data = {0x1, 0x2, 0x3, 0x4,  0x5, 0x6,
                               0x7, 0x8, 0x9, 0x10, 0xA, 0xB};
  ASSERT_TRUE(builder.Append(data.data(), data.size()).ok());
  auto array = builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {array};
  auto records =
      ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();

  std::unordered_map<std::string, std::string> properties = {
      {"write.metadata.metrics.default", "truncate(5)"}};

  ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(properties));
  ICEBERG_UNWRAP_OR_FAIL(auto metrics, GetMetrics(schema, config, records));

  ASSERT_TRUE(metrics.row_count.has_value()) << "row_count should be set";
  EXPECT_EQ(*metrics.row_count, 1);

  // Column sizes should not be empty
  EXPECT_FALSE(metrics.column_sizes.empty());

  AssertCounts(1, 1, 0, metrics);

  // Bounds should be truncated to 5 bytes
  // Lower bound: {0x1, 0x2, 0x3, 0x4, 0x5}
  // Upper bound: {0x1, 0x2, 0x3, 0x4, 0x6} (last byte incremented)
  auto expected_lower = std::vector<uint8_t>{0x1, 0x2, 0x3, 0x4, 0x5};
  auto expected_upper = std::vector<uint8_t>{0x1, 0x2, 0x3, 0x4, 0x6};
  AssertBounds<std::vector<uint8_t>>(1, binary(), expected_lower, expected_upper,
                                     metrics);
}

Result<std::shared_ptr<::arrow::Array>> MetricsTestBase::BuildSimpleRecords(
    std::shared_ptr<::arrow::Schema> arrow_schema, int32_t count) {
  ::arrow::BooleanBuilder boolean_builder;
  ::arrow::Int32Builder int_builder;
  ::arrow::Int64Builder long_builder;
  ::arrow::FloatBuilder float_builder;
  ::arrow::DoubleBuilder double_builder;
  ::arrow::Decimal128Builder decimal_builder(::arrow::decimal128(10, 2));
  ::arrow::StringBuilder string_builder;
  ::arrow::Date32Builder date_builder;
  ::arrow::Time64Builder time_builder(::arrow::time64(::arrow::TimeUnit::MICRO),
                                      ::arrow::default_memory_pool());
  ::arrow::TimestampBuilder timestamp_above_builder(
      ::arrow::timestamp(::arrow::TimeUnit::MICRO), ::arrow::default_memory_pool());
  ::arrow::FixedSizeBinaryBuilder fixed_builder(::arrow::fixed_size_binary(4));
  ::arrow::BinaryBuilder binary_builder;
  ::arrow::TimestampBuilder timestamp_below_builder(
      ::arrow::timestamp(::arrow::TimeUnit::MICRO), ::arrow::default_memory_pool());

  // Append identical records
  for (int i = 0; i < count; i++) {
    ICEBERG_ARROW_RETURN_NOT_OK(boolean_builder.Append(i != 0));
    ICEBERG_ARROW_RETURN_NOT_OK(int_builder.Append(3 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(i == 0 ? long_builder.AppendNull()
                                       : long_builder.Append(i));
    ICEBERG_ARROW_RETURN_NOT_OK(
        i == 0 ? float_builder.Append(std::numeric_limits<float>::quiet_NaN())
               : float_builder.Append(1.0 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(
        i == 0 ? double_builder.Append(std::numeric_limits<double>::quiet_NaN())
               : double_builder.Append(1.0 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(i == 0
                                    ? decimal_builder.AppendNull()
                                    : decimal_builder.Append(::arrow::Decimal128("100") +
                                                             i));  // 1.00 with scale 2
    ICEBERG_ARROW_RETURN_NOT_OK(string_builder.Append("AAA"));
    ICEBERG_ARROW_RETURN_NOT_OK(date_builder.Append(1500 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(time_builder.Append(2000 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(timestamp_above_builder.Append(i + 1));
    ICEBERG_ARROW_RETURN_NOT_OK(fixed_builder.Append("abcd"));
    ICEBERG_ARROW_RETURN_NOT_OK(binary_builder.Append("S"));
    ICEBERG_ARROW_RETURN_NOT_OK(timestamp_below_builder.Append((i + 1) * -1));
  }

  auto boolean_array = boolean_builder.Finish().ValueOrDie();
  auto int_array = int_builder.Finish().ValueOrDie();
  auto long_array = long_builder.Finish().ValueOrDie();
  auto float_array = float_builder.Finish().ValueOrDie();
  auto double_array = double_builder.Finish().ValueOrDie();
  auto decimal_array = decimal_builder.Finish().ValueOrDie();
  auto string_array = string_builder.Finish().ValueOrDie();
  auto date_array = date_builder.Finish().ValueOrDie();
  auto time_array = time_builder.Finish().ValueOrDie();
  auto timestamp_above_array = timestamp_above_builder.Finish().ValueOrDie();
  auto fixed_array = fixed_builder.Finish().ValueOrDie();
  auto binary_array = binary_builder.Finish().ValueOrDie();
  auto timestamp_below_array = timestamp_below_builder.Finish().ValueOrDie();

  std::vector<std::shared_ptr<::arrow::Array>> field_arrays = {
      boolean_array,         int_array,    long_array,
      float_array,           double_array, decimal_array,
      string_array,          date_array,   time_array,
      timestamp_above_array, fixed_array,  binary_array,
      timestamp_below_array};
  return ::arrow::StructArray::Make(field_arrays, arrow_schema->fields()).ValueOrDie();
}

Result<std::shared_ptr<::arrow::Array>> MetricsTestBase::BuildNestedRecords(
    int32_t count) {
  auto leaf_struct_type = ::arrow::struct_({
      ::arrow::field("leafLongCol", ::arrow::int64(), true),
      ::arrow::field("leafBinaryCol", ::arrow::binary(), true),
  });

  auto nested_struct_type = ::arrow::struct_({
      ::arrow::field("longCol", ::arrow::int64(), false),
      ::arrow::field("leafStructCol", leaf_struct_type, false),
      ::arrow::field("doubleCol", ::arrow::float64(), false),
  });

  auto arrow_schema = ::arrow::schema({
      ::arrow::field("intCol", ::arrow::int32(), false),
      ::arrow::field("nestedStructCol", nested_struct_type, false),
  });

  // Build leaf struct: {leafLongCol: 20, leafBinaryCol: "A"}
  ::arrow::Int64Builder leaf_long_builder;
  ::arrow::BinaryBuilder leaf_binary_builder;
  ::arrow::Int64Builder nested_long_builder;
  ::arrow::DoubleBuilder nested_double_builder;
  ::arrow::Int32Builder int_builder;

  for (int32_t i = 0; i < count; i++) {
    ICEBERG_ARROW_RETURN_NOT_OK(leaf_long_builder.Append(20 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(leaf_binary_builder.Append("A"));

    // Build nested struct: {longCol: 100, leafStructCol: {...}, doubleCol: NaN}

    ICEBERG_ARROW_RETURN_NOT_OK(nested_long_builder.Append(100 + i));
    ICEBERG_ARROW_RETURN_NOT_OK(
        nested_double_builder.Append(std::numeric_limits<double>::quiet_NaN()));

    // Build top-level struct: {intCol: 2147483647, nestedStructCol: {...}}
    ICEBERG_ARROW_RETURN_NOT_OK(
        int_builder.Append(std::numeric_limits<int32_t>::min() + i));
  }

  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto leaf_long_array, leaf_long_builder.Finish());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto leaf_binary_array, leaf_binary_builder.Finish());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto leaf_struct_array,
      ::arrow::StructArray::Make({leaf_long_array, leaf_binary_array},
                                 leaf_struct_type->fields()));

  // Build nested struct: {longCol: 100, leafStructCol: {...}, doubleCol: NaN}
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto nested_long_array, nested_long_builder.Finish());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto nested_double_array,
                                 nested_double_builder.Finish());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto nested_struct_array,
      ::arrow::StructArray::Make(
          {nested_long_array, leaf_struct_array, nested_double_array},
          nested_struct_type->fields()));

  // Build top-level struct: {intCol: 2147483647, nestedStructCol: {...}}
  ICEBERG_ARROW_ASSIGN_OR_RETURN(auto int_array, int_builder.Finish());
  ICEBERG_ARROW_ASSIGN_OR_RETURN(
      auto records, ::arrow::StructArray::Make({int_array, nested_struct_array},
                                               arrow_schema->fields()));
  return records;
}

}  // namespace iceberg::test
