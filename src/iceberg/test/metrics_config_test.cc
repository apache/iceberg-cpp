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

#include "iceberg/metrics_config.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/transform.h"

namespace iceberg {

TEST(MetricsConfigTest, MetricsMode) {
  EXPECT_EQ(MetricsMode::Kind::kNone, MetricsMode::None().kind);
  EXPECT_EQ(MetricsMode::Kind::kCounts, MetricsMode::Counts().kind);
  EXPECT_EQ(MetricsMode::Kind::kFull, MetricsMode::Full().kind);

  EXPECT_EQ(MetricsMode::Kind::kNone, MetricsMode::FromString("none").value().kind);
  EXPECT_EQ(MetricsMode::Kind::kCounts, MetricsMode::FromString("counts").value().kind);
  EXPECT_EQ(MetricsMode::Kind::kFull, MetricsMode::FromString("full").value().kind);
  EXPECT_EQ(MetricsMode::Kind::kTruncate,
            MetricsMode::FromString("truncate(32)").value().kind);

  auto result = MetricsMode::FromString("truncate(abc)");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid truncate mode"));

  result = MetricsMode::FromString("truncate(-1)");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Truncate length should be positive"));

  result = MetricsMode::FromString("invalid");
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid metrics mode"));
}

TEST(MetricsConfigTest, ForTable) {
  auto io = std::make_shared<MockFileIO>();
  auto catalog = std::make_shared<MockCatalog>();
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                               SchemaField::MakeOptional(2, "name", string()),
                               SchemaField::MakeOptional(3, "addr", string())},
      1);
  TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "t"};

  {
    // Default
    auto metadata = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});
    ICEBERG_UNWRAP_OR_FAIL(
        auto table, Table::Make(ident, metadata, "s3://bucket/meta.json", io, catalog));

    ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(*table));
    auto mode = config->ColumnMode("id");
    EXPECT_EQ(MetricsMode::Kind::kTruncate, mode.kind);
    EXPECT_EQ(16, std::get<int32_t>(mode.length));

    mode = config->ColumnMode("name");
    EXPECT_EQ(MetricsMode::Kind::kTruncate, mode.kind);
    EXPECT_EQ(16, std::get<int32_t>(mode.length));
    mode = config->ColumnMode("addr");
    EXPECT_EQ(MetricsMode::Kind::kTruncate, mode.kind);
    EXPECT_EQ(16, std::get<int32_t>(mode.length));
  }

  {
    // Custom metrics mode by set default metrics mode properties
    auto metadata = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
                      .schemas = {schema},
                      .current_schema_id = 1,
                      .properties = TableProperties::FromMap(
                          {{TableProperties::kDefaultWriteMetricsMode.key(), "full"}})});
    ICEBERG_UNWRAP_OR_FAIL(
        auto table, Table::Make(ident, metadata, "s3://bucket/meta.json", io, catalog));

    ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(*table));
    auto mode = config->ColumnMode("id");
    EXPECT_EQ(MetricsMode::Kind::kFull, mode.kind);

    mode = config->ColumnMode("name");
    EXPECT_EQ(MetricsMode::Kind::kFull, mode.kind);

    mode = config->ColumnMode("addr");
    EXPECT_EQ(MetricsMode::Kind::kFull, mode.kind);
  }

  {
    // Custom metrics mode by set column's metrics mode
    ICEBERG_UNWRAP_OR_FAIL(
        std::shared_ptr<SortOrder> sort_order,
        SortOrder::Make(*schema, /*sort_id=*/1,
                        std::vector<SortField>(
                            {SortField(/*source_id=*/1, Transform::Identity(),
                                       SortDirection::kAscending, NullOrder::kLast)})));

    auto metadata = std::make_shared<TableMetadata>(TableMetadata{
        .format_version = 2,
        .schemas = {schema},
        .current_schema_id = 1,
        .properties = TableProperties::FromMap(
            {{TableProperties::kDefaultWriteMetricsMode.key(), "none"},
             {TableProperties::kMetricsMaxInferredColumnDefaults.key(), "2"},
             {std::string(TableProperties::kMetricModeColumnConfPrefix) + "name",
              "full"}}),
        .sort_orders = {sort_order},
        .default_sort_order_id = 1,
    });

    ICEBERG_UNWRAP_OR_FAIL(
        auto table, Table::Make(ident, metadata, "s3://bucket/meta.json", io, catalog));

    ICEBERG_UNWRAP_OR_FAIL(auto config, MetricsConfig::Make(*table));
    auto mode = config->ColumnMode("id");
    EXPECT_EQ(MetricsMode::Kind::kTruncate, mode.kind);
    EXPECT_EQ(16, std::get<int32_t>(mode.length));

    mode = config->ColumnMode("name");
    EXPECT_EQ(MetricsMode::Kind::kFull, mode.kind);

    mode = config->ColumnMode("addr");
    EXPECT_EQ(MetricsMode::Kind::kNone, mode.kind);
  }
}

TEST(MetricsConfigTest, LimitFieldIds) {
  {
    // Nested struct type
    // Create nested struct type for level1_struct_a
    auto level2_struct_a_type = struct_(std::vector<SchemaField>{
        SchemaField::MakeRequired(31, "level3_primitive_s", string())});

    auto level1_struct_a_type = struct_(std::vector<SchemaField>{
        SchemaField::MakeOptional(21, "level2_primitive_i", int32()),
        SchemaField::MakeOptional(22, "level2_struct_a", level2_struct_a_type),
        SchemaField::MakeRequired(23, "level2_primitive_b", boolean())});

    // Create nested struct type for level1_struct_b
    auto level2_struct_b_type = struct_(std::vector<SchemaField>{
        SchemaField::MakeRequired(32, "level3_primitive_s", string())});

    auto level1_struct_b_type = struct_(std::vector<SchemaField>{
        SchemaField::MakeRequired(24, "level2_primitive_i", int32()),
        SchemaField::MakeRequired(25, "level2_struct_b", level2_struct_b_type)});
    // Create the main schema
    Schema schema(
        std::vector<SchemaField>{
            SchemaField::MakeRequired(11, "level1_struct_a", level1_struct_a_type),
            SchemaField::MakeRequired(12, "level1_struct_b", level1_struct_b_type),
            SchemaField::MakeRequired(13, "level1_primitive_i", int32())},
        100);

    auto result1 = MetricsConfig::LimitFieldIds(schema, 1);
    EXPECT_EQ(result1, (std::unordered_set<int32_t>{13}))
        << "Should only include top level primitive field";

    auto result2 = MetricsConfig::LimitFieldIds(schema, 2);
    EXPECT_EQ(result2, (std::unordered_set<int32_t>{13, 21}))
        << "Should include level 2 primitive field before nested struct";

    auto result3 = MetricsConfig::LimitFieldIds(schema, 3);
    EXPECT_EQ(result3, (std::unordered_set<int32_t>{13, 21, 23}))
        << "Should include all of level 2 primitive fields of struct a before nested "
           "struct";

    auto result4 = MetricsConfig::LimitFieldIds(schema, 4);
    EXPECT_EQ(result4, (std::unordered_set<int32_t>{13, 21, 23, 31}))
        << "Should include all eligible fields in struct a";

    auto result5 = MetricsConfig::LimitFieldIds(schema, 5);
    EXPECT_EQ(result5, (std::unordered_set<int32_t>{13, 21, 23, 31, 24}))
        << "Should include first primitive field in struct b";

    auto result6 = MetricsConfig::LimitFieldIds(schema, 6);
    EXPECT_EQ(result6, (std::unordered_set<int32_t>{13, 21, 23, 31, 24, 32}))
        << "Should include all primitive fields";

    auto result7 = MetricsConfig::LimitFieldIds(schema, 7);
    EXPECT_EQ(result7, (std::unordered_set<int32_t>{13, 21, 23, 31, 24, 32}))
        << "Should return all primitive fields when limit is higher";
  }

  {
    // Nested map
    auto map_type = map(SchemaField::MakeRequired(2, "key", int32()),
                        SchemaField::MakeRequired(3, "value", int32()));

    Schema schema(std::vector<SchemaField>{SchemaField::MakeRequired(1, "map", map_type),
                                           SchemaField::MakeRequired(4, "top", int32())},
                  100);

    auto result1 = MetricsConfig::LimitFieldIds(schema, 1);
    EXPECT_EQ(result1, (std::unordered_set<int32_t>{4}));

    auto result2 = MetricsConfig::LimitFieldIds(schema, 2);
    EXPECT_EQ(result2, (std::unordered_set<int32_t>{4, 2}));

    auto result3 = MetricsConfig::LimitFieldIds(schema, 3);
    EXPECT_EQ(result3, (std::unordered_set<int32_t>{4, 2, 3}));

    auto result4 = MetricsConfig::LimitFieldIds(schema, 4);
    EXPECT_EQ(result4, (std::unordered_set<int32_t>{4, 2, 3}));
  }

  {
    // Nested list of maps
    auto map_type = map(SchemaField::MakeRequired(3, "key", int32()),
                        SchemaField::MakeRequired(4, "value", int32()));
    auto list_type = list(SchemaField::MakeRequired(2, "element", map_type));

    Schema schema(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "array_of_maps", list_type),
                                 SchemaField::MakeRequired(5, "top", int32())},
        100);

    auto result1 = MetricsConfig::LimitFieldIds(schema, 1);
    EXPECT_EQ(result1, (std::unordered_set<int32_t>{5}));

    auto result2 = MetricsConfig::LimitFieldIds(schema, 2);
    EXPECT_EQ(result2, (std::unordered_set<int32_t>{5, 3}));

    auto result3 = MetricsConfig::LimitFieldIds(schema, 3);
    EXPECT_EQ(result3, (std::unordered_set<int32_t>{5, 3, 4}));

    auto result4 = MetricsConfig::LimitFieldIds(schema, 4);
    EXPECT_EQ(result4, (std::unordered_set<int32_t>{5, 3, 4}));
  }
}

TEST(MetricsConfigTest, ValidateColumnReferences) {
  SchemaField field1 = SchemaField::MakeRequired(1, "col1", int64());
  SchemaField field2 = SchemaField::MakeOptional(2, "col2", string());
  SchemaField field3 = SchemaField::MakeRequired(3, "col3", float64());
  Schema schema(std::vector<SchemaField>{field1, field2, field3}, 100);

  {
    // Empty updates should be valid
    std::unordered_map<std::string, std::string> updates;

    auto result = MetricsConfig::VerifyReferencedColumns(updates, schema);
    EXPECT_THAT(result, IsOk()) << "Validation should pass for empty updates";
  }

  {
    // No column references
    std::unordered_map<std::string, std::string> updates;
    updates["write.format.default"] = "parquet";
    updates["write.target-file-size-bytes"] = "524288000";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, schema);
    EXPECT_THAT(result, IsOk())
        << "Validation should pass when no column references exist";
  }

  {
    // Valid column reference
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col1"] =
        "counts";
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col2"] = "full";
    updates["some.other.property"] = "value";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, schema);
    EXPECT_THAT(result, IsOk()) << "Validation should pass for valid column references";
  }

  {
    // Invalid column reference
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "nonexistent"] =
        "counts";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, schema);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed))
        << "Validation should fail for invalid column references";
  }

  {
    // Mixed valid and invalid column references
    std::unordered_map<std::string, std::string> updates;
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "col1"] =
        "counts";
    updates[std::string(TableProperties::kMetricModeColumnConfPrefix) + "nonexistent"] =
        "full";

    auto result = MetricsConfig::VerifyReferencedColumns(updates, schema);
    EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed))
        << "Validation should fail when any column reference is invalid";
  }
}

}  // namespace iceberg
