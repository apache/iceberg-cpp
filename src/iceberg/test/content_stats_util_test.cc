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

#include "iceberg/stats/content_stats_util.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/metadata_columns.h"
#include "iceberg/metrics_config.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_util.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

using Util = ContentStatsUtil;

std::shared_ptr<MetricsConfig> MakeConfig(
    std::unordered_map<std::string, std::string> properties) {
  auto config = MetricsConfig::Make(std::move(properties));
  EXPECT_THAT(config, IsOk());
  return config.value();
}

std::shared_ptr<MetricsConfig> ConfigWithDefault(const std::string& mode) {
  return MakeConfig({{"write.metadata.metrics.default", mode}});
}

std::vector<int32_t> FieldIds(const StructType& type) {
  std::vector<int32_t> ids;
  for (const auto& field : type.fields()) {
    ids.push_back(field.field_id());
  }
  return ids;
}

std::vector<std::string> FieldNames(const StructType& type) {
  std::vector<std::string> names;
  for (const auto& field : type.fields()) {
    names.emplace_back(field.name());
  }
  return names;
}

const StructType& StatsOf(const StructType& content_stats, int32_t base_id) {
  auto field = content_stats.GetFieldById(base_id);
  EXPECT_THAT(field, IsOk());
  EXPECT_TRUE(field.value().has_value()) << "missing stats struct " << base_id;
  const auto& type = field.value()->get().type();
  EXPECT_EQ(type->type_id(), TypeId::kStruct);
  return static_cast<const StructType&>(*type);
}

SchemaField ValueCount(int32_t base_id) {
  return SchemaField::MakeOptional(base_id + 3, "value_count", int64(),
                                   "Number of values (including null and NaN)");
}

SchemaField NullValueCount(int32_t base_id) {
  return SchemaField::MakeOptional(base_id + 4, "null_value_count", int64(),
                                   "Number of null values");
}

SchemaField NanValueCount(int32_t base_id) {
  return SchemaField::MakeOptional(base_id + 5, "nan_value_count", int64(),
                                   "Number of NaN values");
}

SchemaField TotalBytes(int32_t base_id) {
  return SchemaField::MakeOptional(base_id + 6, "total_bytes", int64());
}

}  // namespace

TEST(ContentStatsUtilTest, BaseId) {
  EXPECT_EQ(Util::BaseId(0), 10'000);
  EXPECT_EQ(Util::BaseId(1), 10'200);
  // spec example: table field 2 uses [10_400, 10_599]
  EXPECT_EQ(Util::BaseId(2), 10'400);
  EXPECT_EQ(Util::BaseId(4), 10'800);
  EXPECT_EQ(Util::BaseId(999'949), 199'999'800);

  // stats for these fields would fall outside [10_000, 200_000_000)
  EXPECT_EQ(Util::BaseId(-1), std::nullopt);
  EXPECT_EQ(Util::BaseId(999'950), std::nullopt);
  EXPECT_EQ(Util::BaseId(Schema::kInvalidColumnId), std::nullopt);

  // reserved metadata fields
  EXPECT_EQ(Util::BaseId(2147483539), 9'000);
  EXPECT_EQ(Util::BaseId(MetadataColumns::kLastUpdatedSequenceNumberColumnId), 9'000);
  EXPECT_EQ(Util::BaseId(2147483540), 9'200);
  EXPECT_EQ(Util::BaseId(MetadataColumns::kRowIdColumnId), 9'200);
  // other metadata fields are not tracked
  EXPECT_EQ(Util::BaseId(MetadataColumns::kFilePathColumnId), std::nullopt);
  EXPECT_EQ(Util::BaseId(MetadataColumns::kFilePositionColumnId), std::nullopt);
}

TEST(ContentStatsUtilTest, FieldIdAndOffset) {
  for (int32_t field_id : {0, 1, 2, 17, 1000, 999'949}) {
    auto base_id = Util::BaseId(field_id).value();
    for (int32_t offset = 0; offset < Util::kNumReservedFieldStatsIds; ++offset) {
      EXPECT_THAT(Util::FieldId(base_id + offset), HasValue(::testing::Eq(field_id)));
      EXPECT_THAT(Util::StatOffset(base_id + offset), HasValue(::testing::Eq(offset)));
    }
  }

  EXPECT_THAT(Util::FieldId(10'401), HasValue(::testing::Eq(2)));
  EXPECT_THAT(Util::StatOffset(10'401), HasValue(::testing::Eq(1)));
  EXPECT_THAT(Util::FieldId(10'817), HasValue(::testing::Eq(4)));
  EXPECT_THAT(Util::StatOffset(10'817), HasValue(::testing::Eq(17)));
}

TEST(ContentStatsUtilTest, ReservedFieldIds) {
  for (int32_t id : {9'000, 9'001, 9'199}) {
    EXPECT_THAT(
        Util::FieldId(id),
        HasValue(::testing::Eq(MetadataColumns::kLastUpdatedSequenceNumberColumnId)));
  }
  for (int32_t id : {9'200, 9'206, 9'399}) {
    EXPECT_THAT(Util::FieldId(id),
                HasValue(::testing::Eq(MetadataColumns::kRowIdColumnId)));
  }
  EXPECT_THAT(Util::StatOffset(9'000), HasValue(::testing::Eq(0)));
  EXPECT_THAT(Util::StatOffset(9'203), HasValue(::testing::Eq(3)));

  // unassigned part of the metadata range
  for (int32_t id : {9'400, 9'999}) {
    EXPECT_THAT(Util::FieldId(id), IsError(ErrorKind::kNotSupported));
    EXPECT_THAT(Util::FieldId(id),
                HasErrorMessage("Unsupported metadata stats field ID"));
    EXPECT_THAT(Util::StatOffset(id), HasValue(::testing::Eq(id % 200)));
  }

  // outside the stats range
  for (int32_t id : {-1, 0, 146, 8'999, 200'000'000, 2147483540}) {
    EXPECT_THAT(Util::FieldId(id), IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(Util::FieldId(id), HasErrorMessage("Invalid stats field ID"));
    EXPECT_THAT(Util::StatOffset(id), IsError(ErrorKind::kInvalidArgument));
  }
  EXPECT_THAT(Util::FieldId(199'999'999), HasValue(::testing::Eq(999'949)));
}

// The spec example: a required int `id` (2) and an optional string `data` (3).
TEST(ContentStatsUtilTest, SpecExample) {
  Schema schema({SchemaField::MakeRequired(2, "id", int32()),
                 SchemaField::MakeOptional(3, "data", string())});

  auto expected = struct_({
      SchemaField::MakeOptional(
          10'400, "id",
          struct_({
              SchemaField::MakeOptional(10'401, "lower_bound", int32()),
              SchemaField::MakeOptional(10'402, "upper_bound", int32()),
              ValueCount(10'400),
          })),
      SchemaField::MakeOptional(
          10'600, "data",
          struct_({
              SchemaField::MakeOptional(10'601, "lower_bound", string()),
              SchemaField::MakeOptional(10'602, "upper_bound", string()),
              ValueCount(10'600),
              NullValueCount(10'600),
              TotalBytes(10'600),
          })),
  });

  auto write_type = Util::StatsWriteType(schema, *ConfigWithDefault("full"));
  EXPECT_EQ(*write_type, *expected) << write_type->ToString();

  // the default truncate(16) mode has the same type
  EXPECT_EQ(*Util::StatsWriteType(schema, *MetricsConfig::Default()), *expected);

  // reads also project null_value_count for the required `id`
  std::vector<int32_t> ids = {2, 3};
  auto read_type = Util::StatsReadType(schema, ids);
  EXPECT_EQ(FieldIds(StatsOf(*read_type, 10'400)),
            (std::vector<int32_t>{10'401, 10'402, 10'403, 10'404}));
  EXPECT_EQ(StatsOf(*read_type, 10'600), StatsOf(*expected, 10'600));
}

// The spec example: an optional geometry `location` (4).
TEST(ContentStatsUtilTest, GeoBounds) {
  for (const auto& type : std::vector<std::shared_ptr<Type>>{geometry(), geography()}) {
    SchemaField field = SchemaField::MakeOptional(4, "location", type);
    auto expected = struct_({
        SchemaField::MakeOptional(
            10'801, "lower_bound",
            struct_({
                SchemaField::MakeRequired(10'810, "x", float64(),
                                          "Bounding box westernmost/xmin; [-180..180]"),
                SchemaField::MakeRequired(10'811, "y", float64(),
                                          "Bounding box southernmost/ymin; [-90..90]"),
                SchemaField::MakeOptional(10'812, "z", float64(), "Bounding box zmin"),
                SchemaField::MakeOptional(10'813, "m", float64(), "Bounding box mmin"),
            })),
        SchemaField::MakeOptional(
            10'802, "upper_bound",
            struct_({
                SchemaField::MakeRequired(10'814, "x", float64(),
                                          "Bounding box easternmost/xmax; [-180..180]"),
                SchemaField::MakeRequired(10'815, "y", float64(),
                                          "Bounding box northernmost/ymax; [-90..90]"),
                SchemaField::MakeOptional(10'816, "z", float64(), "Bounding box zmax"),
                SchemaField::MakeOptional(10'817, "m", float64(), "Bounding box mmax"),
            })),
        ValueCount(10'800),
        NullValueCount(10'800),
        TotalBytes(10'800),
    });

    auto stats = Util::FieldStatsType(field, 10'800, MetricsMode::Full());
    ASSERT_NE(stats, nullptr);
    EXPECT_EQ(*stats, *expected) << stats->ToString();

    // counts mode has no bounds
    auto counts = Util::FieldStatsType(field, 10'800, MetricsMode::Counts());
    ASSERT_NE(counts, nullptr);
    EXPECT_EQ(FieldIds(*counts), (std::vector<int32_t>{10'803, 10'804, 10'806}));
  }
}

TEST(ContentStatsUtilTest, PrimitiveTypes) {
  struct Case {
    std::shared_ptr<Type> type;
    bool has_nan_count;
    bool has_total_bytes;
  };
  std::vector<Case> cases = {
      {.type = boolean(), .has_nan_count = false, .has_total_bytes = false},
      {.type = int32(), .has_nan_count = false, .has_total_bytes = false},
      {.type = int64(), .has_nan_count = false, .has_total_bytes = false},
      {.type = float32(), .has_nan_count = true, .has_total_bytes = false},
      {.type = float64(), .has_nan_count = true, .has_total_bytes = false},
      {.type = decimal(9, 2), .has_nan_count = false, .has_total_bytes = false},
      {.type = date(), .has_nan_count = false, .has_total_bytes = false},
      {.type = time(), .has_nan_count = false, .has_total_bytes = false},
      {.type = timestamp(), .has_nan_count = false, .has_total_bytes = false},
      {.type = timestamp_tz(), .has_nan_count = false, .has_total_bytes = false},
      {.type = timestamp_ns(), .has_nan_count = false, .has_total_bytes = false},
      {.type = timestamptz_ns(), .has_nan_count = false, .has_total_bytes = false},
      {.type = string(), .has_nan_count = false, .has_total_bytes = true},
      {.type = uuid(), .has_nan_count = false, .has_total_bytes = false},
      {.type = fixed(16), .has_nan_count = false, .has_total_bytes = false},
      {.type = binary(), .has_nan_count = false, .has_total_bytes = true},
      {.type = unknown(), .has_nan_count = false, .has_total_bytes = false},
      {.type = variant(), .has_nan_count = false, .has_total_bytes = true},
      {.type = geometry(), .has_nan_count = false, .has_total_bytes = true},
      {.type = geography(), .has_nan_count = false, .has_total_bytes = true},
  };

  constexpr int32_t kBaseId = 10'200;
  for (const auto& c : cases) {
    SCOPED_TRACE(c.type->ToString());
    bool is_geo =
        c.type->type_id() == TypeId::kGeometry || c.type->type_id() == TypeId::kGeography;

    for (bool optional : {false, true}) {
      SchemaField field(1, "col", c.type, optional);
      auto stats = Util::FieldStatsType(field, kBaseId, MetricsMode::Full());
      ASSERT_NE(stats, nullptr);

      std::vector<int32_t> expected_ids = {10'201, 10'202, 10'203};
      std::vector<std::string> expected_names = {"lower_bound", "upper_bound",
                                                 "value_count"};
      if (optional) {
        expected_ids.push_back(10'204);
        expected_names.emplace_back("null_value_count");
      }
      if (c.has_nan_count) {
        expected_ids.push_back(10'205);
        expected_names.emplace_back("nan_value_count");
      }
      if (c.has_total_bytes) {
        expected_ids.push_back(10'206);
        expected_names.emplace_back("total_bytes");
      }
      EXPECT_EQ(FieldIds(*stats), expected_ids);
      EXPECT_EQ(FieldNames(*stats), expected_names);

      for (const auto& stat : stats->fields()) {
        EXPECT_TRUE(stat.optional()) << stat.name();
        if (stat.field_id() == 10'201 || stat.field_id() == 10'202) {
          if (is_geo) {
            EXPECT_EQ(stat.type()->type_id(), TypeId::kStruct);
          } else {
            // bounds use the field's type
            EXPECT_EQ(*stat.type(), *c.type);
          }
        } else {
          EXPECT_EQ(*stat.type(), *int64());
        }
      }
    }
  }
}

TEST(ContentStatsUtilTest, MetricsModes) {
  SchemaField field = SchemaField::MakeOptional(1, "f", float64());
  constexpr int32_t kBaseId = 10'200;

  EXPECT_EQ(Util::FieldStatsType(field, kBaseId, MetricsMode::None()), nullptr);

  auto counts = Util::FieldStatsType(field, kBaseId, MetricsMode::Counts());
  ASSERT_NE(counts, nullptr);
  EXPECT_EQ(FieldIds(*counts), (std::vector<int32_t>{10'203, 10'204, 10'205}));

  auto truncate = Util::FieldStatsType(
      field, kBaseId, MetricsMode{.kind = MetricsMode::Kind::kTruncate, .length = 4});
  ASSERT_NE(truncate, nullptr);
  EXPECT_EQ(FieldIds(*truncate),
            (std::vector<int32_t>{10'201, 10'202, 10'203, 10'204, 10'205}));

  auto full = Util::FieldStatsType(field, kBaseId, MetricsMode::Full());
  ASSERT_NE(full, nullptr);
  EXPECT_EQ(*full, *truncate);

  // nested types have no field stats
  SchemaField nested = SchemaField::MakeOptional(
      1, "s", struct_({SchemaField::MakeOptional(2, "x", int32())}));
  EXPECT_EQ(Util::FieldStatsType(nested, kBaseId, MetricsMode::Full()), nullptr);
}

TEST(ContentStatsUtilTest, WriteTypeMetricsConfig) {
  Schema schema({
      SchemaField::MakeRequired(1, "id", int64()),
      SchemaField::MakeOptional(2, "data", string()),
      SchemaField::MakeOptional(3, "point",
                                struct_({SchemaField::MakeRequired(4, "x", float64()),
                                         SchemaField::MakeRequired(5, "y", float64())})),
  });

  // none: no stats at all
  auto none = Util::StatsWriteType(schema, *ConfigWithDefault("none"));
  EXPECT_TRUE(none->fields().empty());

  // counts: no bounds
  auto counts = Util::StatsWriteType(schema, *ConfigWithDefault("counts"));
  EXPECT_EQ(FieldIds(*counts), (std::vector<int32_t>{10'200, 10'400, 10'800, 11'000}));
  EXPECT_EQ(FieldIds(StatsOf(*counts, 10'200)), (std::vector<int32_t>{10'203}));
  EXPECT_EQ(FieldIds(StatsOf(*counts, 10'400)),
            (std::vector<int32_t>{10'403, 10'404, 10'406}));
  EXPECT_EQ(FieldIds(StatsOf(*counts, 10'800)), (std::vector<int32_t>{10'803, 10'805}));

  // truncate and full: bounds
  for (const auto* mode : {"truncate(8)", "full"}) {
    auto stats = Util::StatsWriteType(schema, *ConfigWithDefault(mode));
    EXPECT_EQ(FieldIds(*stats), (std::vector<int32_t>{10'200, 10'400, 10'800, 11'000}));
    EXPECT_EQ(FieldIds(StatsOf(*stats, 10'200)),
              (std::vector<int32_t>{10'201, 10'202, 10'203}));
    EXPECT_EQ(FieldIds(StatsOf(*stats, 10'400)),
              (std::vector<int32_t>{10'401, 10'402, 10'403, 10'404, 10'406}));
  }

  // per-column overrides use the canonical (dotted) column name
  auto mixed = Util::StatsWriteType(
      schema, *MakeConfig({{"write.metadata.metrics.default", "counts"},
                           {"write.metadata.metrics.column.id", "none"},
                           {"write.metadata.metrics.column.point.x", "full"},
                           {"write.metadata.metrics.column.point.y", "none"}}));
  EXPECT_EQ(FieldIds(*mixed), (std::vector<int32_t>{10'400, 10'800}));
  EXPECT_EQ(FieldIds(StatsOf(*mixed, 10'400)),
            (std::vector<int32_t>{10'403, 10'404, 10'406}));
  EXPECT_EQ(FieldIds(StatsOf(*mixed, 10'800)),
            (std::vector<int32_t>{10'801, 10'802, 10'803, 10'805}));
}

TEST(ContentStatsUtilTest, NestedStructs) {
  Schema schema({
      SchemaField::MakeOptional(
          1, "a",
          struct_({
              SchemaField::MakeRequired(2, "b", int32()),
              SchemaField::MakeOptional(
                  3, "c", struct_({SchemaField::MakeOptional(4, "d", string())})),
          })),
      SchemaField::MakeRequired(5, "e", int32()),
  });

  auto stats = Util::StatsWriteType(schema, *ConfigWithDefault("full"));
  // struct fields have no stats; leaves are named by their path joined with '_'
  EXPECT_EQ(FieldIds(*stats), (std::vector<int32_t>{10'400, 10'800, 11'000}));
  EXPECT_EQ(FieldNames(*stats), (std::vector<std::string>{"a_b", "a_c_d", "e"}));
  for (const auto& field : stats->fields()) {
    EXPECT_TRUE(field.optional());
  }

  // `b` is required: no null count, even though its parent is optional
  EXPECT_EQ(FieldIds(StatsOf(*stats, 10'400)),
            (std::vector<int32_t>{10'401, 10'402, 10'403}));
  EXPECT_EQ(FieldIds(StatsOf(*stats, 10'800)),
            (std::vector<int32_t>{10'801, 10'802, 10'803, 10'804, 10'806}));
}

TEST(ContentStatsUtilTest, ListsAndMapsAreSkipped) {
  Schema schema({
      SchemaField::MakeRequired(1, "id", int64()),
      SchemaField::MakeOptional(2, "tags",
                                list(SchemaField::MakeOptional(3, "element", string()))),
      SchemaField::MakeOptional(4, "props",
                                map(SchemaField::MakeRequired(5, "key", string()),
                                    SchemaField::MakeOptional(6, "value", int32()))),
      SchemaField::MakeOptional(
          7, "points",
          list(SchemaField::MakeOptional(
              8, "element", struct_({SchemaField::MakeRequired(9, "x", float64())})))),
      SchemaField::MakeOptional(
          10, "s",
          struct_({SchemaField::MakeOptional(
                       11, "nested_list",
                       list(SchemaField::MakeRequired(12, "element", int32()))),
                   SchemaField::MakeOptional(13, "v", variant())})),
  });

  auto stats = Util::StatsWriteType(schema, *ConfigWithDefault("full"));
  EXPECT_EQ(FieldIds(*stats), (std::vector<int32_t>{10'200, 12'600}));
  EXPECT_EQ(FieldNames(*stats), (std::vector<std::string>{"id", "s_v"}));

  // variant bounds are variants
  const auto& variant_stats = StatsOf(*stats, 12'600);
  EXPECT_EQ(FieldIds(variant_stats),
            (std::vector<int32_t>{12'601, 12'602, 12'603, 12'604, 12'606}));
  EXPECT_EQ(*variant_stats.fields()[0].type(), *variant());

  // reads skip them too
  std::vector<int32_t> ids = {2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};
  EXPECT_TRUE(Util::StatsReadType(schema, ids)->fields().empty());
}

TEST(ContentStatsUtilTest, ReadType) {
  Schema schema({
      SchemaField::MakeRequired(1, "id", int64()),
      SchemaField::MakeOptional(2, "data", string()),
      SchemaField::MakeOptional(3, "score", float32()),
  });

  // full mode, ordered by stats ID, duplicates and unknown IDs ignored
  std::vector<int32_t> ids = {3, 1, 3, 42, -1, MetadataColumns::kRowIdColumnId};
  auto stats = Util::StatsReadType(schema, ids);
  EXPECT_EQ(FieldIds(*stats), (std::vector<int32_t>{10'200, 10'600}));
  EXPECT_EQ(FieldNames(*stats), (std::vector<std::string>{"id", "score"}));
  EXPECT_EQ(FieldIds(StatsOf(*stats, 10'600)),
            (std::vector<int32_t>{10'601, 10'602, 10'603, 10'604, 10'605}));

  // the required `id` also gets null_value_count
  EXPECT_EQ(FieldIds(StatsOf(*stats, 10'200)),
            (std::vector<int32_t>{10'201, 10'202, 10'203, 10'204}));

  // the read type covers the full write type
  auto write_type = Util::StatsWriteType(schema, *ConfigWithDefault("full"));
  EXPECT_EQ(StatsOf(*stats, 10'600), StatsOf(*write_type, 10'600));
  for (const auto& stat : StatsOf(*write_type, 10'200).fields()) {
    auto read_stat = StatsOf(*stats, 10'200).GetFieldById(stat.field_id());
    ASSERT_THAT(read_stat, IsOk());
    ASSERT_TRUE(read_stat.value().has_value()) << stat.field_id();
    EXPECT_EQ(read_stat.value()->get(), stat);
  }

  EXPECT_TRUE(Util::StatsReadType(schema, std::vector<int32_t>{})->fields().empty());
}

TEST(ContentStatsUtilTest, OutOfRangeFieldIds) {
  Schema schema({
      SchemaField::MakeRequired(1, "id", int64()),
      SchemaField::MakeOptional(999'949, "last", int32()),
      SchemaField::MakeOptional(999'950, "too_big", int32()),
  });

  auto stats = Util::StatsWriteType(schema, *ConfigWithDefault("full"));
  EXPECT_EQ(FieldIds(*stats), (std::vector<int32_t>{10'200, 199'999'800}));

  std::vector<int32_t> ids = {999'950};
  EXPECT_TRUE(Util::StatsReadType(schema, ids)->fields().empty());
}

// Java writes null_value_count for required fields too (apache/iceberg#17413). The
// read type must resolve it by ID from such files, and must also read files written
// by the spec rule, where it is missing, as unknown (null).
TEST(ContentStatsUtilTest, ReadTypeProjectsJavaAndSpecFiles) {
  Schema schema({SchemaField::MakeRequired(2, "id", int32()),
                 SchemaField::MakeOptional(3, "data", string())});
  std::vector<int32_t> ids = {2, 3};
  auto read_schema = Util::StatsReadType(schema, ids)->ToSchema();

  // content_stats as Java's StatsUtil.statsWriteSchema writes it
  auto java_written = struct_({
      SchemaField::MakeOptional(
          10'400, "id",
          struct_({
              SchemaField::MakeOptional(10'401, "lower_bound", int32()),
              SchemaField::MakeOptional(10'402, "upper_bound", int32()),
              ValueCount(10'400),
              NullValueCount(10'400),
          })),
      SchemaField::MakeOptional(
          10'600, "data",
          struct_({
              SchemaField::MakeOptional(10'601, "lower_bound", string()),
              SchemaField::MakeOptional(10'602, "upper_bound", string()),
              ValueCount(10'600),
              NullValueCount(10'600),
              TotalBytes(10'600),
          })),
  });
  auto spec_written = Util::StatsWriteType(schema, *ConfigWithDefault("full"));

  for (bool from_java : {true, false}) {
    SCOPED_TRACE(from_java ? "Java-written" : "spec-written");
    auto file_schema = (from_java ? java_written : spec_written)->ToSchema();
    auto projection = Project(*read_schema, *file_schema, /*prune_source=*/true);
    ASSERT_THAT(projection, IsOk());
    ASSERT_EQ(projection->fields.size(), 2);

    const auto& id_stats = projection->fields[0];
    EXPECT_EQ(id_stats.kind, FieldProjection::Kind::kProjected);
    ASSERT_EQ(id_stats.children.size(), 4);
    for (size_t i = 0; i < 3; ++i) {
      EXPECT_EQ(id_stats.children[i].kind, FieldProjection::Kind::kProjected);
    }
    // 10_404 null_value_count of the required `id`
    EXPECT_EQ(id_stats.children[3].kind, from_java ? FieldProjection::Kind::kProjected
                                                   : FieldProjection::Kind::kNull);

    for (const auto& child : projection->fields[1].children) {
      EXPECT_EQ(child.kind, FieldProjection::Kind::kProjected);
    }
  }
}

}  // namespace iceberg
