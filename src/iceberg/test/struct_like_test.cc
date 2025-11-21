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
#include <arrow/json/from_string.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/decimal.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/manifest_list.h"
#include "iceberg/manifest_reader_internal.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/row/manifest_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

#define EXPECT_SCALAR_EQ(result, expected_type, expected_value) \
  do {                                                          \
    ASSERT_THAT(result, IsOk());                                \
    auto scalar = result.value();                               \
    ASSERT_TRUE(std::holds_alternative<expected_type>(scalar)); \
    EXPECT_EQ(std::get<expected_type>(scalar), expected_value); \
  } while (0)

#define EXPECT_DECIMAL_EQ(result, scale, expected_value)  \
  do {                                                    \
    ASSERT_THAT(result, IsOk());                          \
    auto scalar = result.value();                         \
    ASSERT_TRUE(std::holds_alternative<Decimal>(scalar)); \
    auto decimal = std::get<Decimal>(scalar);             \
    EXPECT_EQ(decimal.ToString(scale), expected_value);   \
  } while (0)

#define EXPECT_SCALAR_NULL(result)                               \
  do {                                                           \
    ASSERT_THAT(result, IsOk());                                 \
    auto scalar = result.value();                                \
    ASSERT_TRUE(std::holds_alternative<std::monostate>(scalar)); \
  } while (0)

TEST(ManifestFileStructLike, BasicFields) {
  ManifestFile manifest_file{
      .manifest_path = "/path/to/manifest.avro",
      .manifest_length = 12345,
      .partition_spec_id = 1,
      .content = ManifestContent::kData,
      .sequence_number = 100,
      .min_sequence_number = 90,
      .added_snapshot_id = 1001,
      .added_files_count = 10,
      .existing_files_count = 5,
      .deleted_files_count = 2,
      .added_rows_count = 1000,
      .existing_rows_count = 500,
      .deleted_rows_count = 20,
  };

  ManifestFileStructLike struct_like(manifest_file);
  EXPECT_EQ(struct_like.num_fields(), 16);

  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kManifestPath)),
      std::string_view, "/path/to/manifest.avro");
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kManifestLength)),
      int64_t, 12345);
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kPartitionSpecId)),
      int32_t, 1);
  EXPECT_SCALAR_EQ(struct_like.GetField(static_cast<size_t>(ManifestFileField::kContent)),
                   int32_t, static_cast<int32_t>(ManifestContent::kData));
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kSequenceNumber)),
      int64_t, 100);
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kAddedFilesCount)),
      int32_t, 10);
  EXPECT_THAT(struct_like.GetField(100), IsError(ErrorKind::kInvalidArgument));
}

TEST(ManifestFileStructLike, OptionalFields) {
  ManifestFile manifest_file{.manifest_path = "/path/to/manifest2.avro",
                             .manifest_length = 54321,
                             .partition_spec_id = 2,
                             .content = ManifestContent::kDeletes,
                             .sequence_number = 200,
                             .min_sequence_number = 180,
                             .added_snapshot_id = 2001,
                             .added_files_count = std::nullopt,  // null optional field
                             .existing_files_count = 15,
                             .deleted_files_count = std::nullopt,  // null optional field
                             .added_rows_count = std::nullopt,     // null optional field
                             .existing_rows_count = 1500,
                             .deleted_rows_count = 200,
                             .partitions = {},
                             .key_metadata = {},
                             .first_row_id = 12345};
  ManifestFileStructLike struct_like(manifest_file);

  EXPECT_SCALAR_NULL(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kAddedFilesCount)));
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kExistingFilesCount)),
      int32_t, 15);
  EXPECT_SCALAR_EQ(
      struct_like.GetField(static_cast<size_t>(ManifestFileField::kFirstRowId)), int64_t,
      12345);
  EXPECT_SCALAR_EQ(struct_like.GetField(static_cast<size_t>(ManifestFileField::kContent)),
                   int32_t, static_cast<int32_t>(ManifestContent::kDeletes));
}

TEST(ManifestFileStructLike, WithPartitions) {
  ManifestFile manifest_file{
      .manifest_path = "/path/to/manifest3.avro",
      .manifest_length = 98765,
      .partition_spec_id = 3,
      .content = ManifestContent::kData,
      .sequence_number = 300,
      .min_sequence_number = 290,
      .added_snapshot_id = 3001,
      .added_files_count = 20,
      .existing_files_count = 10,
      .deleted_files_count = 1,
      .added_rows_count = 2000,
      .existing_rows_count = 1000,
      .deleted_rows_count = 10,
      .partitions = {{.contains_null = true,
                      .contains_nan = false,
                      .lower_bound = std::vector<uint8_t>{0x01, 0x02, 0x03},
                      .upper_bound = std::vector<uint8_t>{0x04, 0x05, 0x06}},
                     {.contains_null = false,
                      .contains_nan = std::nullopt,
                      .lower_bound = std::vector<uint8_t>{0x10, 0x20},
                      .upper_bound = std::nullopt}}};

  ManifestFileStructLike struct_like(manifest_file);

  auto partitions_result = struct_like.GetField(
      static_cast<size_t>(ManifestFileField::kPartitionFieldSummary));
  ASSERT_THAT(partitions_result, IsOk());
  auto partitions_scalar = partitions_result.value();
  ASSERT_TRUE(std::holds_alternative<std::shared_ptr<ArrayLike>>(partitions_scalar));
  auto partitions_array = std::get<std::shared_ptr<ArrayLike>>(partitions_scalar);
  EXPECT_EQ(partitions_array->size(), 2);

  // Test 1st partition summary
  auto first_partition_result = partitions_array->GetElement(0);
  ASSERT_THAT(first_partition_result, IsOk());
  auto first_partition_scalar = first_partition_result.value();
  ASSERT_TRUE(
      std::holds_alternative<std::shared_ptr<StructLike>>(first_partition_scalar));
  auto first_partition_struct =
      std::get<std::shared_ptr<StructLike>>(first_partition_scalar);
  EXPECT_EQ(first_partition_struct->num_fields(), 4);
  EXPECT_SCALAR_EQ(first_partition_struct->GetField(0), bool, true);
  EXPECT_SCALAR_EQ(first_partition_struct->GetField(1), bool, false);
  auto lower_bound_result = first_partition_struct->GetField(2);
  ASSERT_THAT(lower_bound_result, IsOk());
  auto lower_bound_scalar = lower_bound_result.value();
  ASSERT_TRUE(std::holds_alternative<std::string_view>(lower_bound_scalar));
  auto lower_bound_view = std::get<std::string_view>(lower_bound_scalar);
  EXPECT_EQ(lower_bound_view.size(), 3);
  EXPECT_EQ(static_cast<uint8_t>(lower_bound_view[0]), 0x01);
  EXPECT_EQ(static_cast<uint8_t>(lower_bound_view[1]), 0x02);
  EXPECT_EQ(static_cast<uint8_t>(lower_bound_view[2]), 0x03);

  // Test 2nd partition summary with null fields
  auto second_partition_result = partitions_array->GetElement(1);
  ASSERT_THAT(second_partition_result, IsOk());
  auto second_partition_scalar = second_partition_result.value();
  ASSERT_TRUE(
      std::holds_alternative<std::shared_ptr<StructLike>>(second_partition_scalar));
  auto second_partition_struct =
      std::get<std::shared_ptr<StructLike>>(second_partition_scalar);
  EXPECT_SCALAR_NULL(second_partition_struct->GetField(1));
  EXPECT_SCALAR_NULL(second_partition_struct->GetField(3));
}

TEST(ArrowArrayStructLike, PrimitiveFields) {
  auto struct_type = ::arrow::struct_(
      {::arrow::field("id", ::arrow::int64(), /*nullable=*/false),
       ::arrow::field("name", ::arrow::utf8(), /*nullable=*/true),
       ::arrow::field("score", ::arrow::float32(), /*nullable=*/true),
       ::arrow::field("active", ::arrow::boolean(), /*nullable=*/false),
       ::arrow::field("date", ::arrow::date32(), /*nullable=*/false),
       ::arrow::field("time", ::arrow::time64(::arrow::TimeUnit::MICRO),
                      /*nullable=*/false),
       ::arrow::field("timestamp", ::arrow::timestamp(::arrow::TimeUnit::MICRO),
                      /*nullable=*/false),
       ::arrow::field("fixed", ::arrow::fixed_size_binary(4), /*nullable=*/false),
       ::arrow::field("decimal", ::arrow::decimal128(10, 2), /*nullable=*/false)});

  auto arrow_array = ::arrow::json::ArrayFromJSONString(struct_type, R"([
    {"id": 1, "name": "Alice", "score": 95.5, "active": true, "date": 1714396800,
     "time": 123456, "timestamp": 1714396800000000, "fixed": "aaaa", "decimal": "1234.56"},
    {"id": 2, "name": "Bob", "score": null, "active": false, "date": 1714396801,
     "time": 123457, "timestamp": 1714396800000001, "fixed": "bbbb", "decimal": "-1234.56"},
    {"id": 3, "name": null, "score": 87.2, "active": true, "date": 1714396802,
     "time": 123458, "timestamp": 1714396800000002, "fixed": "cccc", "decimal": "1234.00"}])")
                         .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*struct_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &c_array).ok());

  auto struct_like_result = ArrowArrayStructLike::Make(c_schema, c_array);
  ASSERT_THAT(struct_like_result, IsOk());
  auto struct_like = std::move(struct_like_result.value());

  constexpr int64_t kNumRows = 3;
  std::array<int64_t, kNumRows> ids = {1, 2, 3};
  std::array<std::optional<std::string>, kNumRows> names = {"Alice", "Bob", std::nullopt};
  std::array<std::optional<float>, kNumRows> scores = {95.5f, std::nullopt, 87.2f};
  std::array<bool, kNumRows> actives = {true, false, true};
  std::array<int32_t, kNumRows> dates = {1714396800, 1714396801, 1714396802};
  std::array<int64_t, kNumRows> times = {123456, 123457, 123458};
  std::array<int64_t, kNumRows> timestamps = {1714396800000000, 1714396800000001,
                                              1714396800000002};
  std::array<std::string, kNumRows> fixeds = {"aaaa", "bbbb", "cccc"};
  std::array<std::string, kNumRows> decimals = {"1234.56", "-1234.56", "1234.00"};

  for (int64_t i = 0; i < kNumRows; ++i) {
    ASSERT_THAT(struct_like->Reset(i), IsOk());
    EXPECT_SCALAR_EQ(struct_like->GetField(0), int64_t, ids[i]);
    if (names[i].has_value()) {
      EXPECT_SCALAR_EQ(struct_like->GetField(1), std::string_view, names[i]);
    } else {
      EXPECT_SCALAR_NULL(struct_like->GetField(1));
    }
    if (scores[i].has_value()) {
      EXPECT_SCALAR_EQ(struct_like->GetField(2), float, scores[i].value());
    } else {
      EXPECT_SCALAR_NULL(struct_like->GetField(2));
    }
    EXPECT_SCALAR_EQ(struct_like->GetField(3), bool, actives[i]);
    EXPECT_SCALAR_EQ(struct_like->GetField(4), int32_t, dates[i]);
    EXPECT_SCALAR_EQ(struct_like->GetField(5), int64_t, times[i]);
    EXPECT_SCALAR_EQ(struct_like->GetField(6), int64_t, timestamps[i]);
    EXPECT_SCALAR_EQ(struct_like->GetField(7), std::string_view, fixeds[i]);
    EXPECT_DECIMAL_EQ(struct_like->GetField(8), /*scale=*/2, decimals[i]);
  }
}

TEST(ArrowArrayStructLike, NestedStruct) {
  auto person_type =
      ::arrow::struct_({::arrow::field("name", ::arrow::utf8(), /*nullable=*/false),
                        ::arrow::field("age", ::arrow::int32(), /*nullable=*/false)});
  auto root_type =
      ::arrow::struct_({::arrow::field("id", ::arrow::int64(), /*nullable=*/false),
                        ::arrow::field("person", person_type, /*nullable=*/false)});

  auto arrow_array = ::arrow::json::ArrayFromJSONString(root_type, R"([
    {"id": 1, "person": {"name": "Alice", "age": 30}},
    {"id": 2, "person": {"name": "Bob", "age": 25}}])")
                         .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*root_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &c_array).ok());

  auto struct_like_result = ArrowArrayStructLike::Make(c_schema, c_array);
  ASSERT_THAT(struct_like_result, IsOk());
  auto struct_like = std::move(struct_like_result.value());

  constexpr int64_t kNumRows = 2;
  std::array<int64_t, kNumRows> ids = {1, 2};
  std::array<std::string, kNumRows> names = {"Alice", "Bob"};
  std::array<int32_t, kNumRows> ages = {30, 25};

  for (int64_t i = 0; i < kNumRows; ++i) {
    ASSERT_THAT(struct_like->Reset(i), IsOk());
    EXPECT_EQ(struct_like->num_fields(), 2);
    EXPECT_SCALAR_EQ(struct_like->GetField(0), int64_t, ids[i]);

    auto person_result = struct_like->GetField(1);
    ASSERT_THAT(person_result, IsOk());
    auto person_scalar = person_result.value();
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<StructLike>>(person_scalar));

    auto person_struct = std::get<std::shared_ptr<StructLike>>(person_scalar);
    EXPECT_EQ(person_struct->num_fields(), 2);
    EXPECT_SCALAR_EQ(person_struct->GetField(0), std::string_view, names[i]);
    EXPECT_SCALAR_EQ(person_struct->GetField(1), int32_t, ages[i]);
  }
}

TEST(ArrowArrayStructLike, PrimitiveList) {
  auto list_type =
      ::arrow::list(::arrow::field("item", ::arrow::int32(), /*nullable=*/false));

  auto arrow_array = ::arrow::json::ArrayFromJSONString(list_type, R"([
    [1, 2, 3, 4, 5],
    [10, 20],
    []])")
                         .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*list_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &c_array).ok());

  auto array_like_result = ArrowArrayArrayLike::Make(c_schema, c_array);
  ASSERT_THAT(array_like_result, IsOk());
  auto array_like = std::move(array_like_result.value());

  constexpr int64_t kNumRows = 3;
  std::array<std::vector<int32_t>, kNumRows> expected_lists = {
      std::vector<int32_t>{1, 2, 3, 4, 5},
      std::vector<int32_t>{10, 20},
      std::vector<int32_t>{},
  };

  for (int64_t i = 0; i < kNumRows; ++i) {
    ASSERT_THAT(array_like->Reset(i), IsOk());
    const auto& expected_list = expected_lists[i];
    ASSERT_EQ(array_like->size(), expected_list.size());
    for (size_t j = 0; j < expected_list.size(); ++j) {
      EXPECT_SCALAR_EQ(array_like->GetElement(j), int32_t, expected_list[j]);
    }
  }
}

TEST(ArrowArrayStructLike, PrimitiveMap) {
  auto map_type = std::make_shared<::arrow::MapType>(
      ::arrow::field("key", ::arrow::utf8(), /*nullable=*/false),
      ::arrow::field("value", ::arrow::int32(), /*nullable=*/false));

  auto arrow_array = ::arrow::json::ArrayFromJSONString(map_type, R"([
    [["Foo", 1], ["Bar", 2]],
    [["Baz", 1]],
    []])")
                         .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*map_type, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &c_array).ok());

  auto map_like_result = ArrowArrayMapLike::Make(c_schema, c_array);
  ASSERT_THAT(map_like_result, IsOk());
  auto map_like = std::move(map_like_result.value());

  constexpr int64_t kNumRows = 3;
  std::array<std::vector<std::pair<std::string, int32_t>>, kNumRows> expected_maps = {
      std::vector<std::pair<std::string, int32_t>>{{"Foo", 1}, {"Bar", 2}},
      std::vector<std::pair<std::string, int32_t>>{{"Baz", 1}},
      std::vector<std::pair<std::string, int32_t>>{},
  };

  for (int64_t i = 0; i < kNumRows; ++i) {
    ASSERT_THAT(map_like->Reset(i), IsOk());
    const auto& expected_map = expected_maps[i];
    ASSERT_EQ(map_like->size(), expected_map.size());
    for (size_t j = 0; j < expected_map.size(); ++j) {
      EXPECT_SCALAR_EQ(map_like->GetKey(j), std::string_view, expected_map[j].first);
      EXPECT_SCALAR_EQ(map_like->GetValue(j), int32_t, expected_map[j].second);
    }
  }
}

TEST(ArrowArrayStructLike, Accessor) {
  Schema schema{std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "c1", int32()),
      SchemaField::MakeOptional(
          2, "c2",
          struct_({
              SchemaField::MakeOptional(3, "c3", int32()),
              SchemaField::MakeOptional(4, "c4",
                                        struct_({
                                            SchemaField::MakeOptional(5, "c5", int32()),
                                        })),
          })),
  }};

  auto arrow_schema = ::arrow::struct_({
      ::arrow::field("c1", ::arrow::int32()),
      ::arrow::field("c2",
                     ::arrow::struct_({
                         ::arrow::field("c3", ::arrow::int32()),
                         ::arrow::field("c4", ::arrow::struct_({
                                                  ::arrow::field("c5", ::arrow::int32()),
                                              })),
                     })),
  });

  auto arrow_array =
      ::arrow::json::ArrayFromJSONString(
          arrow_schema, R"([ {"c1": 1, "c2": {"c3": 3, "c4": {"c5": 5}}} ])")
          .ValueOrDie();

  ArrowSchema c_schema;
  ArrowArray c_array;
  internal::ArrowSchemaGuard schema_guard(&c_schema);
  internal::ArrowArrayGuard array_guard(&c_array);
  ASSERT_TRUE(::arrow::ExportType(*arrow_schema, &c_schema).ok());
  ASSERT_TRUE(::arrow::ExportArray(*arrow_array, &c_array).ok());

  ICEBERG_UNWRAP_OR_FAIL(auto struct_like, ArrowArrayStructLike::Make(c_schema, c_array));

  // Test nested accessors from 1 to 3 levels deep
  for (int32_t field_id : {1, 3, 5}) {
    ICEBERG_UNWRAP_OR_FAIL(auto accessor, schema.GetAccessorById(field_id));
    ICEBERG_UNWRAP_OR_FAIL(auto scalar, accessor->Get(*struct_like));
    ASSERT_TRUE(std::holds_alternative<int32_t>(scalar));
    EXPECT_EQ(std::get<int32_t>(scalar), field_id);
  }
}

}  // namespace iceberg
