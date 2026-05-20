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

#include "iceberg/data/delete_filter.h"

#include <format>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/data/equality_delete_writer.h"
#include "iceberg/data/position_delete_writer.h"
#include "iceberg/file_format.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {

namespace {

struct ExportedBatch {
  ArrowSchema schema{};
  ArrowArray array{};

  ExportedBatch() = default;
  ~ExportedBatch() {
    if (array.release != nullptr) {
      array.release(&array);
    }
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }

  ExportedBatch(const ExportedBatch&) = delete;
  ExportedBatch& operator=(const ExportedBatch&) = delete;

  ExportedBatch(ExportedBatch&& other) noexcept
      : schema(other.schema), array(other.array) {
    other.schema.release = nullptr;
    other.array.release = nullptr;
  }
  ExportedBatch& operator=(ExportedBatch&& other) noexcept = delete;
};

std::vector<std::string> FieldNames(const Schema& schema) {
  std::vector<std::string> names;
  for (const auto& field : schema.fields()) {
    names.emplace_back(field.name());
  }
  return names;
}

std::vector<int32_t> FieldIds(const Schema& schema) {
  std::vector<int32_t> ids;
  for (const auto& field : schema.fields()) {
    ids.push_back(field.field_id());
  }
  return ids;
}

std::vector<int32_t> StructFieldIds(const StructType& struct_type) {
  std::vector<int32_t> ids;
  for (const auto& field : struct_type.fields()) {
    ids.push_back(field.field_id());
  }
  return ids;
}

void ExpectAliveRows(const AliveRowSelection& alive,
                     const std::vector<int32_t>& expected) {
  ASSERT_EQ(alive.alive_count(), static_cast<int64_t>(expected.size()));
  EXPECT_EQ(alive.indices, expected);
}

class CapturingReader : public Reader {
 public:
  explicit CapturingReader(std::shared_ptr<iceberg::Schema>* projection)
      : projection_(projection) {}

  Status Open(const ReaderOptions& options) override {
    *projection_ = options.projection;
    return {};
  }

  Status Close() override { return {}; }

  Result<std::optional<ArrowArray>> Next() override { return std::nullopt; }

  Result<ArrowSchema> Schema() override {
    ArrowSchema schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(**projection_, &schema));
    return schema;
  }

  Result<std::unordered_map<std::string, std::string>> Metadata() override {
    return std::unordered_map<std::string, std::string>{};
  }

 private:
  std::shared_ptr<iceberg::Schema>* projection_;
};

class ScopedReaderFactory {
 public:
  ScopedReaderFactory(FileFormatType format_type, ReaderFactory factory)
      : format_type_(format_type),
        previous_(ReaderFactoryRegistry::GetFactory(format_type)) {
    ReaderFactoryRegistry::GetFactory(format_type_) = std::move(factory);
  }

  ~ScopedReaderFactory() {
    ReaderFactoryRegistry::GetFactory(format_type_) = std::move(previous_);
  }

 private:
  FileFormatType format_type_;
  ReaderFactory previous_;
};

}  // namespace

class DeleteFilterTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    table_schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string()),
                                 SchemaField::MakeOptional(3, "category", string())});
    partition_spec_ = PartitionSpec::Unpartitioned();
  }

  std::shared_ptr<Schema> Project(std::initializer_list<int32_t> field_ids) const {
    std::unordered_set<int32_t> ids(field_ids.begin(), field_ids.end());
    auto result = table_schema_->Project(ids);
    EXPECT_TRUE(result.has_value()) << "Projection failed: " << result.error().message;
    return std::move(result.value());
  }

  Result<ExportedBatch> MakeBatch(const Schema& schema,
                                  const std::string& json_data) const {
    ArrowSchema type_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &type_schema));
    auto arrow_type_result = ::arrow::ImportType(&type_schema);
    if (!arrow_type_result.ok()) {
      return UnknownError(arrow_type_result.status().ToString());
    }
    auto struct_type = ::arrow::struct_(arrow_type_result.MoveValueUnsafe()->fields());
    auto array_result = ::arrow::json::ArrayFromJSONString(struct_type, json_data);
    if (!array_result.ok()) {
      return UnknownError(array_result.status().ToString());
    }

    ExportedBatch batch;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(schema, &batch.schema));
    auto export_status =
        ::arrow::ExportArray(*array_result.MoveValueUnsafe(), &batch.array);
    if (!export_status.ok()) {
      return UnknownError(export_status.ToString());
    }
    return batch;
  }

  Result<std::shared_ptr<DataFile>> PositionDeleteFile(
      const std::string& path, const std::vector<int64_t>& positions,
      const std::string& data_path = std::string(kDataPath)) {
    PositionDeleteWriterOptions options{
        .path = path,
        .schema = table_schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .flush_threshold = 10000,
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer, PositionDeleteWriter::Make(options));
    for (int64_t pos : positions) {
      ICEBERG_RETURN_UNEXPECTED(writer->WriteDelete(data_path, pos));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files[0];
  }

  Result<std::shared_ptr<DataFile>> EqualityDeleteFile(
      const std::string& path, const std::string& json_data,
      std::vector<int32_t> equality_field_ids) {
    EqualityDeleteWriterOptions options{
        .path = path,
        .schema = table_schema_,
        .spec = partition_spec_,
        .partition = PartitionValues{},
        .format = FileFormatType::kParquet,
        .io = file_io_,
        .equality_field_ids = std::move(equality_field_ids),
        .properties = {{"write.parquet.compression-codec", "uncompressed"}},
    };

    ICEBERG_ASSIGN_OR_RAISE(auto writer, EqualityDeleteWriter::Make(options));
    ICEBERG_ASSIGN_OR_RAISE(auto batch, MakeBatch(*table_schema_, json_data));
    ICEBERG_RETURN_UNEXPECTED(writer->Write(&batch.array));
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return metadata.data_files[0];
  }

  static constexpr std::string_view kDataPath = "data.parquet";

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<Schema> table_schema_;
  std::shared_ptr<PartitionSpec> partition_spec_;
};

enum class RequiredSchemaRequest {
  kProjectFields,
  kIdAndRowPos,
};

struct RequiredSchemaCase {
  const char* name;
  RequiredSchemaRequest request;
  std::vector<int32_t> requested_field_ids;
  std::vector<std::vector<int32_t>> equality_ids_by_file;
  bool has_pos_delete;
  bool need_row_pos_col;
  std::vector<int32_t> expected_field_ids;
  std::vector<std::string> expected_field_names;
  bool expected_has_position_deletes;
  bool expected_has_equality_deletes;
};

template <typename Param>
std::string ParamName(const testing::TestParamInfo<Param>& info) {
  return info.param.name;
}

class DeleteFilterRequiredSchemaTest
    : public DeleteFilterTest,
      public testing::WithParamInterface<RequiredSchemaCase> {
 protected:
  std::shared_ptr<Schema> RequestedSchema(const RequiredSchemaCase& test_case) {
    switch (test_case.request) {
      case RequiredSchemaRequest::kProjectFields: {
        std::unordered_set<int32_t> ids(test_case.requested_field_ids.begin(),
                                        test_case.requested_field_ids.end());
        auto result = table_schema_->Project(ids);
        EXPECT_TRUE(result.has_value())
            << "Projection failed: " << result.error().message;
        return std::move(result.value());
      }
      case RequiredSchemaRequest::kIdAndRowPos:
        return std::make_shared<Schema>(std::vector<SchemaField>{
            SchemaField::MakeRequired(1, "id", int32()), MetadataColumns::kRowPosition});
    }
    return nullptr;
  }

  std::vector<std::shared_ptr<DataFile>> DeleteFiles(
      const RequiredSchemaCase& test_case) {
    std::vector<std::shared_ptr<DataFile>> delete_files;
    for (size_t index = 0; index < test_case.equality_ids_by_file.size(); ++index) {
      delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kEqualityDeletes,
          .file_path = std::format("{}-eq-{}.parquet", test_case.name, index),
          .file_format = FileFormatType::kParquet,
          .equality_ids = test_case.equality_ids_by_file[index],
      }));
    }
    if (test_case.has_pos_delete) {
      delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kPositionDeletes,
          .file_path = std::format("{}-pos.parquet", test_case.name),
          .file_format = FileFormatType::kParquet,
      }));
    }
    return delete_files;
  }
};

TEST_P(DeleteFilterRequiredSchemaTest, ComputesRequiredSchema) {
  const auto& test_case = GetParam();
  auto delete_files = DeleteFiles(test_case);
  auto requested_schema = RequestedSchema(test_case);

  auto filter =
      DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                         requested_schema, file_io_, test_case.need_row_pos_col);

  ASSERT_THAT(filter, IsOk());
  EXPECT_EQ(filter.value()->HasPositionDeletes(),
            test_case.expected_has_position_deletes);
  EXPECT_EQ(filter.value()->HasEqualityDeletes(),
            test_case.expected_has_equality_deletes);
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()),
              testing::ElementsAreArray(test_case.expected_field_ids));
  EXPECT_THAT(FieldNames(*filter.value()->RequiredSchema()),
              testing::ElementsAreArray(test_case.expected_field_names));
}

INSTANTIATE_TEST_SUITE_P(
    RequiredSchema, DeleteFilterRequiredSchemaTest,
    testing::Values(
        RequiredSchemaCase{
            .name = "UnchangedWithoutDeletes",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {2, 1},
            .equality_ids_by_file = {},
            .has_pos_delete = false,
            .need_row_pos_col = true,
            .expected_field_ids = {1, 2},
            .expected_field_names = {"id", "name"},
            .expected_has_position_deletes = false,
            .expected_has_equality_deletes = false,
        },
        RequiredSchemaCase{
            .name = "AddsEqualityFieldsAndRowPos",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {1},
            .equality_ids_by_file = {{2}, {3, 1}},
            .has_pos_delete = true,
            .need_row_pos_col = true,
            .expected_field_ids = {1, 2, 3, MetadataColumns::kFilePositionColumnId},
            .expected_field_names = {"id", "name", "category",
                                     std::string(MetadataColumns::kRowPosition.name())},
            .expected_has_position_deletes = true,
            .expected_has_equality_deletes = true,
        },
        RequiredSchemaCase{
            .name = "AddsEqualityFieldsInDeclaredOrder",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {1},
            .equality_ids_by_file = {{3, 2}},
            .has_pos_delete = false,
            .need_row_pos_col = true,
            .expected_field_ids = {1, 3, 2},
            .expected_field_names = {"id", "category", "name"},
            .expected_has_position_deletes = false,
            .expected_has_equality_deletes = true,
        },
        RequiredSchemaCase{
            .name = "DeduplicatesRowPos",
            .request = RequiredSchemaRequest::kIdAndRowPos,
            .requested_field_ids = {},
            .equality_ids_by_file = {},
            .has_pos_delete = true,
            .need_row_pos_col = true,
            .expected_field_ids = {1, MetadataColumns::kFilePositionColumnId},
            .expected_field_names = {"id",
                                     std::string(MetadataColumns::kRowPosition.name())},
            .expected_has_position_deletes = true,
            .expected_has_equality_deletes = false,
        },
        RequiredSchemaCase{
            .name = "NeedRowPosColFalseOmitsPos",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {1},
            .equality_ids_by_file = {},
            .has_pos_delete = true,
            .need_row_pos_col = false,
            .expected_field_ids = {1},
            .expected_field_names = {"id"},
            .expected_has_position_deletes = true,
            .expected_has_equality_deletes = false,
        },
        RequiredSchemaCase{
            .name = "NeedRowPosColTrueAppendsPos",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {1},
            .equality_ids_by_file = {},
            .has_pos_delete = true,
            .need_row_pos_col = true,
            .expected_field_ids = {1, MetadataColumns::kFilePositionColumnId},
            .expected_field_names = {"id",
                                     std::string(MetadataColumns::kRowPosition.name())},
            .expected_has_position_deletes = true,
            .expected_has_equality_deletes = false,
        },
        RequiredSchemaCase{
            .name = "AddsFieldsInJavaOrder",
            .request = RequiredSchemaRequest::kProjectFields,
            .requested_field_ids = {1},
            .equality_ids_by_file = {{2}, {3}},
            .has_pos_delete = true,
            .need_row_pos_col = true,
            .expected_field_ids = {1, 2, 3, MetadataColumns::kFilePositionColumnId},
            .expected_field_names = {"id", "name", "category",
                                     std::string(MetadataColumns::kRowPosition.name())},
            .expected_has_position_deletes = true,
            .expected_has_equality_deletes = true,
        }),
    ParamName<RequiredSchemaCase>);

TEST_F(DeleteFilterTest, EqualityFieldsCanBeTopLevelPrimitiveOrNestedPrimitive) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info", struct_({SchemaField::MakeOptional(5, "city", string())}))});
  auto requested_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  auto eq_by_struct = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-id.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {1},
  });

  std::vector<std::shared_ptr<DataFile>> top_level_primitive_delete = {eq_by_struct};
  auto top_level_filter =
      DeleteFilter::Make(std::string(kDataPath), top_level_primitive_delete,
                         nested_schema, requested_schema, file_io_);
  ASSERT_THAT(top_level_filter, IsOk());
  EXPECT_THAT(FieldIds(*top_level_filter.value()->RequiredSchema()),
              testing::ElementsAre(1));

  auto eq_by_nested_field = std::make_shared<DataFile>(*eq_by_struct);
  eq_by_nested_field->equality_ids = {5};
  std::vector<std::shared_ptr<DataFile>> nested_delete = {eq_by_nested_field};

  auto nested_filter = DeleteFilter::Make(std::string(kDataPath), nested_delete,
                                          nested_schema, requested_schema, file_io_);

  ASSERT_THAT(nested_filter, IsOk());
  EXPECT_THAT(FieldIds(*nested_filter.value()->RequiredSchema()),
              testing::ElementsAre(1, 4));
}

TEST_F(DeleteFilterTest, RequiredSchemaMergesNestedSibling) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info",
          struct_({SchemaField::MakeOptional(5, "city", string()),
                   SchemaField::MakeOptional(6, "state", string())}))});
  auto requested_schema = std::shared_ptr<Schema>(
      nested_schema->Project(std::unordered_set<int32_t>{1, 5}).value());
  auto eq_by_state = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-state.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {6},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_state};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);

  ASSERT_THAT(filter, IsOk());
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 4));
  const auto& info = filter.value()->RequiredSchema()->fields()[1];
  auto info_type = std::dynamic_pointer_cast<StructType>(info.type());
  ASSERT_NE(info_type, nullptr);
  EXPECT_THAT(StructFieldIds(*info_type), testing::ElementsAre(5, 6));
}

TEST_F(DeleteFilterTest, StructEqualityFieldErrors) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info",
          struct_({SchemaField::MakeOptional(5, "city", string()),
                   SchemaField::MakeOptional(6, "state", string())}))});
  auto requested_schema = std::shared_ptr<Schema>(
      nested_schema->Project(std::unordered_set<int32_t>{1, 5}).value());
  auto eq_by_info = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-info.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {4},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_info};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(filter, HasErrorMessage("must reference a primitive field"));
}

enum class AliveRowsDeleteKind {
  kNone,
  kPosition,
  kEqualityName,
  kEqualityNameAndCategory,
  kMixedPositionAndEqualityId,
};

enum class CounterMode {
  kNone,
  kAttachCounter,
  kNullCounter,
};

struct AliveRowsCase {
  const char* name;
  AliveRowsDeleteKind delete_kind;
  std::vector<int64_t> position_delete_positions;
  std::string position_delete_data_path;
  bool need_row_pos_col;
  CounterMode counter_mode;
  std::string batch_json;
  std::vector<int32_t> expected_alive_rows;
  std::optional<int64_t> expected_delete_count;
};

class DeleteFilterAliveRowsTest : public DeleteFilterTest,
                                  public testing::WithParamInterface<AliveRowsCase> {};

TEST_P(DeleteFilterAliveRowsTest, ComputesAliveRows) {
  const auto& test_case = GetParam();
  std::vector<std::shared_ptr<DataFile>> delete_files;
  switch (test_case.delete_kind) {
    case AliveRowsDeleteKind::kNone:
      break;
    case AliveRowsDeleteKind::kPosition: {
      auto data_path = test_case.position_delete_data_path.empty()
                           ? std::string(kDataPath)
                           : test_case.position_delete_data_path;
      ICEBERG_UNWRAP_OR_FAIL(
          auto pos_delete,
          PositionDeleteFile(std::format("{}-pos.parquet", test_case.name),
                             test_case.position_delete_positions, data_path));
      delete_files.push_back(pos_delete);
      break;
    }
    case AliveRowsDeleteKind::kEqualityName: {
      ICEBERG_UNWRAP_OR_FAIL(
          auto eq_by_name,
          EqualityDeleteFile(std::format("{}-eq-name.parquet", test_case.name),
                             R"([[0, "Bob", "unused"]])", {2}));
      delete_files.push_back(eq_by_name);
      break;
    }
    case AliveRowsDeleteKind::kEqualityNameAndCategory: {
      ICEBERG_UNWRAP_OR_FAIL(
          auto eq_by_name,
          EqualityDeleteFile(std::format("{}-eq-name.parquet", test_case.name),
                             R"([[0, "Bob", "unused"]])", {2}));
      ICEBERG_UNWRAP_OR_FAIL(
          auto eq_by_category,
          EqualityDeleteFile(std::format("{}-eq-category.parquet", test_case.name),
                             R"([[0, "unused", "red"]])", {3}));
      delete_files.push_back(eq_by_name);
      delete_files.push_back(eq_by_category);
      break;
    }
    case AliveRowsDeleteKind::kMixedPositionAndEqualityId: {
      ICEBERG_UNWRAP_OR_FAIL(
          auto pos_delete,
          PositionDeleteFile(std::format("{}-pos.parquet", test_case.name),
                             test_case.position_delete_positions));
      ICEBERG_UNWRAP_OR_FAIL(
          auto eq_by_id,
          EqualityDeleteFile(std::format("{}-eq-id.parquet", test_case.name),
                             R"([[3, "unused", "unused"]])", {1}));
      delete_files.push_back(pos_delete);
      delete_files.push_back(eq_by_id);
      break;
    }
  }

  auto requested_schema = Project({1});
  std::shared_ptr<DeleteCounter> counter;
  if (test_case.counter_mode == CounterMode::kAttachCounter) {
    counter = std::make_shared<DeleteCounter>();
  }
  auto filter =
      DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                         requested_schema, file_io_, test_case.need_row_pos_col, counter);
  ASSERT_THAT(filter, IsOk());
  ICEBERG_UNWRAP_OR_FAIL(
      auto batch, MakeBatch(*filter.value()->RequiredSchema(), test_case.batch_json));

  auto alive = filter.value()->ComputeAliveRows(batch.schema, batch.array);

  ASSERT_THAT(alive, IsOk());
  ExpectAliveRows(alive.value(), test_case.expected_alive_rows);
  if (test_case.expected_delete_count.has_value()) {
    ASSERT_NE(counter, nullptr);
    EXPECT_EQ(counter->Get(), test_case.expected_delete_count.value());
  }
}

INSTANTIATE_TEST_SUITE_P(
    AliveRows, DeleteFilterAliveRowsTest,
    testing::Values(
        AliveRowsCase{
            .name = "AllReturnedWithoutDeletes",
            .delete_kind = AliveRowsDeleteKind::kNone,
            .position_delete_positions = {},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([[1], [2], [3]])",
            .expected_alive_rows = {0, 1, 2},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "PositionDeletesFilterByRowPos",
            .delete_kind = AliveRowsDeleteKind::kPosition,
            .position_delete_positions = {1, 3},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([[10, 0], [20, 1], [30, 2], [40, 3]])",
            .expected_alive_rows = {0, 2},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "EqualityDeletesApplyOrSemantics",
            .delete_kind = AliveRowsDeleteKind::kEqualityNameAndCategory,
            .position_delete_positions = {},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json =
                R"([[1, "Alice", "blue"], [2, "Bob", "blue"], [3, "Carol", "red"], [4, "Dan", "green"]])",
            .expected_alive_rows = {0, 3},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "MixedDeletesPosBeforeEqCanDeleteAll",
            .delete_kind = AliveRowsDeleteKind::kMixedPositionAndEqualityId,
            .position_delete_positions = {0, 1},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([[1, 0], [2, 1], [3, 2]])",
            .expected_alive_rows = {},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "EmptyBatchReturnsEmptyBitmap",
            .delete_kind = AliveRowsDeleteKind::kNone,
            .position_delete_positions = {},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([])",
            .expected_alive_rows = {},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "NeedRowPosColFalseSkipsPosFiltering",
            .delete_kind = AliveRowsDeleteKind::kPosition,
            .position_delete_positions = {0, 1},
            .position_delete_data_path = "",
            .need_row_pos_col = false,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([[10], [20], [30]])",
            .expected_alive_rows = {0, 1, 2},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "CounterCountsPosDeletes",
            .delete_kind = AliveRowsDeleteKind::kPosition,
            .position_delete_positions = {0, 2},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kAttachCounter,
            .batch_json = R"([[10, 0], [20, 1], [30, 2], [40, 3]])",
            .expected_alive_rows = {1, 3},
            .expected_delete_count = 2,
        },
        AliveRowsCase{
            .name = "CounterCountsEqDeletes",
            .delete_kind = AliveRowsDeleteKind::kEqualityName,
            .position_delete_positions = {},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kAttachCounter,
            .batch_json = R"([[1, "Alice"], [2, "Bob"], [3, "Bob"], [4, "Dan"]])",
            .expected_alive_rows = {0, 3},
            .expected_delete_count = 2,
        },
        AliveRowsCase{
            .name = "NullCounterIsNoOp",
            .delete_kind = AliveRowsDeleteKind::kPosition,
            .position_delete_positions = {0},
            .position_delete_data_path = "",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNullCounter,
            .batch_json = R"([[10, 0], [20, 1]])",
            .expected_alive_rows = {1},
            .expected_delete_count = std::nullopt,
        },
        AliveRowsCase{
            .name = "PosDeleteOnlyFiltersMatchingPath",
            .delete_kind = AliveRowsDeleteKind::kPosition,
            .position_delete_positions = {0, 1, 2},
            .position_delete_data_path = "other-data.parquet",
            .need_row_pos_col = true,
            .counter_mode = CounterMode::kNone,
            .batch_json = R"([[10, 0], [20, 1], [30, 2]])",
            .expected_alive_rows = {0, 1, 2},
            .expected_delete_count = std::nullopt,
        }),
    ParamName<AliveRowsCase>);

TEST_F(DeleteFilterTest, TopLevelStructEqualityErrors) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info", struct_({SchemaField::MakeOptional(5, "city", string())}))});
  auto requested_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  auto eq_by_info = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-info.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {4},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_info};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(filter, HasErrorMessage("must reference a primitive field"));
}

TEST_F(DeleteFilterTest, NestedStructFieldEqualityFiltersRows) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info",
          struct_({SchemaField::MakeOptional(5, "city", string()),
                   SchemaField::MakeOptional(6, "state", string())}))});
  auto requested_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  EqualityDeleteWriterOptions options{
      .path = "eq-city.parquet",
      .schema = nested_schema,
      .spec = partition_spec_,
      .partition = PartitionValues{},
      .format = FileFormatType::kParquet,
      .io = file_io_,
      .equality_field_ids = {5},
      .properties = {{"write.parquet.compression-codec", "uncompressed"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto writer, EqualityDeleteWriter::Make(options));
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_batch,
      MakeBatch(*nested_schema,
                R"([{"id": 0, "info": {"city": "Paris", "state": "FR"}}])"));
  ASSERT_THAT(writer->Write(&delete_batch.array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto eq_by_city_meta, writer->Metadata());
  auto eq_by_city = eq_by_city_meta.data_files[0];

  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_city};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 4));

  ICEBERG_UNWRAP_OR_FAIL(auto data_batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([{"id": 1, "info": {"city": "London"}},
                                  {"id": 2, "info": {"city": "Paris"}},
                                  {"id": 3, "info": null}])"));

  auto alive = filter.value()->ComputeAliveRows(data_batch.schema, data_batch.array);

  ASSERT_THAT(alive, IsOk());
  ExpectAliveRows(alive.value(), {0, 2});
}

TEST_F(DeleteFilterTest, NestedEqualityWithPartialStructNoOverDelete) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info",
          struct_({SchemaField::MakeOptional(5, "city", string()),
                   SchemaField::MakeOptional(6, "state", string())}))});
  auto requested_schema = std::shared_ptr<Schema>(
      nested_schema->Project(std::unordered_set<int32_t>{1, 5}).value());

  EqualityDeleteWriterOptions options{
      .path = "eq-state-partial.parquet",
      .schema = nested_schema,
      .spec = partition_spec_,
      .partition = PartitionValues{},
      .format = FileFormatType::kParquet,
      .io = file_io_,
      .equality_field_ids = {6},
      .properties = {{"write.parquet.compression-codec", "uncompressed"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto writer, EqualityDeleteWriter::Make(options));
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_batch,
      MakeBatch(*nested_schema,
                R"([{"id": 0, "info": {"city": "ignored", "state": "CA"}}])"));
  ASSERT_THAT(writer->Write(&delete_batch.array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto eq_by_state_meta, writer->Metadata());
  auto eq_by_state = eq_by_state_meta.data_files[0];

  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_state};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto data_batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([{"id": 1, "info": {"city": "SF", "state": "CA"}},
                                  {"id": 2, "info": {"city": "NYC", "state": "NY"}},
                                  {"id": 3, "info": null}])"));

  auto alive = filter.value()->ComputeAliveRows(data_batch.schema, data_batch.array);

  ASSERT_THAT(alive, IsOk());
  ExpectAliveRows(alive.value(), {1, 2});
}

TEST_F(DeleteFilterTest, EqualityDeleteProjectionSortsNestedFieldsById) {
  auto nested_schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(
          4, "info",
          struct_({SchemaField::MakeOptional(6, "state", string()),
                   SchemaField::MakeOptional(5, "city", string())}))});
  auto requested_schema = std::shared_ptr<Schema>(
      nested_schema->Project(std::unordered_set<int32_t>{1}).value());
  auto eq_delete = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-city-state.orc",
      .file_format = FileFormatType::kOrc,
      .equality_ids = {6, 5},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_delete};

  std::shared_ptr<Schema> captured_projection;
  ScopedReaderFactory reader_factory(
      FileFormatType::kOrc, [&captured_projection]() -> Result<std::unique_ptr<Reader>> {
        return std::make_unique<CapturingReader>(&captured_projection);
      });

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, nested_schema,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());
  ASSERT_THAT(filter.value()->EqDeletedRowFilter(), IsOk());

  ASSERT_NE(captured_projection, nullptr);
  ASSERT_EQ(captured_projection->fields().size(), 1);
  auto info_type =
      std::dynamic_pointer_cast<StructType>(captured_projection->fields()[0].type());
  ASSERT_NE(info_type, nullptr);
  EXPECT_THAT(StructFieldIds(*info_type), testing::ElementsAre(5, 6));
}

TEST_F(DeleteFilterTest, DroppedTopLevelFieldResolvedBySchemas) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())},
      /*schema_id=*/2);
  auto historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "dropped_value", string())},
      /*schema_id=*/1);
  auto requested_schema = current_schema;
  auto eq_by_dropped = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-dropped.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {7},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_dropped};
  std::vector<std::shared_ptr<Schema>> schemas = {current_schema, historic_schema};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, current_schema,
                                   requested_schema, file_io_, schemas);

  ASSERT_THAT(filter, IsOk());
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 7));
  EXPECT_THAT(FieldNames(*filter.value()->RequiredSchema()),
              testing::ElementsAre("id", "dropped_value"));
}

TEST_F(DeleteFilterTest, MakeFieldLookupSchemasMayIncludeCurrent) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "current_value", string())},
      /*schema_id=*/2);
  auto old_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "old_value", int32())},
      /*schema_id=*/1);
  std::vector<std::shared_ptr<Schema>> schemas = {old_schema, current_schema};

  auto lookup_result = DeleteFilter::MakeFieldLookup(current_schema, schemas);
  ASSERT_THAT(lookup_result, IsOk());

  auto field_result = lookup_result.value()(7);
  ASSERT_THAT(field_result, IsOk());
  ASSERT_TRUE(field_result.value().has_value());
  EXPECT_EQ(field_result.value()->field.name(), "current_value");
  EXPECT_EQ(field_result.value()->field.type()->type_id(), TypeId::kString);
}

TEST_F(DeleteFilterTest, MakeFieldLookupCurrentSchemaWins) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "current_value", string())},
      /*schema_id=*/3);
  auto fallback_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "fallback_value", int32())},
      /*schema_id=*/2);
  std::vector<std::shared_ptr<Schema>> schemas = {fallback_schema};

  auto lookup_result = DeleteFilter::MakeFieldLookup(current_schema, schemas);
  ASSERT_THAT(lookup_result, IsOk());

  auto field_result = lookup_result.value()(7);
  ASSERT_THAT(field_result, IsOk());
  ASSERT_TRUE(field_result.value().has_value());
  EXPECT_EQ(field_result.value()->field.name(), "current_value");
  EXPECT_EQ(field_result.value()->field.type()->type_id(), TypeId::kString);
}

TEST_F(DeleteFilterTest, MakeFieldLookupLatestFallbackSchemaWins) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())},
      /*schema_id=*/3);
  auto older_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "old_value", int32())},
      /*schema_id=*/1);
  auto newer_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "new_value", string())},
      /*schema_id=*/2);
  std::vector<std::shared_ptr<Schema>> schemas = {older_schema, newer_schema};

  auto lookup_result = DeleteFilter::MakeFieldLookup(current_schema, schemas);
  ASSERT_THAT(lookup_result, IsOk());

  auto field_result = lookup_result.value()(7);
  ASSERT_THAT(field_result, IsOk());
  ASSERT_TRUE(field_result.value().has_value());
  EXPECT_EQ(field_result.value()->field.name(), "new_value");
  EXPECT_EQ(field_result.value()->field.type()->type_id(), TypeId::kString);
}

TEST_F(DeleteFilterTest, DroppedNestedFieldResolvedBySchemas) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeOptional(
              4, "info", struct_({SchemaField::MakeOptional(5, "city", string())}))},
      /*schema_id=*/2);
  auto historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeOptional(
              4, "info",
              struct_({SchemaField::MakeOptional(5, "city", string()),
                       SchemaField::MakeOptional(6, "state", string())}))},
      /*schema_id=*/1);
  auto requested_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});
  auto eq_by_dropped_nested = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-dropped-state.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {6},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_dropped_nested};
  std::vector<std::shared_ptr<Schema>> schemas = {current_schema, historic_schema};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, current_schema,
                                   requested_schema, file_io_, schemas);

  ASSERT_THAT(filter, IsOk());
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 4));
  const auto& info = filter.value()->RequiredSchema()->fields()[1];
  auto info_type = std::dynamic_pointer_cast<StructType>(info.type());
  ASSERT_NE(info_type, nullptr);
  EXPECT_THAT(StructFieldIds(*info_type), testing::ElementsAre(6));
}

TEST_F(DeleteFilterTest, MetadataLookupUsesSchemas) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())},
      /*schema_id=*/2);
  auto historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "dropped_value", string())},
      /*schema_id=*/1);
  auto metadata = std::make_shared<TableMetadata>(TableMetadata{
      .format_version = TableMetadata::kDefaultTableFormatVersion,
      .schemas = {historic_schema, current_schema},
      .current_schema_id = current_schema->schema_id(),
  });
  auto requested_schema = current_schema;
  auto eq_by_dropped = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-dropped.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {7},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_dropped};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, metadata,
                                   requested_schema, file_io_);

  ASSERT_THAT(filter, IsOk());
  EXPECT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 7));
}

TEST_F(DeleteFilterTest, MetadataLookupPrefersLatestFallbackSchema) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())},
      /*schema_id=*/3);
  auto older_historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "old_name", int32())},
      /*schema_id=*/1);
  auto newer_historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "new_name", string())},
      /*schema_id=*/2);
  auto metadata = std::make_shared<TableMetadata>(TableMetadata{
      .format_version = TableMetadata::kDefaultTableFormatVersion,
      .schemas = {older_historic_schema, newer_historic_schema, current_schema},
      .current_schema_id = current_schema->schema_id(),
  });
  auto eq_by_dropped = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-dropped.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {7},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_dropped};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, metadata,
                                   current_schema, file_io_);

  ASSERT_THAT(filter, IsOk());
  ASSERT_THAT(FieldIds(*filter.value()->RequiredSchema()), testing::ElementsAre(1, 7));
  const auto& dropped_field = filter.value()->RequiredSchema()->fields()[1];
  EXPECT_EQ(dropped_field.name(), "new_name");
  EXPECT_EQ(dropped_field.type()->type_id(), TypeId::kString);
}

TEST_F(DeleteFilterTest, DroppedNestedFieldFiltersRowsWithSchemas) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeOptional(
              4, "info", struct_({SchemaField::MakeOptional(5, "city", string())}))},
      /*schema_id=*/2);
  auto historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeOptional(
              4, "info",
              struct_({SchemaField::MakeOptional(5, "city", string()),
                       SchemaField::MakeOptional(6, "state", string())}))},
      /*schema_id=*/1);
  auto requested_schema = current_schema;

  EqualityDeleteWriterOptions options{
      .path = "eq-dropped-state-filter.parquet",
      .schema = historic_schema,
      .spec = partition_spec_,
      .partition = PartitionValues{},
      .format = FileFormatType::kParquet,
      .io = file_io_,
      .equality_field_ids = {6},
      .properties = {{"write.parquet.compression-codec", "uncompressed"}},
  };
  ICEBERG_UNWRAP_OR_FAIL(auto writer, EqualityDeleteWriter::Make(options));
  ICEBERG_UNWRAP_OR_FAIL(
      auto delete_batch,
      MakeBatch(*historic_schema,
                R"([{"id": 0, "info": {"city": "ignored", "state": "CA"}}])"));
  ASSERT_THAT(writer->Write(&delete_batch.array), IsOk());
  ASSERT_THAT(writer->Close(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto eq_by_state_meta, writer->Metadata());
  auto eq_by_state = eq_by_state_meta.data_files[0];
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_state};
  std::vector<std::shared_ptr<Schema>> schemas = {current_schema, historic_schema};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, current_schema,
                                   requested_schema, file_io_, schemas);
  ASSERT_THAT(filter, IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto data_batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([{"id": 1, "info": {"city": "SF", "state": "CA"}},
                                  {"id": 2, "info": {"city": "NYC", "state": "NY"}},
                                  {"id": 3, "info": null}])"));

  auto alive = filter.value()->ComputeAliveRows(data_batch.schema, data_batch.array);

  ASSERT_THAT(alive, IsOk());
  ExpectAliveRows(alive.value(), {1, 2});
}

TEST_F(DeleteFilterTest, DeletionVectorErrorPropagatesFromCompute) {
  auto dv_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "dv.puffin",
      .file_format = FileFormatType::kPuffin,
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {dv_file};
  auto requested_schema = Project({1});

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);

  ASSERT_THAT(filter, IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch,
                         MakeBatch(*filter.value()->RequiredSchema(), R"([[1, 0]])"));
  auto alive = filter.value()->ComputeAliveRows(batch.schema, batch.array);
  ASSERT_THAT(alive, IsError(ErrorKind::kNotSupported));
}

TEST_F(DeleteFilterTest, EmptyBatchPropagatesDeleteLoadErrors) {
  auto dv_file = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "dv-empty.puffin",
      .file_format = FileFormatType::kPuffin,
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {dv_file};
  auto requested_schema = Project({1});
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch,
                         MakeBatch(*filter.value()->RequiredSchema(), R"([])"));

  auto alive = filter.value()->ComputeAliveRows(batch.schema, batch.array);

  ASSERT_THAT(alive, IsError(ErrorKind::kNotSupported));
}

TEST_F(DeleteFilterTest, CounterAccumulatesAcrossBatches) {
  ICEBERG_UNWRAP_OR_FAIL(auto pos_delete,
                         PositionDeleteFile("pos-multi-batch.parquet", {1}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {pos_delete};
  auto requested_schema = Project({1});
  auto counter = std::make_shared<DeleteCounter>();
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_,
                                   /*need_row_pos_col=*/true, counter);
  ASSERT_THAT(filter, IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto batch1, MakeBatch(*filter.value()->RequiredSchema(),
                                                R"([[10, 0], [20, 1], [30, 2]])"));
  ICEBERG_UNWRAP_OR_FAIL(
      auto batch2, MakeBatch(*filter.value()->RequiredSchema(), R"([[40, 3], [50, 4]])"));

  ASSERT_THAT(filter.value()->ComputeAliveRows(batch1.schema, batch1.array), IsOk());
  ASSERT_THAT(filter.value()->ComputeAliveRows(batch2.schema, batch2.array), IsOk());
  EXPECT_EQ(counter->Get(), 1);
}

enum class MakeErrorDeleteKind {
  kNullDeleteFile,
  kDataFile,
  kEqualityDeleteWithEmptyIds,
  kUnknownEqualityFieldId,
};

struct MakeErrorCase {
  const char* name;
  MakeErrorDeleteKind delete_kind;
};

class DeleteFilterMakeErrorTest : public DeleteFilterTest,
                                  public testing::WithParamInterface<MakeErrorCase> {};

TEST_P(DeleteFilterMakeErrorTest, InvalidDeleteFilesError) {
  const auto& test_case = GetParam();
  std::vector<std::shared_ptr<DataFile>> delete_files;
  switch (test_case.delete_kind) {
    case MakeErrorDeleteKind::kNullDeleteFile:
      delete_files.push_back(nullptr);
      break;
    case MakeErrorDeleteKind::kDataFile:
      delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kData,
          .file_path = "data.parquet",
          .file_format = FileFormatType::kParquet,
      }));
      break;
    case MakeErrorDeleteKind::kEqualityDeleteWithEmptyIds:
      delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kEqualityDeletes,
          .file_path = "eq-no-ids.parquet",
          .file_format = FileFormatType::kParquet,
          .equality_ids = {},
      }));
      break;
    case MakeErrorDeleteKind::kUnknownEqualityFieldId:
      delete_files.push_back(std::make_shared<DataFile>(DataFile{
          .content = DataFile::Content::kEqualityDeletes,
          .file_path = "eq-unknown.parquet",
          .file_format = FileFormatType::kParquet,
          .equality_ids = {999},
      }));
      break;
  }

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_);

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
}

INSTANTIATE_TEST_SUITE_P(
    MakeErrors, DeleteFilterMakeErrorTest,
    testing::Values(
        MakeErrorCase{
            .name = "NullDeleteFile",
            .delete_kind = MakeErrorDeleteKind::kNullDeleteFile,
        },
        MakeErrorCase{
            .name = "DataFileAsDeleteFile",
            .delete_kind = MakeErrorDeleteKind::kDataFile,
        },
        MakeErrorCase{
            .name = "EqualityDeleteWithEmptyIds",
            .delete_kind = MakeErrorDeleteKind::kEqualityDeleteWithEmptyIds,
        },
        MakeErrorCase{
            .name = "UnknownEqualityFieldId",
            .delete_kind = MakeErrorDeleteKind::kUnknownEqualityFieldId,
        }),
    ParamName<MakeErrorCase>);

TEST_F(DeleteFilterTest, EqualityFieldNestedInListOrMapErrors) {
  auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "id", int32()),
      SchemaField::MakeOptional(4, "tags",
                                list(SchemaField::MakeRequired(5, "element", string()))),
      SchemaField::MakeOptional(6, "attrs",
                                map(SchemaField::MakeRequired(7, "key", string()),
                                    SchemaField::MakeOptional(8, "value", string())))});
  auto requested_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())});

  for (const auto& [field_id, nested_type] :
       {std::pair{5, std::string_view("list")}, std::pair{8, std::string_view("map")}}) {
    auto eq_delete = std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kEqualityDeletes,
        .file_path = "eq-nested-container.parquet",
        .file_format = FileFormatType::kParquet,
        .equality_ids = {field_id},
    });
    std::vector<std::shared_ptr<DataFile>> delete_files = {eq_delete};

    auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, schema,
                                     requested_schema, file_io_);

    EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
    EXPECT_THAT(filter,
                HasErrorMessage(std::format("must not be nested in {}", nested_type)));
  }
}

TEST_F(DeleteFilterTest, NullPosInBatchErrors) {
  ICEBERG_UNWRAP_OR_FAIL(auto pos_delete,
                         PositionDeleteFile("pos-null-pos.parquet", {0}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {pos_delete};
  auto requested_schema = Project({1});
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch, MakeBatch(*filter.value()->RequiredSchema(),
                                               R"([[10, null], [20, null]])"));

  auto alive = filter.value()->ComputeAliveRows(batch.schema, batch.array);

  EXPECT_THAT(alive, IsError(ErrorKind::kInvalidArrowData));
}

TEST_F(DeleteFilterTest, ExpectedSchemaIsRequestedSchema) {
  auto requested_schema = Project({1});
  auto eq_by_name = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-name.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {2},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());
  EXPECT_EQ(filter.value()->ExpectedSchema(), requested_schema);
  EXPECT_NE(filter.value()->RequiredSchema(), requested_schema);
}

TEST_F(DeleteFilterTest, IncrementDeleteCountForwardsToCounter) {
  std::vector<std::shared_ptr<DataFile>> delete_files;
  auto counter = std::make_shared<DeleteCounter>();
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_,
                                   /*need_row_pos_col=*/true, counter);
  ASSERT_THAT(filter, IsOk());

  filter.value()->IncrementDeleteCount(3);
  filter.value()->IncrementDeleteCount();

  EXPECT_EQ(counter->Get(), 4);
}

TEST_F(DeleteFilterTest, DeletedRowPositionsNullWithNoPosDeletes) {
  std::vector<std::shared_ptr<DataFile>> delete_files;
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto index = filter.value()->DeletedRowPositions();

  ASSERT_THAT(index, IsOk());
  EXPECT_EQ(index.value(), nullptr);
}

TEST_F(DeleteFilterTest, DeletedRowPositionsLazyLoads) {
  ICEBERG_UNWRAP_OR_FAIL(auto pos_delete,
                         PositionDeleteFile("pos-index.parquet", {1, 3}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {pos_delete};
  auto requested_schema = Project({1});
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto index = filter.value()->DeletedRowPositions();

  ASSERT_THAT(index, IsOk());
  ASSERT_NE(index.value(), nullptr);
  EXPECT_TRUE(index.value()->IsDeleted(1));
  EXPECT_TRUE(index.value()->IsDeleted(3));
  EXPECT_FALSE(index.value()->IsDeleted(0));
  EXPECT_FALSE(index.value()->IsDeleted(2));
}

TEST_F(DeleteFilterTest, EqDeletedRowFilterTrueWithNoEqDeletes) {
  std::vector<std::shared_ptr<DataFile>> delete_files;
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto predicate_result = filter.value()->EqDeletedRowFilter();

  ASSERT_THAT(predicate_result, IsOk());
  ASSERT_TRUE(static_cast<bool>(predicate_result.value()));

  ICEBERG_UNWRAP_OR_FAIL(auto batch, MakeBatch(*filter.value()->RequiredSchema(),
                                               R"([[1, "Alice", "blue"]])"));
  ICEBERG_UNWRAP_OR_FAIL(auto row, ArrowArrayStructLike::Make(batch.schema, batch.array));
  ICEBERG_UNWRAP_OR_FAIL(auto alive, predicate_result.value()(*row));
  EXPECT_TRUE(alive);
}

TEST_F(DeleteFilterTest, EqDeletedRowFilterReturnsTrueForAliveRows) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_by_name,
      EqualityDeleteFile("eq-filter.parquet", R"([[0, "Bob", "unused"]])", {2}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto requested_schema = Project({1});
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto predicate_result = filter.value()->EqDeletedRowFilter();
  ASSERT_THAT(predicate_result, IsOk());
  auto& predicate = predicate_result.value();
  ASSERT_TRUE(static_cast<bool>(predicate));

  ICEBERG_UNWRAP_OR_FAIL(auto batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([[1, "Alice"], [2, "Bob"], [3, "Carol"]])"));
  ICEBERG_UNWRAP_OR_FAIL(auto row, ArrowArrayStructLike::Make(batch.schema, batch.array));

  ICEBERG_UNWRAP_OR_FAIL(auto alice_alive, predicate(*row));
  EXPECT_TRUE(alice_alive);

  ASSERT_THAT(row->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto bob_alive, predicate(*row));
  EXPECT_FALSE(bob_alive);

  ASSERT_THAT(row->Reset(2), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto carol_alive, predicate(*row));
  EXPECT_TRUE(carol_alive);
}

TEST_F(DeleteFilterTest, EqDeletedRowFilterIsCached) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_by_name,
      EqualityDeleteFile("eq-cache.parquet", R"([[0, "Bob", "unused"]])", {2}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto result1 = filter.value()->EqDeletedRowFilter();
  auto result2 = filter.value()->EqDeletedRowFilter();
  ASSERT_THAT(result1, IsOk());
  ASSERT_THAT(result2, IsOk());
  EXPECT_TRUE(static_cast<bool>(result1.value()));
  EXPECT_TRUE(static_cast<bool>(result2.value()));
}

TEST_F(DeleteFilterTest, FindEqDeleteRowsTrueForDeleted) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_by_name,
      EqualityDeleteFile("eq-find.parquet", R"([[0, "Bob", "unused"]])", {2}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto requested_schema = Project({1});
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   requested_schema, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto predicate_result = filter.value()->FindEqualityDeleteRows();
  ASSERT_THAT(predicate_result, IsOk());
  auto& predicate = predicate_result.value();
  ASSERT_TRUE(static_cast<bool>(predicate));

  ICEBERG_UNWRAP_OR_FAIL(auto batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([[1, "Alice"], [2, "Bob"], [3, "Carol"]])"));
  ICEBERG_UNWRAP_OR_FAIL(auto row, ArrowArrayStructLike::Make(batch.schema, batch.array));

  ICEBERG_UNWRAP_OR_FAIL(auto alice_deleted, predicate(*row));
  EXPECT_FALSE(alice_deleted);

  ASSERT_THAT(row->Reset(1), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto bob_deleted, predicate(*row));
  EXPECT_TRUE(bob_deleted);
}

TEST_F(DeleteFilterTest, FindEqDeleteRowsFalseWithNoEqDeletes) {
  std::vector<std::shared_ptr<DataFile>> delete_files;
  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, table_schema_,
                                   table_schema_, file_io_);
  ASSERT_THAT(filter, IsOk());

  auto predicate_result = filter.value()->FindEqualityDeleteRows();

  ASSERT_THAT(predicate_result, IsOk());
  ASSERT_TRUE(static_cast<bool>(predicate_result.value()));

  ICEBERG_UNWRAP_OR_FAIL(auto batch, MakeBatch(*filter.value()->RequiredSchema(),
                                               R"([[1, "Alice", "blue"]])"));
  ICEBERG_UNWRAP_OR_FAIL(auto row, ArrowArrayStructLike::Make(batch.schema, batch.array));
  ICEBERG_UNWRAP_OR_FAIL(auto deleted, predicate_result.value()(*row));
  EXPECT_FALSE(deleted);
}

TEST_F(DeleteFilterTest, ExplicitFieldLookupFiltersRows) {
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_by_name,
      EqualityDeleteFile("eq-lookup.parquet", R"([[0, "Bob", "unused"]])", {2}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto requested_schema = Project({1});

  ICEBERG_UNWRAP_OR_FAIL(auto base_lookup, DeleteFilter::MakeFieldLookup(table_schema_));
  DeleteFilter::FieldLookup custom_lookup =
      [base_lookup = std::move(base_lookup)](
          int32_t field_id) -> Result<std::optional<DeleteFilter::FieldLookupResult>> {
    if (field_id == 2) {
      return base_lookup(field_id);
    }
    return std::nullopt;
  };

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, requested_schema,
                                   file_io_, std::move(custom_lookup));

  ASSERT_THAT(filter, IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch,
                         MakeBatch(*filter.value()->RequiredSchema(),
                                   R"([[1, "Alice"], [2, "Bob"], [3, "Carol"]])"));

  auto alive = filter.value()->ComputeAliveRows(batch.schema, batch.array);

  ASSERT_THAT(alive, IsOk());
  ExpectAliveRows(alive.value(), {0, 2});
}

TEST_F(DeleteFilterTest, ExplicitFieldLookupNulloptErrors) {
  // A lookup that returns nullopt for the equality field must produce an error
  // at Make() time (during ComputeRequiredSchema), not silently skip the field.
  auto eq_by_name = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-missing.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {2},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto requested_schema = Project({1});

  DeleteFilter::FieldLookup empty_lookup =
      [](int32_t) -> Result<std::optional<DeleteFilter::FieldLookupResult>> {
    return std::nullopt;
  };

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, requested_schema,
                                   file_io_, std::move(empty_lookup));

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
}

TEST_F(DeleteFilterTest, ExplicitFieldLookupRejectsListOrMapProjection) {
  auto eq_by_element = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-element.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {5},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_element};
  auto requested_schema = Project({1});

  DeleteFilter::FieldLookup list_lookup =
      [](int32_t field_id) -> Result<std::optional<DeleteFilter::FieldLookupResult>> {
    auto element = SchemaField::MakeRequired(5, "element", string());
    return DeleteFilter::FieldLookupResult{
        .field = element,
        .projection_field = SchemaField::MakeOptional(4, "tags", list(element)),
    };
  };

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, requested_schema,
                                   file_io_, std::move(list_lookup));

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(filter, HasErrorMessage("must not be nested in list"));
}

TEST_F(DeleteFilterTest, ExplicitFieldLookupSkipsExistingFields) {
  // When the equality field is already in requested_schema, the custom lookup
  // must NOT be called
  ICEBERG_UNWRAP_OR_FAIL(
      auto eq_by_name,
      EqualityDeleteFile("eq-already-present.parquet", R"([[0, "Bob", "unused"]])", {2}));
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_name};
  auto requested_schema = Project({1, 2});

  bool lookup_called = false;
  DeleteFilter::FieldLookup tracking_lookup =
      [&lookup_called](
          int32_t) -> Result<std::optional<DeleteFilter::FieldLookupResult>> {
    lookup_called = true;
    return std::nullopt;  // would fail if called
  };

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, requested_schema,
                                   file_io_, std::move(tracking_lookup));

  ASSERT_THAT(filter, IsOk());
  EXPECT_FALSE(lookup_called);
}

TEST_F(DeleteFilterTest, SchemasLookupDeduplicatesCurrentSchemaId) {
  auto current_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32())},
      /*schema_id=*/2);
  auto same_id_historic_schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                               SchemaField::MakeOptional(7, "not_historic", string())},
      /*schema_id=*/2);
  auto eq_by_dropped = std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "eq-dropped.parquet",
      .file_format = FileFormatType::kParquet,
      .equality_ids = {7},
  });
  std::vector<std::shared_ptr<DataFile>> delete_files = {eq_by_dropped};
  std::vector<std::shared_ptr<Schema>> schemas = {current_schema,
                                                  same_id_historic_schema};

  auto filter = DeleteFilter::Make(std::string(kDataPath), delete_files, current_schema,
                                   current_schema, file_io_, schemas);

  EXPECT_THAT(filter, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(filter, HasErrorMessage("Cannot find equality delete field id 7"));
}

}  // namespace iceberg
