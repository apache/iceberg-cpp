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

#include <algorithm>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/column_page.h>
#include <parquet/column_reader.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_dictionary_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_metrics_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

namespace iceberg::parquet {
namespace {

class ParquetRowGroupFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    parquet::RegisterAll();
    io_ = std::make_shared<MockFileIO>();
    SetKeyType(int32());
    projection_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(2, "value", int64()),
                                 MetadataColumns::kRowPosition, MetadataColumns::kRowId});
  }

  void SetKeyType(std::shared_ptr<Type> type) {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", std::move(type)),
                                 SchemaField::MakeRequired(2, "value", int64())});
  }

  Status Write(bool statistics = true,
               const std::string& json = "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]") {
    ArrowSchema c_schema;
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*schema_, &c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto arrow_schema, ::arrow::ImportSchema(&c_schema));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto array,
                                   ::arrow::json::ArrayFromJSONString(
                                       ::arrow::struct_(arrow_schema->fields()), json));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch,
                                   ::arrow::RecordBatch::FromStructArray(array));
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto table,
                                   ::arrow::Table::FromRecordBatches({batch}));
    return Write(table, statistics);
  }

  Status Write(const std::shared_ptr<::arrow::Table>& table, bool statistics = true) {
    ICEBERG_ASSIGN_OR_RAISE(auto out, arrow::OpenArrowOutputStream(io_, path_));
    ::parquet::WriterProperties::Builder properties;
    properties.disable_dictionary();
    if (writer_properties_) {
      properties = ::parquet::WriterProperties::Builder(*writer_properties_);
    }
    if (!statistics) {
      properties.disable_statistics();
    }
    ICEBERG_ARROW_RETURN_NOT_OK(
        ::parquet::arrow::WriteTable(*table, ::arrow::default_memory_pool(), out,
                                     row_group_size_, properties.build()));
    ICEBERG_ARROW_RETURN_NOT_OK(out->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto input, arrow::OpenArrowInputStream(io_, path_));
    metadata_ = ::parquet::ParquetFileReader::Open(input)->metadata();
    return {};
  }

  ReaderOptions Options(std::shared_ptr<Expression> filter, bool case_sensitive = true) {
    // Bind against the table schema: key is intentionally outside the projection.
    // Tests of Open's binding behavior set options.filter directly instead.
    if (filter && filter->op() != Expression::Operation::kTrue &&
        filter->op() != Expression::Operation::kFalse) {
      auto is_bound = IsBoundVisitor::IsBound(filter);
      EXPECT_THAT(is_bound, IsOk());
      if (is_bound && !*is_bound) {
        auto bound = Binder::Bind(*schema_, filter, case_sensitive);
        EXPECT_THAT(bound, IsOk());
        if (bound) {
          filter = *bound;
        }
      }
    }
    ReaderOptions options{.path = path_,
                          .io = io_,
                          .projection = projection_,
                          .filter = std::move(filter),
                          .first_row_id = 100};
    options.case_sensitive = case_sensitive;
    options.properties.Set(ReaderProperties::kBatchSize, int64_t{3});
    return options;
  }

  Result<std::vector<int64_t>> Read(ReaderOptions options) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, options));
    ICEBERG_ASSIGN_OR_RAISE(auto c_schema, reader->Schema());
    ICEBERG_ARROW_ASSIGN_OR_RETURN(auto schema, ::arrow::ImportSchema(&c_schema));
    std::vector<int64_t> positions;
    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto array, reader->Next());
      if (!array) {
        break;
      }
      ICEBERG_ARROW_ASSIGN_OR_RETURN(auto batch,
                                     ::arrow::ImportRecordBatch(&*array, schema));
      auto values = std::static_pointer_cast<::arrow::Int64Array>(batch->column(0));
      auto pos = std::static_pointer_cast<::arrow::Int64Array>(batch->column(1));
      auto ids = std::static_pointer_cast<::arrow::Int64Array>(batch->column(2));
      for (int64_t i = 0; i < batch->num_rows(); ++i) {
        EXPECT_EQ(values->Value(i), pos->Value(i));
        EXPECT_EQ(ids->Value(i), 100 + pos->Value(i));
        positions.push_back(pos->Value(i));
      }
    }
    ICEBERG_RETURN_UNEXPECTED(reader->Close());
    return positions;
  }

  void Check(const ReaderOptions& options, const std::vector<int64_t>& expected) {
    SCOPED_TRACE(options.filter ? options.filter->ToString() : "no filter");
    ICEBERG_UNWRAP_OR_FAIL(auto actual, Read(options));
    EXPECT_EQ(actual, expected);
  }

  int64_t row_group_size_ = 2;
  std::shared_ptr<::parquet::WriterProperties> writer_properties_;
  std::string path_ = "rg.parquet";
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Schema> projection_;
  std::shared_ptr<::parquet::FileMetaData> metadata_;
};

TEST_F(ParquetRowGroupFilterTest, NonContiguousGroupsKeepPhysicalPositions) {
  ASSERT_THAT(Write(), IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 3);
  auto filter = Expressions::Or(Expressions::LessThan("key", Literal::Int(2)),
                                Expressions::GreaterThanOrEqual("key", Literal::Int(20)));
  Check(Options(filter), {0, 1, 4, 5});
  auto options = Options(filter);
  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, BatchSizesAcrossMultiplePhysicalGaps) {
  ASSERT_THAT(Write(true,
                    "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5],"
                    "[30,6],[31,7],[40,8],[41,9]]"),
              IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 5);
  auto filter = Expressions::Or(Expressions::LessThan("key", Literal::Int(2)),
                                Expressions::GreaterThanOrEqual("key", Literal::Int(20)));
  // Retain RG0 and RG2..4: a physical gap followed by multiple adjacent groups.
  for (int64_t batch_size : {1, 2, 3, 8, 32}) {
    SCOPED_TRACE(batch_size);
    auto options = Options(filter);
    options.properties.Set(ReaderProperties::kBatchSize, batch_size);
    Check(options, {0, 1, 4, 5, 6, 7, 8, 9});

    // With RG3 also removed, verify positions across two physical gaps.
    options.filter = Expressions::And(
        filter,
        Expressions::Or(Expressions::LessThan("key", Literal::Int(30)),
                        Expressions::GreaterThanOrEqual("key", Literal::Int(40))));
    ICEBERG_UNWRAP_OR_FAIL(options.filter, Binder::Bind(*schema_, options.filter, true));
    Check(options, {0, 1, 4, 5, 8, 9});
  }
}

TEST_F(ParquetRowGroupFilterTest, AllNoneAndResidualRows) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(nullptr), {0, 1, 2, 3, 4, 5});
  Check(Options(True::Instance()), {0, 1, 2, 3, 4, 5});
  Check(Options(False::Instance()), {});
  Check(Options(Expressions::Equal("key", Literal::Int(100))), {});
  Check(Options(Expressions::Equal("key", Literal::Int(-1))), {});
  // RG-only: key=1 is retained with key=0. This is not an exact row filter.
  Check(Options(Expressions::Equal("key", Literal::Int(0))), {0, 1});
  Check(Options(Expressions::And(Expressions::GreaterThan("key", Literal::Int(9)),
                                 Expressions::LessThanOrEqual("key", Literal::Int(11)))),
        {2, 3});
}

TEST_F(ParquetRowGroupFilterTest, RenameBoundPredicateAndCaseSensitivity) {
  ASSERT_THAT(Write(), IsOk());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "renamed", int32()), schema_->fields()[1]});
  auto filter = Expressions::Equal("renamed", Literal::Int(20));
  Check(Options(filter), {4, 5});
  ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*schema_, filter, true));
  auto options = Options(bound);
  Check(options, {4, 5});
  options = Options(Expressions::Equal("RENAMED", Literal::Int(20)), false);
  Check(options, {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, OpenBindsUnboundProjectedReferences) {
  ASSERT_THAT(Write(), IsOk());
  auto options = Options(nullptr);
  options.filter = Expressions::GreaterThanOrEqual("VALUE", Literal::Long(4));
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("Cannot find field 'VALUE'"));
  options.case_sensitive = false;
  Check(options, {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, MissingStatisticsRetainGroups) {
  ASSERT_THAT(Write(false), IsOk());
  Check(Options(Expressions::Equal("key", Literal::Int(100))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::IsNull("key")), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::NotNull("key")), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, MissingNullCountRetainsNullBranches) {
  ASSERT_THAT(Write(true, "[[null,0],[150,1]]"), IsOk());
  auto original = metadata_->RowGroup(0);
  auto builder = ::parquet::FileMetaDataBuilder::Make(
      metadata_->schema(), ::parquet::default_writer_properties());
  auto row_group = builder->AppendRowGroup();
  row_group->set_num_rows(original->num_rows());
  for (int i = 0; i < original->num_columns(); ++i) {
    auto column = original->ColumnChunk(i);
    auto stats = column->statistics()->Encode();
    if (i == 0) {
      stats.has_null_count = false;
    }
    auto chunk = row_group->NextColumnChunk();
    chunk->SetStatistics(stats);
    chunk->Finish(column->num_values(), column->dictionary_page_offset(),
                  /*index_page_offset=*/0, column->data_page_offset(),
                  column->total_compressed_size(), column->total_uncompressed_size(),
                  column->has_dictionary_page(), /*dictionary_fallback=*/false, {}, {});
  }
  row_group->Finish(original->total_byte_size());
  auto rebuilt = builder->Finish();
  auto group = rebuilt->RowGroup(0);
  auto stats = group->ColumnChunk(0)->statistics();
  ASSERT_NE(stats, nullptr);
  ASSERT_TRUE(stats->HasMinMax());
  ASSERT_FALSE(stats->HasNullCount());

  ::parquet::arrow::SchemaManifest manifest;
  ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(
                  rebuilt->schema(), /*key_value_metadata=*/nullptr,
                  ::parquet::default_arrow_reader_properties(), &manifest)
                  .ok());
  struct Case {
    std::shared_ptr<Expression> filter;
    bool should_read;
  };
  for (const auto& test : std::vector<Case>{
           {.filter = Expressions::IsNull("key"), .should_read = true},
           {.filter = Expressions::NotNull("key"), .should_read = true},
           {.filter =
                Expressions::Not(Expressions::GreaterThan("key", Literal::Int(100))),
            .should_read = true},
           {.filter = Expressions::LessThan("key", Literal::Int(100)),
            .should_read = false}}) {
    SCOPED_TRACE(test.filter->ToString());
    ICEBERG_UNWRAP_OR_FAIL(auto filter,
                           ParquetMetricsRowGroupFilter::Make(Options(test.filter).filter,
                                                              *rebuilt->schema()));
    ICEBERG_UNWRAP_OR_FAIL(auto should_read, filter->ShouldRead(manifest, *group));
    EXPECT_EQ(should_read, test.should_read);
  }
}

TEST_F(ParquetRowGroupFilterTest, OpenRejectsUnresolvableReferences) {
  ASSERT_THAT(Write(), IsOk());
  auto options = Options(nullptr);
  options.filter = Expressions::Equal("missing", Literal::Int(0));
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("Cannot find field 'missing'"));
  options.filter = Expressions::Equal("key", Literal::Int(0));
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("Cannot find field 'key'"));
}

TEST_F(ParquetRowGroupFilterTest, PromotedIntegerStatistics) {
  ASSERT_THAT(Write(), IsOk());
  SetKeyType(int64());
  Check(Options(Expressions::Equal("key", Literal::Long(100))), {});
  Check(Options(Expressions::Equal("key", Literal::Long(10))), {2, 3});
}

TEST_F(ParquetRowGroupFilterTest, MissingColumnWithDefaultRetainsGroups) {
  ASSERT_THAT(Write(), IsOk());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      schema_->fields()[0], schema_->fields()[1],
      SchemaField::MakeOptional(3, "defaulted", int32())
          .WithInitialDefault(std::make_shared<Literal>(Literal::Int(7)))});
  Check(Options(Expressions::Equal("defaulted", Literal::Int(7))), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, FileAndFilterTypeCompatibility) {
  struct Case {
    std::shared_ptr<Type> file_type;
    std::shared_ptr<Type> filter_type;
    std::string json;
    Literal value;
    bool compatible;
  };
  const std::string numbers = "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]";
  const std::string decimals =
      R"([["0.00",0],["1.00",1],["10.00",2],["11.00",3],["20.00",4],["21.00",5]])";
  const std::string timestamps =
      R"([["1970-01-01 00:00:00",0],["1970-01-01 00:00:01",1],["1970-01-01 00:00:10",2],["1970-01-01 00:00:11",3],["1970-01-01 00:00:20",4],["1970-01-01 00:00:21",5]])";
  for (const auto& test :
       std::vector<Case>{{.file_type = float32(),
                          .filter_type = float64(),
                          .json = numbers,
                          .value = Literal::Double(10),
                          .compatible = true},
                         {.file_type = float64(),
                          .filter_type = float32(),
                          .json = numbers,
                          .value = Literal::Float(10),
                          .compatible = false},
                         {.file_type = int64(),
                          .filter_type = int32(),
                          .json = numbers,
                          .value = Literal::Int(10),
                          .compatible = false},
                         {.file_type = int32(),
                          .filter_type = date(),
                          .json = numbers,
                          .value = Literal::Date(10),
                          .compatible = false},
                         {.file_type = date(),
                          .filter_type = int32(),
                          .json = numbers,
                          .value = Literal::Int(10),
                          .compatible = false},
                         {.file_type = decimal(9, 2),
                          .filter_type = decimal(18, 2),
                          .json = decimals,
                          .value = Literal::Decimal(1000, 18, 2),
                          .compatible = true},
                         {.file_type = decimal(9, 2),
                          .filter_type = decimal(8, 2),
                          .json = decimals,
                          .value = Literal::Decimal(1000, 8, 2),
                          .compatible = false},
                         {.file_type = decimal(9, 2),
                          .filter_type = decimal(9, 3),
                          .json = decimals,
                          .value = Literal::Decimal(1000, 9, 3),
                          .compatible = false},
                         {.file_type = timestamp(),
                          .filter_type = timestamp_ns(),
                          .json = timestamps,
                          .value = Literal::TimestampNs(10000000),
                          .compatible = false},
                         {.file_type = timestamp(),
                          .filter_type = timestamp_tz(),
                          .json = timestamps,
                          .value = Literal::TimestampTz(10000000),
                          .compatible = false},
                         {.file_type = timestamp_ns(),
                          .filter_type = timestamp_ns(),
                          .json = timestamps,
                          .value = Literal::TimestampNs(10000000000),
                          .compatible = true},
                         {.file_type = timestamp_tz(),
                          .filter_type = timestamp_tz(),
                          .json = timestamps,
                          .value = Literal::TimestampTz(10000000),
                          .compatible = true},
                         {.file_type = fixed(1),
                          .filter_type = fixed(1),
                          .json = R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
                          .value = Literal::Fixed({'m'}),
                          .compatible = true},
                         {.file_type = fixed(1),
                          .filter_type = fixed(2),
                          .json = R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
                          .value = Literal::Fixed({'m', 'm'}),
                          .compatible = false}}) {
    SCOPED_TRACE(test.file_type->ToString() + " -> " + test.filter_type->ToString());
    SetKeyType(test.file_type);
    ASSERT_THAT(Write(true, test.json), IsOk());
    SetKeyType(test.filter_type);
    const bool floating = test.filter_type->type_id() == TypeId::kFloat ||
                          test.filter_type->type_id() == TypeId::kDouble;
    Check(Options(Expressions::Equal("key", test.value)),
          !floating && test.compatible ? std::vector<int64_t>{2, 3}
                                       : std::vector<int64_t>{0, 1, 2, 3, 4, 5});
    if (floating) {
      // Null counts still distinguish compatible types when bounds are disabled.
      SetKeyType(test.file_type);
      ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3]]"), IsOk());
      SetKeyType(test.filter_type);
      Check(Options(Expressions::IsNull("key")), test.compatible
                                                     ? std::vector<int64_t>{0, 1}
                                                     : std::vector<int64_t>{0, 1, 2, 3});
      Check(Options(Expressions::NotNull("key")), test.compatible
                                                      ? std::vector<int64_t>{2, 3}
                                                      : std::vector<int64_t>{0, 1, 2, 3});
    }
  }
}

TEST_F(ParquetRowGroupFilterTest, NullNegativeAndUnsupportedBooleanBranches) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  Check(Options(Expressions::IsNull("key")), {0, 1});
  Check(Options(Expressions::NotNull("key")), {2, 3, 4, 5});
  Check(Options(Expressions::NotEqual("key", Literal::Int(10))), {0, 1, 2, 3, 4, 5});
  auto unsupported =
      Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16), Literal::Int(0));
  auto supported = Expressions::Equal("key", Literal::Int(20));
  Check(Options(Expressions::And(supported, unsupported)), {4, 5});
  Check(Options(Expressions::Or(supported, unsupported)), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(unsupported)), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(supported)), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, RewriteNotForBoundPredicates) {
  ASSERT_THAT(Write(), IsOk());
  auto less_than = Expressions::LessThan("key", Literal::Int(10));
  auto greater_than = Expressions::GreaterThan("key", Literal::Int(11));
  Check(Options(Expressions::Not(less_than)), {2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::Or(less_than, greater_than))), {2, 3});
  Check(Options(Expressions::Not(
            Expressions::And(Expressions::GreaterThanOrEqual("key", Literal::Int(10)),
                             Expressions::LessThanOrEqual("key", Literal::Int(11))))),
        {0, 1, 4, 5});
  Check(Options(Expressions::Not(Expressions::Not(less_than))), {0, 1});
  Check(Options(Expressions::Not(Expressions::NotEqual("key", Literal::Int(10)))),
        {2, 3});
  Check(Options(Expressions::Not(Expressions::NotIn("key", {Literal::Int(10)}))), {2, 3});

  ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*schema_, less_than, true));
  Check(Options(Expressions::Not(bound)), {2, 3, 4, 5});

  // Unsupported transforms must remain conservative under rewritten NOTs.
  auto unsupported =
      Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16), Literal::Int(0));
  Check(Options(Expressions::Not(Expressions::Or(less_than, unsupported))), {2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::And(less_than, unsupported))),
        {0, 1, 2, 3, 4, 5});

  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  Check(Options(Expressions::Not(Expressions::IsNull("key"))), {2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::NotNull("key"))), {0, 1});
}

TEST_F(ParquetRowGroupFilterTest, RewrittenNotPrunesNullableGroups) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[null,2],[50,3],[100,4],[150,5]]"), IsOk());
  // Negated ranges match nulls; non-null groups still use range pruning.
  Check(Options(Expressions::Not(Expressions::LessThan("key", Literal::Int(100)))),
        {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::LessThanOrEqual("key", Literal::Int(100)))),
        {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::GreaterThan("key", Literal::Int(100)))),
        {0, 1, 2, 3, 4, 5});
  Check(Options(
            Expressions::Not(Expressions::GreaterThanOrEqual("key", Literal::Int(100)))),
        {0, 1, 2, 3});
}

TEST_F(ParquetRowGroupFilterTest, BoundComparisonBoundariesAndAllNullGroups) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  struct Case {
    std::shared_ptr<Expression> filter;
    std::vector<int64_t> expected;
  };
  for (const auto& test : std::vector<Case>{
           {.filter = Expressions::LessThan("key", Literal::Int(10)), .expected = {}},
           {.filter = Expressions::LessThanOrEqual("key", Literal::Int(10)),
            .expected = {2, 3}},
           {.filter = Expressions::GreaterThan("key", Literal::Int(21)), .expected = {}},
           {.filter = Expressions::GreaterThanOrEqual("key", Literal::Int(21)),
            .expected = {4, 5}},
           {.filter = Expressions::Equal("key", Literal::Int(11)), .expected = {2, 3}},
           {.filter = Expressions::In("key", {Literal::Int(11), Literal::Int(20)}),
            .expected = {2, 3, 4, 5}},
       }) {
    Check(Options(test.filter), test.expected);
  }
}

TEST_F(ParquetRowGroupFilterTest, InPredicateLimit) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::In("key", {Literal::Int(0), Literal::Int(20)})),
        {0, 1, 4, 5});
  Check(Options(Expressions::In("key", {Literal::Int(5), Literal::Int(25)})), {});
  std::vector<Literal> values;
  for (int i = 100; i < 200; ++i) {
    values.push_back(Literal::Int(i));
  }
  Check(Options(Expressions::In("key", values)), {});
  for (int i = 200; i < 300; ++i) {
    values.push_back(Literal::Int(i));
  }
  Check(Options(Expressions::In("key", values)), {});
  // The limit applies to the bound set, not the unbound list with duplicates.
  auto duplicate_values = values;
  duplicate_values.push_back(values.back());
  auto options = Options(nullptr);
  auto fields = projection_->fields();
  std::vector<SchemaField> projected_fields(fields.begin(), fields.end());
  projected_fields.push_back(schema_->fields()[0]);
  options.projection = std::make_shared<Schema>(std::move(projected_fields));
  options.filter = Expressions::In("key", duplicate_values);
  Check(options, {});

  values.push_back(Literal::Int(300));
  Check(Options(Expressions::In("key", values)), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::And(Expressions::In("key", values),
                                 Expressions::Equal("key", Literal::Int(20)))),
        {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, LargeExpressionStillPrunes) {
  ASSERT_THAT(Write(), IsOk());
  auto filter = Expressions::Equal("key", Literal::Int(100));
  std::shared_ptr<Expression> large = filter;
  for (int i = 0; i < 260; ++i) {
    large = Expressions::Or(large, filter);
  }
  Check(Options(large), {});
}

TEST_F(ParquetRowGroupFilterTest, SplitIntersectionAndEmptySplit) {
  ASSERT_THAT(Write(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  auto file_size = input->GetSize().ValueOrDie();
  ASSERT_TRUE(input->Close().ok());
  ICEBERG_UNWRAP_OR_FAIL(auto midpoint,
                         RowGroupMidpoint(*metadata_->RowGroup(1), file_size));
  auto options = Options(Expressions::GreaterThanOrEqual("key", Literal::Int(20)));
  options.split = Split{.offset = static_cast<size_t>(midpoint), .length = 100000};
  Check(options, {4, 5});
  ICEBERG_UNWRAP_OR_FAIL(
      options.filter,
      Binder::Bind(*schema_, Expressions::LessThan("key", Literal::Int(2)), true));
  Check(options, {});

  // A one-byte split selects only the group with this midpoint. Statistics must
  // refine that selection without adding groups from outside the split.
  options.split->length = 1;
  options.filter = Options(Expressions::Equal("key", Literal::Int(10))).filter;
  Check(options, {2, 3});
  options.filter = Options(Expressions::Equal("key", Literal::Int(20))).filter;
  Check(options, {});
  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {2, 3});
  options.filter = nullptr;
  options.split->length = 0;
  Check(options, {});
}

TEST_F(ParquetRowGroupFilterTest, NameMappingWithoutFieldIdsRetainsGroups) {
  auto keys = ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0,1,10,11,20,21]")
                  .ValueOrDie();
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[0,1,2,3,4,5]").ValueOrDie();
  auto file_schema = ::arrow::schema({::arrow::field("stored_key", ::arrow::int32()),
                                      ::arrow::field("stored_value", ::arrow::int64())});
  ASSERT_THAT(Write(::arrow::Table::Make(file_schema, {keys, values})), IsOk());
  ASSERT_EQ(metadata_->schema()->Column(0)->schema_node()->field_id(), -1);
  auto mapping =
      NameMapping::Make({MappedField{.names = {"stored_key"}, .field_id = 1},
                         MappedField{.names = {"stored_value"}, .field_id = 2}});
  auto options = Options(Expressions::Equal("key", Literal::Int(100)));
  options.name_mapping = std::move(mapping);
  // Name mapping resolves the projection. Footer columns without IDs remain
  // unavailable to statistics filtering, so the reader must keep their groups.
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, PrimitiveComparisonTypes) {
  struct Case {
    std::shared_ptr<Type> type;
    std::string json;
    Literal literal;
  };
  for (const auto& test : std::vector<Case>{
           {.type = int64(),
            .json = "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]",
            .literal = Literal::Long(10)},
           {.type = date(),
            .json = "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]",
            .literal = Literal::Date(10)},
           {.type = string(),
            .json = R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            .literal = Literal::String("m")},
           {.type = boolean(),
            .json = "[[false,0],[false,1],[true,2],[true,3],[false,4],[false,5]]",
            .literal = Literal::Boolean(true)},
           {.type = binary(),
            .json = R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            .literal = Literal::Binary({'m'})}}) {
    SCOPED_TRACE(test.type->ToString());
    SetKeyType(test.type);
    ASSERT_THAT(Write(true, test.json), IsOk());
    Check(Options(Expressions::Equal("key", test.literal)), {2, 3});
  }
}

TEST_F(ParquetRowGroupFilterTest, UuidBoundsUseUnsignedByteOrder) {
  SetKeyType(uuid());
  // Group 0 holds UUIDs below 0x80..., group 1 holds UUIDs at or above it. A signed
  // byte comparison would order group 1 before group 0.
  std::vector<Uuid> uuids;
  for (const auto* str :
       {"00000000-0000-0000-0000-000000000001", "7fffffff-ffff-ffff-ffff-ffffffffffff",
        "80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-fffffffffffe"}) {
    ICEBERG_UNWRAP_OR_FAIL(auto uuid, Uuid::FromString(str));
    uuids.push_back(uuid);
  }
  ::arrow::FixedSizeBinaryBuilder keys(::arrow::fixed_size_binary(Uuid::kLength));
  for (const auto& uuid : uuids) {
    ASSERT_TRUE(keys.Append(uuid.bytes().data()).ok());
  }
  auto key_array = ::arrow::ExtensionType::WrapArray(::arrow::extension::uuid(),
                                                     keys.Finish().ValueOrDie());
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[0,1,2,3]").ValueOrDie();
  ArrowSchema c_schema;
  ASSERT_THAT(ToArrowSchema(*schema_, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  ASSERT_THAT(Write(::arrow::Table::Make(arrow_schema, {key_array, values})), IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 2);
  ASSERT_TRUE(metadata_->RowGroup(0)->ColumnChunk(0)->statistics()->HasMinMax());

  const auto& group0_max = uuids[1];
  const auto& group1_min = uuids[2];
  Check(Options(Expressions::LessThan("key", Literal::UUID(group1_min))), {0, 1});
  Check(Options(Expressions::LessThanOrEqual("key", Literal::UUID(group0_max))), {0, 1});
  Check(Options(Expressions::GreaterThan("key", Literal::UUID(group0_max))), {2, 3});
  Check(Options(Expressions::GreaterThanOrEqual("key", Literal::UUID(group1_min))),
        {2, 3});
  Check(Options(Expressions::Equal("key", Literal::UUID(group1_min))), {2, 3});
  Check(
      Options(Expressions::Not(Expressions::LessThan("key", Literal::UUID(group1_min)))),
      {2, 3});
}

TEST_F(ParquetRowGroupFilterTest, FloatingPointStatistics) {
  // TODO(YinZheng-Sun): remove this once row group pruning is applied to
  // float/double columns.
  SetKeyType(float64());
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(100))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Equal("key", Literal::Double(10))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::IsNaN("key")), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, NestedPrimitiveStatistics) {
  SetKeyType(std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "nested", int32())}));
  ASSERT_THAT(Write(true, "[[[0],0],[[1],1],[[10],2],[[11],3],[[20],4],[[21],5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key.nested", Literal::Int(100))), {});
  Check(Options(Expressions::Equal("key.nested", Literal::Int(10))), {2, 3});
}

TEST_F(ParquetRowGroupFilterTest, RequiredLeafWithNullableParent) {
  SetKeyType(std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(3, "nested", int32())}));
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[[10],2],[[11],3],[null,4],[null,5]]"),
              IsOk());
  auto is_null = Expressions::IsNull("key.nested");
  auto not_null = Expressions::NotNull("key.nested");
  Check(Options(is_null), {0, 1, 4, 5});
  Check(Options(not_null), {2, 3});
  Check(Options(Expressions::Not(is_null)), {2, 3});
  Check(Options(Expressions::Not(not_null)), {0, 1, 4, 5});
  Check(Options(Expressions::Not(Expressions::LessThan("key.nested", Literal::Int(100)))),
        {0, 1, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, UnsupportedTransformRetainsGroups) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16),
                                                   Literal::Int(15))),
        {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, NegativePredicates) {
  ASSERT_THAT(Write(true, "[[5,0],[5,1],[null,2],[5,3],[6,4],[6,5]]"), IsOk());
  Check(Options(Expressions::NotEqual("key", Literal::Int(5))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::NotIn("key", {Literal::Int(5), Literal::Int(6)})),
        {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::LessThan("key", Literal::Int(100)))),
        {2, 3});
}

TEST_F(ParquetRowGroupFilterTest, PrefixPredicates) {
  SetKeyType(string());
  ASSERT_THAT(
      Write(
          true,
          R"([["apple",0],["apricot",1],["banana",2],["blueberry",3],[null,4],["apricot",5]])"),
      IsOk());
  Check(Options(Expressions::StartsWith("key", "ap")), {0, 1, 4, 5});
  Check(Options(Expressions::NotStartsWith("key", "ap")), {2, 3, 4, 5});
  Check(Options(Expressions::StartsWith("key", "aa")), {});
  Check(Options(Expressions::StartsWith("key", "z")), {});
  Check(Options(Expressions::StartsWith("key", "apricots")), {});
  Check(Options(Expressions::StartsWith("key", "")), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::NotStartsWith("key", "")), {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, Utf8StringBoundsUseUnsignedByteOrder) {
  SetKeyType(string());
  // RG2 holds multi-byte UTF-8 values (U+00E9, U+00FC) whose leading byte 0xC3
  // sorts after ASCII only under unsigned byte comparison.
  ASSERT_THAT(
      Write(true, R"([["a",0],["b",1],["y",2],["z",3],["\u00e9",4],["\u00fc",5]])"),
      IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 3);
  const std::string e_acute = "\xC3\xA9";
  const std::string u_umlaut = "\xC3\xBC";
  Check(Options(Expressions::GreaterThan("key", Literal::String("z"))), {4, 5});
  Check(Options(Expressions::LessThan("key", Literal::String("c"))), {0, 1});
  Check(Options(Expressions::Equal("key", Literal::String(e_acute))), {4, 5});
  Check(Options(Expressions::StartsWith("key", e_acute)), {4, 5});
  Check(Options(Expressions::StartsWith("key", u_umlaut)), {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, DecimalAndTemporalStatistics) {
  struct Case {
    std::shared_ptr<Type> type;
    std::string json;
    Literal value;
  };
  for (
      const auto& test : std::vector<Case>{
          {.type = decimal(9, 2),
           .json =
               R"([["0.00",0],["1.00",1],["10.00",2],["11.00",3],["20.00",4],["21.00",5]])",
           .value = Literal::Decimal(1000, 9, 2)},
          {.type = time(),
           .json = "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]",
           .value = Literal::Time(10)},
          {.type = timestamp(),
           .json =
               R"([["1970-01-01 00:00:00",0],["1970-01-01 00:00:01",1],["1970-01-01 00:00:10",2],["1970-01-01 00:00:11",3],["1970-01-01 00:00:20",4],["1970-01-01 00:00:21",5]])",
           .value = Literal::Timestamp(10000000)}}) {
    SCOPED_TRACE(test.type->ToString());
    SetKeyType(test.type);
    ASSERT_THAT(Write(true, test.json), IsOk());
    Check(Options(Expressions::Equal("key", test.value)), {2, 3});
    Check(Options(Expressions::LessThan("key", test.value)), {0, 1});
  }
}

TEST_F(ParquetRowGroupFilterTest, FloatingPointNullAndSignedZero) {
  SetKeyType(float64());
  ASSERT_THAT(Write(true, "[[-0.0,0],[0.0,1],[null,2],[null,3],[10.0,4],[11.0,5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(-0.0))), {0, 1, 4, 5});
  Check(Options(Expressions::Equal("key", Literal::Double(0.0))), {0, 1, 4, 5});
  Check(Options(Expressions::IsNull("key")), {2, 3});
  Check(Options(Expressions::NotNull("key")), {0, 1, 4, 5});
  // Like Java, IS NAN excludes all-null groups; NOT NAN retains them.
  Check(Options(Expressions::IsNaN("key")), {0, 1, 4, 5});
  Check(Options(Expressions::NotNaN("key")), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::IsNaN("key"))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::NotNaN("key"))), {0, 1, 4, 5});
  Check(Options(Expressions::LessThan("key", Literal::Double(-100))), {0, 1, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, AllNaNGroupRetainedWithoutComparableBounds) {
  SetKeyType(float64());
  ASSERT_THAT(Write(true, "[[NaN,0],[-NaN,1],[10.0,2],[11.0,3],[null,4],[null,5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(100))), {0, 1, 2, 3});
  Check(Options(Expressions::IsNaN("key")), {0, 1, 2, 3});
  Check(Options(Expressions::NotNaN("key")), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::LessThan("key", Literal::Double(-100))), {0, 1, 2, 3});
}

TEST_F(ParquetRowGroupFilterTest, InvalidFilterFailsDuringOpen) {
  ASSERT_THAT(Write(), IsOk());
  auto options = Options(nullptr);
  options.filter = Expressions::Count("value");
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("does not support bound aggregate"));

  ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*schema_, options.filter, true));
  auto bound_options = Options(bound);
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, bound_options),
              HasErrorMessage("does not support bound aggregate"));

  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, FloatBoundsMustNotHideNaNs) {
  SetKeyType(float32());
  ASSERT_THAT(Write(true, "[[NaN,0],[2.0,1],[-NaN,2],[2.0,3]]"), IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 2);

  // Both groups contain NaN, but Parquet records min = max = 2.
  for (int group : {0, 1}) {
    auto stats = std::static_pointer_cast<::parquet::FloatStatistics>(
        metadata_->RowGroup(group)->ColumnChunk(0)->statistics());
    ASSERT_NE(stats, nullptr);
    ASSERT_TRUE(stats->HasMinMax());
    EXPECT_EQ(stats->min(), 2.0F);
    EXPECT_EQ(stats->max(), 2.0F);
  }

  Check(Options(Expressions::GreaterThan("key", Literal::Float(3))), {0, 1, 2, 3});
  Check(Options(Expressions::LessThan("key", Literal::Float(-3))), {0, 1, 2, 3});
}

TEST_F(ParquetRowGroupFilterTest, DoubleBoundsMustNotHideNaNs) {
  SetKeyType(float64());
  ASSERT_THAT(Write(true, "[[NaN,0],[2.0,1],[-NaN,2],[2.0,3]]"), IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 2);

  for (int group : {0, 1}) {
    auto stats = std::static_pointer_cast<::parquet::DoubleStatistics>(
        metadata_->RowGroup(group)->ColumnChunk(0)->statistics());
    ASSERT_NE(stats, nullptr);
    ASSERT_TRUE(stats->HasMinMax());
    EXPECT_EQ(stats->min(), 2.0);
    EXPECT_EQ(stats->max(), 2.0);
  }

  Check(Options(Expressions::GreaterThan("key", Literal::Double(3))), {0, 1, 2, 3});
  Check(Options(Expressions::LessThan("key", Literal::Double(-3))), {0, 1, 2, 3});
}

// Exercise dictionary filtering separately from footer statistics, and through
// the production reader (where the filter column is outside the projection).
class ParquetDictionaryFilterTest : public ParquetRowGroupFilterTest {
 protected:
  void SetUp() override {
    ParquetRowGroupFilterTest::SetUp();
    ::parquet::WriterProperties::Builder properties;
    writer_properties_ = properties.enable_dictionary()->build();
  }

  void CheckDictionaries(const std::shared_ptr<Expression>& expression,
                         const std::vector<bool>& expected) {
    ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
    auto reader = ::parquet::ParquetFileReader::Open(input);
    ::parquet::arrow::SchemaManifest manifest;
    ::parquet::ArrowReaderProperties properties;
    properties.set_arrow_extensions_enabled(true);
    ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(metadata_->schema(),
                                                       metadata_->key_value_metadata(),
                                                       properties, &manifest)
                    .ok());
    ICEBERG_UNWRAP_OR_FAIL(
        auto filter, ParquetDictionaryRowGroupFilter::Make(Options(expression).filter,
                                                           *metadata_->schema()));
    ASSERT_EQ(expected.size(), metadata_->num_row_groups());
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      SCOPED_TRACE(expression->ToString() + " group " + std::to_string(i));
      auto group = reader->RowGroup(i);
      ICEBERG_UNWRAP_OR_FAIL(auto actual, filter->ShouldRead(manifest, *group));
      EXPECT_EQ(actual, expected[i]);
    }
  }
};

TEST_F(ParquetDictionaryFilterTest, ComparisonsAndBooleanExpressions) {
  ASSERT_THAT(Write(true, "[[0,0],[10,1],[5,2],[15,3],[0,4],[10,5]]"), IsOk());
  CheckDictionaries(Expressions::Equal("key", Literal::Int(5)), {false, true, false});
  CheckDictionaries(Expressions::NotEqual("key", Literal::Int(5)), {true, true, true});
  CheckDictionaries(Expressions::LessThan("key", Literal::Int(5)), {true, false, true});
  CheckDictionaries(Expressions::LessThanOrEqual("key", Literal::Int(5)),
                    {true, true, true});
  CheckDictionaries(Expressions::GreaterThan("key", Literal::Int(10)),
                    {false, true, false});
  CheckDictionaries(Expressions::GreaterThanOrEqual("key", Literal::Int(10)),
                    {true, true, true});
  auto equal = Expressions::Equal("key", Literal::Int(5));
  CheckDictionaries(Expressions::And(equal, Expressions::AlwaysTrue()),
                    {false, true, false});
  CheckDictionaries(Expressions::Or(equal, Expressions::AlwaysFalse()),
                    {false, true, false});
  CheckDictionaries(Expressions::Or(equal, Expressions::AlwaysTrue()),
                    {true, true, true});
  // Negating a bound range adds an IS_NULL branch (a required leaf can have
  // an optional ancestor), which dictionary filtering leaves to statistics.
  CheckDictionaries(
      Expressions::Not(Expressions::GreaterThanOrEqual("key", Literal::Int(5))),
      {true, true, true});
  CheckDictionaries(Expressions::Not(Expressions::NotEqual("key", Literal::Int(5))),
                    {false, true, false});
  Check(
      Options(Expressions::Not(Expressions::GreaterThanOrEqual("key", Literal::Int(5)))),
      {0, 1, 4, 5});
  // All three footer ranges contain 5. Only the second dictionary contains it.
  Check(Options(equal), {2, 3});
  auto options = Options(equal);
  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetDictionaryFilterTest, SetsNegativePredicatesAndNulls) {
  ASSERT_THAT(Write(true, "[[5,0],[5,1],[null,2],[5,3],[0,4],[10,5]]"), IsOk());
  CheckDictionaries(Expressions::NotEqual("key", Literal::Int(5)), {false, true, true});
  CheckDictionaries(Expressions::In("key", {Literal::Int(5), Literal::Int(7)}),
                    {true, true, false});
  CheckDictionaries(Expressions::NotIn("key", {Literal::Int(5), Literal::Int(7)}),
                    {false, true, true});
  CheckDictionaries(Expressions::IsNull("key"), {true, true, true});
  CheckDictionaries(Expressions::NotNull("key"), {true, true, true});
  ASSERT_THAT(Write(false, "[[5,0],[5,1],[5,2],[5,3]]"), IsOk());
  CheckDictionaries(Expressions::NotEqual("key", Literal::Int(5)), {true, true});
  CheckDictionaries(Expressions::NotIn("key", {Literal::Int(5), Literal::Int(7)}),
                    {true, true});
}

TEST_F(ParquetDictionaryFilterTest, Prefixes) {
  SetKeyType(string());
  ASSERT_THAT(Write(true, R"([["aa",0],["az",1],["ba",2],["bz",3],[null,4],["aa",5]])"),
              IsOk());
  CheckDictionaries(Expressions::StartsWith("key", "a"), {true, false, true});
  CheckDictionaries(Expressions::NotStartsWith("key", "a"), {false, true, true});
  CheckDictionaries(Expressions::StartsWith("key", ""), {true, true, true});
  CheckDictionaries(Expressions::NotStartsWith("key", ""), {false, false, true});
}

TEST_F(ParquetDictionaryFilterTest, FloatingPointNaNAndSignedZero) {
  for (const auto& type : std::vector<std::shared_ptr<Type>>{float32(), float64()}) {
    SetKeyType(type);
    ASSERT_THAT(
        Write(true, "[[NaN,0],[NaN,1],[null,2],[NaN,3],[0,4],[-0.0,5],[1,6],[NaN,7]]"),
        IsOk());
    CheckDictionaries(Expressions::IsNaN("key"), {true, true, false, true});
    CheckDictionaries(Expressions::NotNaN("key"), {false, true, true, true});
    auto zero =
        type->type_id() == TypeId::kFloat ? Literal::Float(0) : Literal::Double(0);
    CheckDictionaries(Expressions::Equal("key", zero), {false, false, true, false});
    CheckDictionaries(Expressions::Not(Expressions::IsNaN("key")),
                      {false, true, true, true});
  }
}

TEST_F(ParquetDictionaryFilterTest, PhysicalTypesAndPromotions) {
  struct Case {
    std::shared_ptr<Type> file_type;
    std::shared_ptr<Type> read_type;
    std::string json;
    Literal value;
  };
  const std::string numbers = "[[0,0],[10,1],[5,2],[15,3],[0,4],[10,5]]";
  const std::string decimals =
      R"([["0.00",0],["10.00",1],["5.00",2],["15.00",3],["0.00",4],["10.00",5]])";
  for (const auto& test :
       std::vector<Case>{{.file_type = int32(),
                          .read_type = int64(),
                          .json = numbers,
                          .value = Literal::Long(5)},
                         {.file_type = int64(),
                          .read_type = int64(),
                          .json = numbers,
                          .value = Literal::Long(5)},
                         {.file_type = float32(),
                          .read_type = float64(),
                          .json = numbers,
                          .value = Literal::Double(5)},
                         {.file_type = float64(),
                          .read_type = float64(),
                          .json = numbers,
                          .value = Literal::Double(5)},
                         {.file_type = date(),
                          .read_type = date(),
                          .json = numbers,
                          .value = Literal::Date(5)},
                         {.file_type = binary(),
                          .read_type = binary(),
                          .json = R"([["a",0],["z",1],["m",2],["z",3],["a",4],["z",5]])",
                          .value = Literal::Binary({'m'})},
                         {.file_type = fixed(1),
                          .read_type = fixed(1),
                          .json = R"([["a",0],["z",1],["m",2],["z",3],["a",4],["z",5]])",
                          .value = Literal::Fixed({'m'})},
                         {.file_type = decimal(9, 2),
                          .read_type = decimal(18, 2),
                          .json = decimals,
                          .value = Literal::Decimal(500, 18, 2)},
                         {.file_type = decimal(18, 2),
                          .read_type = decimal(18, 2),
                          .json = decimals,
                          .value = Literal::Decimal(500, 18, 2)},
                         {.file_type = decimal(30, 2),
                          .read_type = decimal(30, 2),
                          .json = decimals,
                          .value = Literal::Decimal(500, 30, 2)}}) {
    SCOPED_TRACE(test.file_type->ToString() + " -> " + test.read_type->ToString());
    SetKeyType(test.file_type);
    ::parquet::WriterProperties::Builder properties;
    writer_properties_ =
        properties.enable_dictionary()->enable_store_decimal_as_integer()->build();
    ASSERT_THAT(Write(true, test.json), IsOk());
    SetKeyType(test.read_type);
    CheckDictionaries(Expressions::Equal("key", test.value), {false, true, false});
    Check(Options(Expressions::Equal("key", test.value)), {2, 3});
  }
}

TEST_F(ParquetDictionaryFilterTest, DataPageV2) {
  ::parquet::WriterProperties::Builder properties;
  writer_properties_ = properties.enable_dictionary()
                           ->data_page_version(::parquet::ParquetDataPageVersion::V2)
                           ->build();
  ASSERT_THAT(Write(false), IsOk());
  CheckDictionaries(Expressions::Equal("key", Literal::Int(10)), {false, true, false});
  Check(Options(Expressions::Equal("key", Literal::Int(10))), {2, 3});
}

TEST_F(ParquetDictionaryFilterTest, IncompatibleTypesAndMissingFields) {
  ASSERT_THAT(Write(), IsOk());
  SetKeyType(date());
  CheckDictionaries(Expressions::Equal("key", Literal::Date(100)), {true, true, true});
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "missing", int32())});
  CheckDictionaries(Expressions::Equal("missing", Literal::Int(100)), {true, true, true});
}

TEST_F(ParquetDictionaryFilterTest, NoDictionaryAndMixedEncodingRetainGroups) {
  ::parquet::WriterProperties::Builder plain;
  writer_properties_ = plain.disable_dictionary()->build();
  ASSERT_THAT(Write(), IsOk());
  CheckDictionaries(Expressions::Equal("key", Literal::Int(100)), {true, true, true});

  ::parquet::WriterProperties::Builder fallback;
  writer_properties_ = fallback.enable_dictionary()
                           ->dictionary_pagesize_limit(1)
                           ->write_batch_size(1)
                           ->data_pagesize(32)
                           ->build();
  row_group_size_ = 128;
  ::arrow::Int32Builder keys;
  ::arrow::Int64Builder positions;
  for (int i = 0; i < row_group_size_; ++i) {
    ASSERT_TRUE(keys.Append(2 * i).ok());
    ASSERT_TRUE(positions.Append(i).ok());
  }
  ArrowSchema c_schema;
  ASSERT_THAT(ToArrowSchema(*schema_, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  auto table = ::arrow::Table::Make(
      arrow_schema, {keys.Finish().ValueOrDie(), positions.Finish().ValueOrDie()});
  ASSERT_THAT(Write(table), IsOk());
  auto column = metadata_->RowGroup(0)->ColumnChunk(0);
  ASSERT_TRUE(column->has_dictionary_page());
  ASSERT_TRUE(std::ranges::any_of(column->encoding_stats(), [](const auto& stat) {
    return stat.page_type == ::parquet::PageType::DATA_PAGE &&
           stat.encoding == ::parquet::Encoding::PLAIN;
  }));
  CheckDictionaries(Expressions::Equal("key", Literal::Int(1)), {true});
  // Value 254 occurs after dictionary fallback and must remain readable.
  auto options = Options(Expressions::Equal("key", Literal::Int(254)));
  ICEBERG_UNWRAP_OR_FAIL(auto actual, Read(options));
  ASSERT_EQ(actual.size(), 128);
  EXPECT_EQ(actual.back(), 127);
}

TEST_F(ParquetDictionaryFilterTest, SplitPositionsRenameAndReuse) {
  ASSERT_THAT(Write(true, "[[5,0],[15,1],[0,2],[10,3],[5,4],[15,5]]"), IsOk());
  auto filter = Expressions::Equal("key", Literal::Int(5));
  Check(Options(filter), {0, 1, 4, 5});
  auto options = Options(filter);
  options.properties.Set(ReaderProperties::kBatchSize, int64_t{3});
  Check(options, {0, 1, 4, 5});
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  ICEBERG_UNWRAP_OR_FAIL(auto midpoint, RowGroupMidpoint(*metadata_->RowGroup(1),
                                                         input->GetSize().ValueOrDie()));
  options.split = Split{.offset = static_cast<size_t>(midpoint), .length = 1};
  Check(options, {});
  options.filter = Options(Expressions::Equal("key", Literal::Int(0))).filter;
  Check(options, {2, 3});
  options.split->length = 0;
  Check(options, {});

  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "renamed", int32())});
  Check(Options(Expressions::Equal("RENAMED", Literal::Int(5)), false), {0, 1, 4, 5});
  ICEBERG_UNWRAP_OR_FAIL(auto reader,
                         ReaderFactoryRegistry::Open(
                             FileFormatType::kParquet,
                             Options(Expressions::Equal("renamed", Literal::Int(999)))));
  ICEBERG_UNWRAP_OR_FAIL(auto empty, reader->Next());
  EXPECT_FALSE(empty.has_value());
  ASSERT_THAT(reader->Close(), IsOk());
  auto unfiltered = Options(nullptr);
  ASSERT_THAT(reader->Open(unfiltered), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch, reader->Next());
  ASSERT_TRUE(batch.has_value());
  EXPECT_GT(batch->length, 0);
  batch->release(&*batch);
  ASSERT_THAT(reader->Close(), IsOk());
}

TEST_F(ParquetDictionaryFilterTest, NameMappingWithoutFieldIdsRetainsGroups) {
  auto keys = ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0,1,10,11,20,21]")
                  .ValueOrDie();
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[0,1,2,3,4,5]").ValueOrDie();
  auto file_schema = ::arrow::schema({::arrow::field("stored_key", ::arrow::int32()),
                                      ::arrow::field("stored_value", ::arrow::int64())});
  ASSERT_THAT(Write(::arrow::Table::Make(file_schema, {keys, values})), IsOk());
  ASSERT_EQ(metadata_->schema()->Column(0)->schema_node()->field_id(), -1);
  auto mapping =
      NameMapping::Make({MappedField{.names = {"stored_key"}, .field_id = 1},
                         MappedField{.names = {"stored_value"}, .field_id = 2}});
  auto options = Options(Expressions::Equal("key", Literal::Int(100)));
  options.name_mapping = std::move(mapping);
  // Name mapping resolves the projection. Footer columns without IDs remain
  // unavailable to statistics filtering, so the reader must keep their groups.
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetDictionaryFilterTest, UnsupportedTransformRetainsGroups) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16),
                                                   Literal::Int(15))),
        {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetDictionaryFilterTest, MissingColumnWithDefaultRetainsGroups) {
  ASSERT_THAT(Write(), IsOk());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      schema_->fields()[0], schema_->fields()[1],
      SchemaField::MakeOptional(3, "defaulted", int32())
          .WithInitialDefault(std::make_shared<Literal>(Literal::Int(7)))});
  Check(Options(Expressions::Equal("defaulted", Literal::Int(7))), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetDictionaryFilterTest, RequiredLeafWithNullableParent) {
  SetKeyType(std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeRequired(3, "nested", int32())}));
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[[10],2],[[11],3],[null,4],[null,5]]"),
              IsOk());
  auto is_null = Expressions::IsNull("key.nested");
  auto not_null = Expressions::NotNull("key.nested");
  Check(Options(is_null), {0, 1, 4, 5});
  Check(Options(not_null), {2, 3});
  Check(Options(Expressions::Not(is_null)), {2, 3});
  Check(Options(Expressions::Not(not_null)), {0, 1, 4, 5});
  Check(Options(Expressions::Not(Expressions::LessThan("key.nested", Literal::Int(100)))),
        {0, 1, 4, 5});
}

TEST_F(ParquetDictionaryFilterTest, NestedPrimitiveStatistics) {
  SetKeyType(std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "nested", int32())}));
  ASSERT_THAT(Write(true, "[[[0],0],[[1],1],[[10],2],[[11],3],[[20],4],[[21],5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key.nested", Literal::Int(100))), {});
  Check(Options(Expressions::Equal("key.nested", Literal::Int(10))), {2, 3});
}

TEST_F(ParquetDictionaryFilterTest, UuidBoundsUseUnsignedByteOrder) {
  SetKeyType(uuid());
  // Group 0 holds UUIDs below 0x80..., group 1 holds UUIDs at or above it. A signed
  // byte comparison would order group 1 before group 0.
  std::vector<Uuid> uuids;
  for (const auto* str :
       {"00000000-0000-0000-0000-000000000001", "7fffffff-ffff-ffff-ffff-ffffffffffff",
        "80000000-0000-0000-0000-000000000000", "ffffffff-ffff-ffff-ffff-fffffffffffe"}) {
    ICEBERG_UNWRAP_OR_FAIL(auto uuid, Uuid::FromString(str));
    uuids.push_back(uuid);
  }
  ::arrow::FixedSizeBinaryBuilder keys(::arrow::fixed_size_binary(Uuid::kLength));
  for (const auto& uuid : uuids) {
    ASSERT_TRUE(keys.Append(uuid.bytes().data()).ok());
  }
  auto key_array = ::arrow::ExtensionType::WrapArray(::arrow::extension::uuid(),
                                                     keys.Finish().ValueOrDie());
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[0,1,2,3]").ValueOrDie();
  ArrowSchema c_schema;
  ASSERT_THAT(ToArrowSchema(*schema_, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  ASSERT_THAT(Write(::arrow::Table::Make(arrow_schema, {key_array, values})), IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 2);
  ASSERT_TRUE(metadata_->RowGroup(0)->ColumnChunk(0)->statistics()->HasMinMax());

  CheckDictionaries(Expressions::Equal("key", Literal::UUID(uuids[2])), {false, true});
  const auto& group0_max = uuids[1];
  const auto& group1_min = uuids[2];
  Check(Options(Expressions::LessThan("key", Literal::UUID(group1_min))), {0, 1});
  Check(Options(Expressions::LessThanOrEqual("key", Literal::UUID(group0_max))), {0, 1});
  Check(Options(Expressions::GreaterThan("key", Literal::UUID(group0_max))), {2, 3});
  Check(Options(Expressions::GreaterThanOrEqual("key", Literal::UUID(group1_min))),
        {2, 3});
  Check(Options(Expressions::Equal("key", Literal::UUID(group1_min))), {2, 3});
  Check(
      Options(Expressions::Not(Expressions::LessThan("key", Literal::UUID(group1_min)))),
      {2, 3});
}

// Substitute metadata/pages while retaining the real file's dictionary bytes.
class DictionaryTestPageReader : public ::parquet::PageReader {
 public:
  explicit DictionaryTestPageReader(std::shared_ptr<::parquet::Page> page)
      : page_(std::move(page)) {}
  std::shared_ptr<::parquet::Page> NextPage() override {
    return std::exchange(page_, nullptr);
  }
  void set_max_page_header_size(uint32_t) override {}

 private:
  std::shared_ptr<::parquet::Page> page_;
};

class DictionaryTestContents : public ::parquet::RowGroupReader::Contents {
 public:
  DictionaryTestContents(std::shared_ptr<::parquet::RowGroupReader> reader,
                         std::unique_ptr<::parquet::RowGroupMetaData> metadata,
                         int* page_reads, bool replace_page = false,
                         std::shared_ptr<::parquet::Page> page = nullptr)
      : reader_(std::move(reader)),
        metadata_(std::move(metadata)),
        page_reads_(page_reads),
        replace_page_(replace_page),
        page_(std::move(page)) {}
  std::unique_ptr<::parquet::PageReader> GetColumnPageReader(int i) override {
    ++*page_reads_;
    return replace_page_ ? std::make_unique<DictionaryTestPageReader>(page_)
                         : reader_->GetColumnPageReader(i);
  }
  const ::parquet::RowGroupMetaData* metadata() const override { return metadata_.get(); }
  const ::parquet::ReaderProperties* properties() const override { return &properties_; }

 private:
  std::shared_ptr<::parquet::RowGroupReader> reader_;
  std::unique_ptr<::parquet::RowGroupMetaData> metadata_;
  int* page_reads_;
  bool replace_page_;
  std::shared_ptr<::parquet::Page> page_;
  ::parquet::ReaderProperties properties_;
};

TEST_F(ParquetDictionaryFilterTest, EncodingMetadataAndDictionaryCache) {
  ASSERT_THAT(Write(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  auto reader = ::parquet::ParquetFileReader::Open(input);
  ::parquet::arrow::SchemaManifest manifest;
  ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(
                  metadata_->schema(), nullptr,
                  ::parquet::default_arrow_reader_properties(), &manifest)
                  .ok());
  auto expr = Expressions::And(Expressions::Equal("key", Literal::Int(100)),
                               Expressions::Equal("key", Literal::Int(101)));
  ICEBERG_UNWRAP_OR_FAIL(auto filter, ParquetDictionaryRowGroupFilter::Make(
                                          Options(expr).filter, *metadata_->schema()));
  int reads = 0;
  ::parquet::RowGroupReader tracked(std::make_unique<DictionaryTestContents>(
      reader->RowGroup(0), metadata_->RowGroup(0), &reads));
  ICEBERG_UNWRAP_OR_FAIL(auto result, filter->ShouldRead(manifest, tracked));
  EXPECT_FALSE(result);
  EXPECT_EQ(reads, 1);  // Both predicates share one decoded dictionary.

  // With neither data-page encoding statistics nor an unambiguous legacy
  // encoding list, pruning must not read or rely on the dictionary.
  {
    auto builder = ::parquet::FileMetaDataBuilder::Make(
        metadata_->schema(), ::parquet::default_writer_properties());
    auto original = metadata_->RowGroup(0);
    auto row_group = builder->AppendRowGroup();
    row_group->set_num_rows(original->num_rows());
    for (int i = 0; i < original->num_columns(); ++i) {
      auto column = original->ColumnChunk(i);
      row_group->NextColumnChunk()->Finish(
          column->num_values(), column->dictionary_page_offset(), 0,
          column->data_page_offset(), column->total_compressed_size(),
          column->total_uncompressed_size(), true, false, {}, {});
    }
    row_group->Finish(original->total_byte_size());
    auto rebuilt = builder->Finish();
    ASSERT_TRUE(rebuilt->RowGroup(0)->ColumnChunk(0)->encoding_stats().empty());
    reads = 0;
    ::parquet::RowGroupReader group(std::make_unique<DictionaryTestContents>(
        reader->RowGroup(0), rebuilt->RowGroup(0), &reads));
    ICEBERG_UNWRAP_OR_FAIL(auto should_read, filter->ShouldRead(manifest, group));
    EXPECT_TRUE(should_read);
    EXPECT_EQ(reads, 0);
  }
}

TEST_F(ParquetDictionaryFilterTest, MissingAndTruncatedDictionaryReturnErrors) {
  ASSERT_THAT(Write(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  auto reader = ::parquet::ParquetFileReader::Open(input);
  ::parquet::arrow::SchemaManifest manifest;
  ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(
                  metadata_->schema(), nullptr,
                  ::parquet::default_arrow_reader_properties(), &manifest)
                  .ok());
  ICEBERG_UNWRAP_OR_FAIL(auto filter,
                         ParquetDictionaryRowGroupFilter::Make(
                             Options(Expressions::Equal("key", Literal::Int(100))).filter,
                             *metadata_->schema()));
  std::shared_ptr<::parquet::Page> truncated =
      std::make_shared<::parquet::DictionaryPage>(
          std::make_shared<::arrow::Buffer>(nullptr, 0), 2, ::parquet::Encoding::PLAIN);
  for (const auto& page :
       std::vector<std::shared_ptr<::parquet::Page>>{nullptr, truncated}) {
    int reads = 0;
    ::parquet::RowGroupReader group(std::make_unique<DictionaryTestContents>(
        reader->RowGroup(0), metadata_->RowGroup(0), &reads, true, page));
    EXPECT_FALSE(filter->ShouldRead(manifest, group).has_value());
  }
}

}  // namespace
}  // namespace iceberg::parquet
