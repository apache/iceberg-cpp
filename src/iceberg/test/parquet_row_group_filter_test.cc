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

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"

namespace iceberg::parquet {
namespace {

class ParquetRowGroupFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    parquet::RegisterAll();
    io_ = std::make_shared<MockFileIO>();
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", int32()),
                                 SchemaField::MakeRequired(2, "value", int64())});
    projection_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(2, "value", int64()),
                                 MetadataColumns::kRowPosition, MetadataColumns::kRowId});
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
    return WriteTable(table, statistics);
  }

  Status WriteTable(const std::shared_ptr<::arrow::Table>& table, bool statistics) {
    ICEBERG_ASSIGN_OR_RAISE(auto out, arrow::OpenArrowOutputStream(io_, path_));
    ::parquet::WriterProperties::Builder properties;
    properties.disable_dictionary();
    if (!statistics) {
      properties.disable_statistics();
    }
    ICEBERG_ARROW_RETURN_NOT_OK(::parquet::arrow::WriteTable(
        *table, ::arrow::default_memory_pool(), out, 2, properties.build()));
    ICEBERG_ARROW_RETURN_NOT_OK(out->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto input, arrow::OpenArrowInputStream(io_, path_));
    metadata_ = ::parquet::ParquetFileReader::Open(input)->metadata();
    return {};
  }

  ReaderOptions Options(std::shared_ptr<Expression> filter, bool case_sensitive = true) {
    // Direct readers need bound references for columns outside the projection.
    // Leave invalid or already-bound expressions intact for their dedicated tests.
    if (filter) {
      auto bound = Binder::Bind(*schema_, filter, case_sensitive);
      if (bound) {
        filter = *bound;
      }
    }
    ReaderOptions options{.path = path_,
                          .io = io_,
                          .projection = projection_,
                          .filter = std::move(filter),
                          .first_row_id = 100};
    options.properties.Set(ReaderProperties::kFilterCaseSensitive, case_sensitive);
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
    ICEBERG_UNWRAP_OR_FAIL(auto actual, Read(options));
    EXPECT_EQ(actual, expected);
  }

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

TEST_F(ParquetRowGroupFilterTest, BatchesStayWithinRowGroups) {
  ASSERT_THAT(Write(), IsOk());
  auto options = Options(nullptr);
  options.properties.Set(ReaderProperties::kBatchSize, int64_t{32});
  for (bool filtered : {false, true}) {
    SCOPED_TRACE(filtered);
    if (filtered) {
      options.filter =
          Expressions::Or(Expressions::LessThan("key", Literal::Int(2)),
                          Expressions::GreaterThanOrEqual("key", Literal::Int(20)));
      ICEBERG_UNWRAP_OR_FAIL(options.filter,
                             Binder::Bind(*schema_, options.filter, true));
    }
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, options));
    std::vector<int64_t> batch_sizes;
    while (true) {
      ICEBERG_UNWRAP_OR_FAIL(auto array, reader->Next());
      if (!array) {
        break;
      }
      batch_sizes.push_back(array->length);
      array->release(&*array);
    }
    EXPECT_EQ(batch_sizes,
              filtered ? std::vector<int64_t>({2, 2}) : std::vector<int64_t>({2, 2, 2}));
    ICEBERG_UNWRAP_OR_FAIL(auto end, reader->Next());
    EXPECT_FALSE(end.has_value());
    ASSERT_THAT(reader->Close(), IsOk());
  }
}

TEST_F(ParquetRowGroupFilterTest, AllNoneAndResidualRows) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(nullptr), {0, 1, 2, 3, 4, 5});
  Check(Options(True::Instance()), {0, 1, 2, 3, 4, 5});
  Check(Options(False::Instance()), {});
  Check(Options(Expressions::Equal("key", Literal::Int(100))), {});
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
  options.properties.Set(ReaderProperties::kFilterCaseSensitive, false);
  Check(options, {4, 5});
}

TEST_F(ParquetRowGroupFilterTest, MissingStatsUnknownFieldsAndTypePromotion) {
  ASSERT_THAT(Write(false), IsOk());
  Check(Options(Expressions::Equal("key", Literal::Int(100))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::IsNull("key")), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::NotNull("key")), {0, 1, 2, 3, 4, 5});
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal("missing", Literal::Int(0))), {0, 1, 2, 3, 4, 5});
  auto options = Options(Expressions::Equal("key", Literal::Int(0)));
  options.filter = Expressions::Equal("key", Literal::Int(0));
  Check(options, {0, 1, 2, 3, 4, 5});
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "key", int64()), schema_->fields()[1],
      SchemaField::MakeOptional(3, "defaulted", int32())
          .WithInitialDefault(std::make_shared<Literal>(Literal::Int(7)))});
  Check(Options(Expressions::Equal("key", Literal::Long(100))), {});
  Check(Options(Expressions::Equal("key", Literal::Long(10))), {2, 3});
  Check(Options(Expressions::Equal("defaulted", Literal::Int(8))), {0, 1, 2, 3, 4, 5});
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
  for (const auto& test : std::vector<Case>{
           {float32(), float64(), numbers, Literal::Double(10), true},
           {float64(), float32(), numbers, Literal::Float(10), false},
           {int64(), int32(), numbers, Literal::Int(10), false},
           {int32(), date(), numbers, Literal::Date(10), false},
           {date(), int32(), numbers, Literal::Int(10), false},
           {decimal(9, 2), decimal(18, 2), decimals, Literal::Decimal(1000, 18, 2), true},
           {decimal(9, 2), decimal(8, 2), decimals, Literal::Decimal(1000, 8, 2), false},
           {decimal(9, 2), decimal(9, 3), decimals, Literal::Decimal(1000, 9, 3), false},
           {timestamp(), timestamp_ns(), timestamps, Literal::TimestampNs(10000000),
            false},
           {timestamp(), timestamp_tz(), timestamps, Literal::TimestampTz(10000000),
            false},
           {timestamp_ns(), timestamp_ns(), timestamps, Literal::TimestampNs(10000000000),
            true},
           {timestamp_tz(), timestamp_tz(), timestamps, Literal::TimestampTz(10000000),
            true},
           {fixed(1), fixed(1), R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            Literal::Fixed({'m'}), true},
           {fixed(1), fixed(2), R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            Literal::Fixed({'m', 'm'}), false}}) {
    SCOPED_TRACE(test.file_type->ToString() + " -> " + test.filter_type->ToString());
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeOptional(1, "key", test.file_type), schema_->fields()[1]});
    ASSERT_THAT(Write(true, test.json), IsOk());
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeOptional(1, "key", test.filter_type), schema_->fields()[1]});
    Check(Options(Expressions::Equal("key", test.value)),
          test.compatible ? std::vector<int64_t>{2, 3}
                          : std::vector<int64_t>{0, 1, 2, 3, 4, 5});
  }
}

TEST_F(ParquetRowGroupFilterTest, NullNegativeAndUnsupportedBooleanBranches) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  Check(Options(Expressions::IsNull("key")), {0, 1});
  Check(Options(Expressions::NotNull("key")), {2, 3, 4, 5});
  Check(Options(Expressions::NotEqual("key", Literal::Int(10))), {0, 1, 2, 3, 4, 5});
  auto unsupported = Expressions::Equal("missing", Literal::Int(0));
  ICEBERG_UNWRAP_OR_FAIL(
      auto supported,
      Binder::Bind(*schema_, Expressions::Equal("key", Literal::Int(20)), true));
  Check(Options(Expressions::And(supported, unsupported)), {4, 5});
  Check(Options(Expressions::Or(supported, unsupported)), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(unsupported)), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(supported)), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, RewriteNotBeforeBinding) {
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

  // Rewrite first: weakening an unknown predicate before NOT could discard rows.
  auto unknown = Expressions::Equal("missing", Literal::Int(0));
  Check(Options(Expressions::Not(Expressions::Or(bound, unknown))), {2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::And(bound, unknown))), {0, 1, 2, 3, 4, 5});

  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  Check(Options(Expressions::Not(Expressions::IsNull("key"))), {2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::NotNull("key"))), {0, 1});
}

TEST_F(ParquetRowGroupFilterTest, BoundComparisonBoundariesAndAllNullGroups) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[10,2],[11,3],[20,4],[21,5]]"), IsOk());
  struct Case {
    std::shared_ptr<Expression> filter;
    std::vector<int64_t> expected;
  };
  for (const auto& test : std::vector<Case>{
           {Expressions::LessThan("key", Literal::Int(10)), {}},
           {Expressions::LessThanOrEqual("key", Literal::Int(10)), {2, 3}},
           {Expressions::GreaterThan("key", Literal::Int(21)), {}},
           {Expressions::GreaterThanOrEqual("key", Literal::Int(21)), {4, 5}},
           {Expressions::Equal("key", Literal::Int(11)), {2, 3}},
           {Expressions::In("key", {Literal::Int(11), Literal::Int(20)}), {2, 3, 4, 5}},
       }) {
    ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*schema_, test.filter, true));
    Check(Options(bound), test.expected);
  }
}

TEST_F(ParquetRowGroupFilterTest, InPredicateLimit) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::In("key", {Literal::Int(0), Literal::Int(20)})),
        {0, 1, 4, 5});
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
  auto offset = metadata_->RowGroup(1)->file_offset();
  ASSERT_GT(offset, metadata_->RowGroup(0)->file_offset());
  auto options = Options(Expressions::GreaterThanOrEqual("key", Literal::Int(20)));
  options.split = Split{static_cast<size_t>(offset), 100000};
  Check(options, {4, 5});
  ICEBERG_UNWRAP_OR_FAIL(
      options.filter,
      Binder::Bind(*schema_, Expressions::LessThan("key", Literal::Int(2)), true));
  Check(options, {});
  options.filter = nullptr;
  options.split = Split{static_cast<size_t>(offset), 0};
  Check(options, {});
}

TEST_F(ParquetRowGroupFilterTest, PrimitiveComparisonTypes) {
  struct Case {
    std::shared_ptr<Type> type;
    std::string json;
    Literal literal;
  };
  for (const auto& test : std::vector<Case>{
           {int64(), "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]", Literal::Long(10)},
           {date(), "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]", Literal::Date(10)},
           {string(), R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            Literal::String("m")},
           {boolean(), "[[false,0],[false,1],[true,2],[true,3],[false,4],[false,5]]",
            Literal::Boolean(true)},
           {binary(), R"([["a",0],["b",1],["m",2],["n",3],["y",4],["z",5]])",
            Literal::Binary({'m'})}}) {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", test.type),
                                 SchemaField::MakeRequired(2, "value", int64())});
    ASSERT_THAT(Write(true, test.json), IsOk());
    Check(Options(Expressions::Equal("key", test.literal)), {2, 3});
  }
}

TEST_F(ParquetRowGroupFilterTest, FloatAndNestedStatsWithTransformFallback) {
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", float64()),
                               SchemaField::MakeRequired(2, "value", int64())});
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(100))), {});
  Check(Options(Expressions::Equal("key", Literal::Double(10))), {2, 3});
  Check(Options(Expressions::IsNaN("key")), {0, 1, 2, 3, 4, 5});
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "key",
                                std::make_shared<StructType>(std::vector<SchemaField>{
                                    SchemaField::MakeOptional(3, "nested", int32())})),
      SchemaField::MakeRequired(2, "value", int64())});
  ASSERT_THAT(Write(true, "[[[0],0],[[1],1],[[10],2],[[11],3],[[20],4],[[21],5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key.nested", Literal::Int(100))), {});
  Check(Options(Expressions::Equal("key.nested", Literal::Int(10))), {2, 3});
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", int32()),
                               SchemaField::MakeRequired(2, "value", int64())});
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16),
                                                   Literal::Int(15))),
        {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, NegativeAndPrefixPredicates) {
  ASSERT_THAT(Write(true, "[[5,0],[5,1],[null,2],[5,3],[6,4],[6,5]]"), IsOk());
  Check(Options(Expressions::NotEqual("key", Literal::Int(5))), {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::NotIn("key", {Literal::Int(5), Literal::Int(6)})),
        {0, 1, 2, 3, 4, 5});
  Check(Options(Expressions::Not(Expressions::LessThan("key", Literal::Int(100)))), {});
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "key", string()), schema_->fields()[1]});
  ASSERT_THAT(
      Write(
          true,
          R"([["apple",0],["apricot",1],["banana",2],["blueberry",3],[null,4],["apricot",5]])"),
      IsOk());
  Check(Options(Expressions::StartsWith("key", "ap")), {0, 1, 4, 5});
  Check(Options(Expressions::NotStartsWith("key", "ap")), {2, 3, 4, 5});
}

TEST_F(ParquetRowGroupFilterTest, DecimalAndTemporalStatistics) {
  struct Case {
    std::shared_ptr<Type> type;
    std::string json;
    Literal value;
  };
  for (
      const auto& test : std::vector<Case>{
          {decimal(9, 2),
           R"([["0.00",0],["1.00",1],["10.00",2],["11.00",3],["20.00",4],["21.00",5]])",
           Literal::Decimal(1000, 9, 2)},
          {time(), "[[0,0],[1,1],[10,2],[11,3],[20,4],[21,5]]", Literal::Time(10)},
          {timestamp(),
           R"([["1970-01-01 00:00:00",0],["1970-01-01 00:00:01",1],["1970-01-01 00:00:10",2],["1970-01-01 00:00:11",3],["1970-01-01 00:00:20",4],["1970-01-01 00:00:21",5]])",
           Literal::Timestamp(10000000)}}) {
    schema_ = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeOptional(1, "key", test.type),
                                 SchemaField::MakeRequired(2, "value", int64())});
    ASSERT_THAT(Write(true, test.json), IsOk());
    Check(Options(Expressions::Equal("key", test.value)), {2, 3});
    Check(Options(Expressions::LessThan("key", test.value)), {0, 1});
  }
}

TEST_F(ParquetRowGroupFilterTest, FloatingPointNullNaNAndSignedZero) {
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(1, "key", float64()), schema_->fields()[1]});
  ASSERT_THAT(Write(true, "[[-0.0,0],[0.0,1],[null,2],[null,3],[10.0,4],[11.0,5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(-0.0))), {0, 1});
  Check(Options(Expressions::Equal("key", Literal::Double(0.0))), {0, 1});
  Check(Options(Expressions::IsNull("key")), {2, 3});
  Check(Options(Expressions::NotNull("key")), {0, 1, 4, 5});
  Check(Options(Expressions::IsNaN("key")), {0, 1, 4, 5});
  Check(Options(Expressions::LessThan("key", Literal::Double(-100))), {});
  ASSERT_THAT(Write(true, "[[NaN,0],[-NaN,1],[10.0,2],[11.0,3],[null,4],[null,5]]"),
              IsOk());
  Check(Options(Expressions::Equal("key", Literal::Double(100))), {0, 1});
  Check(Options(Expressions::IsNaN("key")), {0, 1, 2, 3});
  Check(Options(Expressions::LessThan("key", Literal::Double(-100))), {0, 1});
}

TEST_F(ParquetRowGroupFilterTest, InvalidFilterFailsDuringOpen) {
  ASSERT_THAT(Write(), IsOk());
  auto options = Options(Expressions::Count("key"));
  options.filter = Expressions::Count("key");
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("does not support unbound aggregate"));

  ICEBERG_UNWRAP_OR_FAIL(auto bound, Binder::Bind(*schema_, options.filter, true));
  auto bound_options = Options(bound);
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, bound_options),
              HasErrorMessage("does not support bound aggregate"));

  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {0, 1, 2, 3, 4, 5});
}

}  // namespace
}  // namespace iceberg::parquet
