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

#include <limits>
#include <map>

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/extension/uuid.h>
#include <arrow/io/memory.h>
#include <arrow/json/from_string.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <parquet/arrow/writer.h>
#include <parquet/bloom_filter.h>
#include <parquet/bloom_filter_reader.h>
#include <parquet/column_writer.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/statistics.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/expression/binder.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/file_reader.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/parquet/parquet_bloom_row_group_filter_internal.h"
#include "iceberg/parquet/parquet_data_util_internal.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"
#include "iceberg/util/uuid.h"

namespace iceberg::parquet {
namespace {

struct BloomReadRange {
  int64_t offset;
  int64_t length;
  bool operator==(const BloomReadRange&) const = default;
};

class BloomRecordingStream : public SeekableInputStream {
 public:
  BloomRecordingStream(std::unique_ptr<SeekableInputStream> stream,
                       std::vector<BloomReadRange>& reads)
      : stream_(std::move(stream)), reads_(reads) {}
  Result<int64_t> Position() const override { return stream_->Position(); }
  Status Seek(int64_t position) override { return stream_->Seek(position); }
  Result<int64_t> Read(std::span<std::byte> out) override {
    ICEBERG_ASSIGN_OR_RAISE(auto position, stream_->Position());
    reads_.push_back({.offset = position, .length = static_cast<int64_t>(out.size())});
    return stream_->Read(out);
  }
  Status ReadFully(int64_t position, std::span<std::byte> out) override {
    reads_.push_back({.offset = position, .length = static_cast<int64_t>(out.size())});
    return stream_->ReadFully(position, out);
  }
  Status Close() override { return stream_->Close(); }

 private:
  std::unique_ptr<SeekableInputStream> stream_;
  std::vector<BloomReadRange>& reads_;
};

class BloomRecordingInput : public InputFile {
 public:
  BloomRecordingInput(std::unique_ptr<InputFile> input,
                      std::vector<BloomReadRange>& reads)
      : input_(std::move(input)), reads_(reads) {}
  std::string_view location() const override { return input_->location(); }
  Result<int64_t> Size() const override { return input_->Size(); }
  Result<std::unique_ptr<SeekableInputStream>> Open() override {
    ICEBERG_ASSIGN_OR_RAISE(auto stream, input_->Open());
    return std::make_unique<BloomRecordingStream>(std::move(stream), reads_);
  }

 private:
  std::unique_ptr<InputFile> input_;
  std::vector<BloomReadRange>& reads_;
};

class BloomRecordingFileIO : public MockFileIO {
 public:
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path) override {
    ICEBERG_ASSIGN_OR_RAISE(auto file, MockFileIO::NewInputFile(std::move(path)));
    return std::make_unique<BloomRecordingInput>(std::move(file), reads);
  }
  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path,
                                                  size_t length) override {
    ICEBERG_ASSIGN_OR_RAISE(auto file, MockFileIO::NewInputFile(std::move(path), length));
    return std::make_unique<BloomRecordingInput>(std::move(file), reads);
  }
  std::vector<BloomReadRange> reads;
};

class ParquetBloomFilterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    parquet::RegisterAll();
    io_ = std::make_shared<BloomRecordingFileIO>();
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

  Status Write(bool statistics = false,
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
    properties.enable_store_decimal_as_integer();
    if (write_bloom_) {
      properties.enable_bloom_filter("key", {.ndv = 1000, .fpp = 0.0001});
      properties.enable_bloom_filter("key.nested", {.ndv = 1000, .fpp = 0.0001});
    }
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

  bool write_bloom_ = true;
  int64_t row_group_size_ = 2;
  std::shared_ptr<::parquet::WriterProperties> writer_properties_;
  std::string path_ = "bloom.parquet";
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Schema> projection_;
  std::shared_ptr<::parquet::FileMetaData> metadata_;

  void CheckBloom(const std::shared_ptr<Expression>& expression,
                  const std::vector<bool>& expected, bool case_sensitive = true) {
    ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
    auto reader = ::parquet::ParquetFileReader::Open(input);
    ::parquet::arrow::SchemaManifest manifest;
    ::parquet::ArrowReaderProperties properties;
    properties.set_arrow_extensions_enabled(true);
    ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(metadata_->schema(),
                                                       metadata_->key_value_metadata(),
                                                       properties, &manifest)
                    .ok());
    ICEBERG_UNWRAP_OR_FAIL(auto filter, ParquetBloomRowGroupFilter::Make(
                                            Options(expression, case_sensitive).filter,
                                            *metadata_->schema()));
    ASSERT_EQ(expected.size(), metadata_->num_row_groups());
    for (int i = 0; i < metadata_->num_row_groups(); ++i) {
      SCOPED_TRACE(expression->ToString() + " group " + std::to_string(i));
      auto bloom_reader = reader->GetBloomFilterReader().RowGroup(i);
      ICEBERG_UNWRAP_OR_FAIL(
          auto actual,
          filter->ShouldRead(manifest, *metadata_->RowGroup(i), bloom_reader.get()));
      EXPECT_EQ(actual, expected[i]);
    }
  }
};

TEST_F(ParquetBloomFilterTest, EqualityMembershipAndInclusiveBooleanRules) {
  ASSERT_THAT(Write(true, "[[0,0],[10,1],[5,2],[15,3],[0,4],[10,5]]"), IsOk());
  CheckBloom(Expressions::Equal("key", Literal::Int(5)), {false, true, false});
  CheckBloom(Expressions::In("key", {Literal::Int(5), Literal::Int(7)}),
             {false, true, false});
  auto supported = Expressions::Equal("key", Literal::Int(5));
  auto unsupported = Expressions::LessThan("key", Literal::Int(100));
  CheckBloom(Expressions::And(supported, unsupported), {false, true, false});
  CheckBloom(Expressions::Or(supported, unsupported), {true, true, true});
  CheckBloom(Expressions::Not(Expressions::NotEqual("key", Literal::Int(5))),
             {false, true, false});
  CheckBloom(
      Expressions::Not(Expressions::NotIn("key", {Literal::Int(5), Literal::Int(7)})),
      {false, true, false});
  CheckBloom(Expressions::AlwaysTrue(), {true, true, true});
  CheckBloom(Expressions::AlwaysFalse(), {false, false, false});
  Check(Options(supported), {2, 3});
  Check(Options(Expressions::In("key", {Literal::Int(5), Literal::Int(7)})), {2, 3});
  auto options = Options(supported);
  options.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  Check(options, {0, 1, 2, 3, 4, 5});
  Check(Options(nullptr), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetBloomFilterTest, UnsupportedPredicatesAndNullsRemainInclusive) {
  ASSERT_THAT(Write(true, "[[null,0],[null,1],[5,2],[5,3],[10,4],[10,5]]"), IsOk());
  for (const auto& expression : std::vector<std::shared_ptr<Expression>>{
           Expressions::IsNull("key"), Expressions::NotNull("key"),
           Expressions::LessThan("key", Literal::Int(1)),
           Expressions::LessThanOrEqual("key", Literal::Int(1)),
           Expressions::GreaterThan("key", Literal::Int(100)),
           Expressions::GreaterThanOrEqual("key", Literal::Int(100)),
           Expressions::NotEqual("key", Literal::Int(5)),
           Expressions::NotIn("key", {Literal::Int(5), Literal::Int(10)}),
           Expressions::Not(Expressions::LessThan("key", Literal::Int(5)))}) {
    CheckBloom(expression, {true, true, true});
  }
  SetKeyType(string());
  ASSERT_THAT(Write(true, R"([["abc",0],["abc",1],[null,2],["abc",3]])"), IsOk());
  CheckBloom(Expressions::StartsWith("key", "z"), {true, true});
  CheckBloom(Expressions::NotStartsWith("key", "a"), {true, true});
}

TEST_F(ParquetBloomFilterTest, PhysicalTypesAndSchemaPromotions) {
  struct Case {
    std::shared_ptr<Type> file_type;
    std::shared_ptr<Type> read_type;
    std::string json;
    Literal present;
    Literal absent;
  };
  const std::string numbers = "[[0,0],[10,1],[5,2],[15,3],[0,4],[10,5]]";
  const std::string decimals =
      R"([["0.00",0],["10.00",1],["5.00",2],["15.00",3],["0.00",4],["10.00",5]])";
  const std::string timestamps =
      R"([["1970-01-01 00:00:00",0],["1970-01-01 00:00:10",1],["1970-01-01 00:00:05",2],["1970-01-01 00:00:15",3],["1970-01-01 00:00:00",4],["1970-01-01 00:00:10",5]])";
  const std::string strings = R"([["a",0],["z",1],["m",2],["z",3],["a",4],["z",5]])";
  for (const auto& test :
       std::vector<Case>{{.file_type = int32(),
                          .read_type = int32(),
                          .json = numbers,
                          .present = Literal::Int(5),
                          .absent = Literal::Int(7)},
                         {.file_type = int32(),
                          .read_type = int64(),
                          .json = numbers,
                          .present = Literal::Long(5),
                          .absent = Literal::Long(7)},
                         {.file_type = int64(),
                          .read_type = int64(),
                          .json = numbers,
                          .present = Literal::Long(5),
                          .absent = Literal::Long(7)},
                         {.file_type = float32(),
                          .read_type = float32(),
                          .json = numbers,
                          .present = Literal::Float(5),
                          .absent = Literal::Float(7)},
                         {.file_type = float32(),
                          .read_type = float64(),
                          .json = numbers,
                          .present = Literal::Double(5),
                          .absent = Literal::Double(7)},
                         {.file_type = float64(),
                          .read_type = float64(),
                          .json = numbers,
                          .present = Literal::Double(5),
                          .absent = Literal::Double(7)},
                         {.file_type = date(),
                          .read_type = date(),
                          .json = numbers,
                          .present = Literal::Date(5),
                          .absent = Literal::Date(7)},
                         {.file_type = time(),
                          .read_type = time(),
                          .json = numbers,
                          .present = Literal::Time(5),
                          .absent = Literal::Time(7)},
                         {.file_type = timestamp(),
                          .read_type = timestamp(),
                          .json = timestamps,
                          .present = Literal::Timestamp(5000000),
                          .absent = Literal::Timestamp(7000000)},
                         {.file_type = timestamp_tz(),
                          .read_type = timestamp_tz(),
                          .json = timestamps,
                          .present = Literal::TimestampTz(5000000),
                          .absent = Literal::TimestampTz(7000000)},
                         {.file_type = timestamp_ns(),
                          .read_type = timestamp_ns(),
                          .json = timestamps,
                          .present = Literal::TimestampNs(5000000000),
                          .absent = Literal::TimestampNs(7000000000)},
                         {.file_type = timestamptz_ns(),
                          .read_type = timestamptz_ns(),
                          .json = timestamps,
                          .present = Literal::TimestampTzNs(5000000000),
                          .absent = Literal::TimestampTzNs(7000000000)},
                         {.file_type = string(),
                          .read_type = string(),
                          .json = strings,
                          .present = Literal::String("m"),
                          .absent = Literal::String("x")},
                         {.file_type = binary(),
                          .read_type = binary(),
                          .json = strings,
                          .present = Literal::Binary({'m'}),
                          .absent = Literal::Binary({'x'})},
                         {.file_type = fixed(1),
                          .read_type = fixed(1),
                          .json = strings,
                          .present = Literal::Fixed({'m'}),
                          .absent = Literal::Fixed({'x'})},
                         {.file_type = decimal(9, 2),
                          .read_type = decimal(9, 2),
                          .json = decimals,
                          .present = Literal::Decimal(500, 9, 2),
                          .absent = Literal::Decimal(700, 9, 2)},
                         {.file_type = decimal(9, 2),
                          .read_type = decimal(18, 2),
                          .json = decimals,
                          .present = Literal::Decimal(500, 18, 2),
                          .absent = Literal::Decimal(700, 18, 2)},
                         {.file_type = decimal(18, 2),
                          .read_type = decimal(18, 2),
                          .json = decimals,
                          .present = Literal::Decimal(500, 18, 2),
                          .absent = Literal::Decimal(700, 18, 2)},
                         {.file_type = decimal(30, 2),
                          .read_type = decimal(38, 2),
                          .json = decimals,
                          .present = Literal::Decimal(500, 38, 2),
                          .absent = Literal::Decimal(700, 38, 2)}}) {
    SCOPED_TRACE(test.file_type->ToString() + " -> " + test.read_type->ToString());
    SetKeyType(test.file_type);
    ASSERT_THAT(Write(false, test.json), IsOk());
    SetKeyType(test.read_type);
    CheckBloom(Expressions::Equal("key", test.present), {false, true, false});
    CheckBloom(Expressions::Equal("key", test.absent), {false, false, false});
    CheckBloom(Expressions::In("key", {test.present, test.absent}), {false, true, false});
    Check(Options(Expressions::Equal("key", test.present)), {2, 3});
  }
}

TEST_F(ParquetBloomFilterTest, MissingBloomFiltersAndFields) {
  write_bloom_ = false;
  ASSERT_THAT(Write(), IsOk());
  CheckBloom(Expressions::Equal("key", Literal::Int(100)), {true, true, true});
  Check(Options(Expressions::Equal("key", Literal::Int(100))), {0, 1, 2, 3, 4, 5});
  write_bloom_ = true;
  ASSERT_THAT(Write(), IsOk());
  schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeOptional(3, "missing", int32())
          .WithInitialDefault(std::make_shared<Literal>(Literal::Int(100)))});
  // Initial-default fields are absent from the physical schema.
  CheckBloom(Expressions::Equal("missing", Literal::Int(100)), {true, true, true});
  Check(Options(Expressions::Equal("missing", Literal::Int(100))), {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetBloomFilterTest, IncompatibleTypesRetainGroups) {
  ASSERT_THAT(Write(), IsOk());
  SetKeyType(date());
  CheckBloom(Expressions::Equal("key", Literal::Date(100)), {true, true, true});
  SetKeyType(int64());
  CheckBloom(
      Expressions::Equal("key", Literal::Long(std::numeric_limits<int64_t>::max())),
      {false, false, false});
  SetKeyType(float64());
  CheckBloom(Expressions::Equal("key", Literal::Double(100)), {true, true, true});
}

TEST_F(ParquetBloomFilterTest, RenameCaseSensitivityAndUnboundReferences) {
  ASSERT_THAT(Write(), IsOk());
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeOptional(1, "renamed", int32())});
  CheckBloom(Expressions::Equal("RENAMED", Literal::Int(10)), {false, true, false},
             false);
  auto options = Options(Expressions::Equal("RENAMED", Literal::Int(10)), false);
  Check(options, {2, 3});
  options.filter =
      Expressions::Equal("value", Literal::Long(2));  // Projected but has no Bloom.
  Check(options, {0, 1, 2, 3, 4, 5});
  options.filter = Expressions::Equal("unresolved", Literal::Int(2));
  EXPECT_THAT(ReaderFactoryRegistry::Open(FileFormatType::kParquet, options),
              HasErrorMessage("Cannot find field"));
}

TEST_F(ParquetBloomFilterTest, NonContiguousGroupsSplitsAndLineage) {
  ASSERT_THAT(Write(false, "[[5,0],[15,1],[0,2],[10,3],[5,4],[15,5]]"), IsOk());
  auto options = Options(Expressions::Equal("key", Literal::Int(5)));
  for (int64_t batch_size : {1, 2, 3, 8}) {
    options.properties.Set(ReaderProperties::kBatchSize, batch_size);
    Check(options, {0, 1, 4, 5});
  }
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  ICEBERG_UNWRAP_OR_FAIL(auto midpoint, RowGroupMidpoint(*metadata_->RowGroup(1),
                                                         input->GetSize().ValueOrDie()));
  options.split = Split{.offset = static_cast<size_t>(midpoint), .length = 1};
  Check(options, {});
  options.filter = Options(Expressions::Equal("key", Literal::Int(0))).filter;
  Check(options, {2, 3});
  options.split->length = 0;
  Check(options, {});
}

TEST_F(ParquetBloomFilterTest, NaNsAndSignedZeros) {
  for (const auto& type : std::vector<std::shared_ptr<Type>>{float32(), float64()}) {
    SetKeyType(type);
    ASSERT_THAT(Write(false, "[[NaN,0],[NaN,1],[null,2],[0,3],[-0.0,4],[-0.0,5]]"),
                IsOk());
    CheckBloom(Expressions::IsNaN("key"), {true, true, true});
    CheckBloom(Expressions::NotNaN("key"), {true, true, true});
    auto positive =
        type->type_id() == TypeId::kFloat ? Literal::Float(0.0F) : Literal::Double(0.0);
    auto negative =
        type->type_id() == TypeId::kFloat ? Literal::Float(-0.0F) : Literal::Double(-0.0);
    CheckBloom(Expressions::Equal("key", positive), {false, true, false});
    CheckBloom(Expressions::Equal("key", negative), {false, false, true});
  }
}
TEST_F(ParquetBloomFilterTest, NameMappingWithoutFieldIdsRetainsGroups) {
  ::parquet::WriterProperties::Builder properties;
  writer_properties_ =
      properties.disable_dictionary()
          ->enable_bloom_filter("stored_key", {.ndv = 1000, .fpp = 0.0001})
          ->build();
  auto keys = ::arrow::json::ArrayFromJSONString(::arrow::int32(), "[0,1,10,11,20,21]")
                  .ValueOrDie();
  auto values =
      ::arrow::json::ArrayFromJSONString(::arrow::int64(), "[0,1,2,3,4,5]").ValueOrDie();
  auto file_schema = ::arrow::schema({::arrow::field("stored_key", ::arrow::int32()),
                                      ::arrow::field("stored_value", ::arrow::int64())});
  ASSERT_THAT(Write(::arrow::Table::Make(file_schema, {keys, values})), IsOk());
  ASSERT_EQ(metadata_->schema()->Column(0)->schema_node()->field_id(), -1);
  ASSERT_TRUE(metadata_->RowGroup(0)->ColumnChunk(0)->bloom_filter_offset().has_value());
  auto mapping =
      NameMapping::Make({MappedField{.names = {"stored_key"}, .field_id = 1},
                         MappedField{.names = {"stored_value"}, .field_id = 2}});
  auto options = Options(Expressions::Equal("key", Literal::Int(100)));
  options.name_mapping = std::move(mapping);
  // Name mapping resolves the projection. Footer columns without IDs remain
  // unavailable to both filters, even though a Bloom payload exists.
  Check(options, {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetBloomFilterTest, UnsupportedTransformRetainsGroups) {
  ASSERT_THAT(Write(), IsOk());
  Check(Options(Expressions::Equal<BoundTransform>(Expressions::Bucket("key", 16),
                                                   Literal::Int(15))),
        {0, 1, 2, 3, 4, 5});
}

TEST_F(ParquetBloomFilterTest, RequiredLeafWithNullableParent) {
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

TEST_F(ParquetBloomFilterTest, NestedPrimitiveStatistics) {
  SetKeyType(std::make_shared<StructType>(
      std::vector<SchemaField>{SchemaField::MakeOptional(3, "nested", int32())}));
  ASSERT_THAT(Write(true, "[[[0],0],[[1],1],[[10],2],[[11],3],[[20],4],[[21],5]]"),
              IsOk());
  CheckBloom(Expressions::Equal("key.nested", Literal::Int(100)), {false, false, false});
  CheckBloom(Expressions::Equal("key.nested", Literal::Int(10)), {false, true, false});
  Check(Options(Expressions::Equal("key.nested", Literal::Int(100))), {});
  Check(Options(Expressions::Equal("key.nested", Literal::Int(10))), {2, 3});
}

TEST_F(ParquetBloomFilterTest, UuidBoundsUseUnsignedByteOrder) {
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

  CheckBloom(Expressions::Equal("key", Literal::UUID(uuids[2])), {false, true});
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

class CountingBloomReader : public ::parquet::RowGroupBloomFilterReader {
 public:
  enum class Mode { kNormal, kMissing, kThrow, kSaturated };
  CountingBloomReader(std::shared_ptr<::parquet::RowGroupBloomFilterReader> delegate,
                      Mode mode)
      : delegate_(std::move(delegate)), mode_(mode) {}
  std::unique_ptr<::parquet::BloomFilter> GetColumnBloomFilter(int i) override {
    ++calls;
    switch (mode_) {
      case Mode::kMissing:
        return nullptr;
      case Mode::kThrow:
        throw ::parquet::ParquetException("test Bloom read failure");
      case Mode::kSaturated: {
        std::array<uint8_t, 32> bits;
        bits.fill(0xff);
        auto bloom = std::make_unique<::parquet::BlockSplitBloomFilter>();
        bloom->Init(bits.data(), static_cast<uint32_t>(bits.size()));
        return bloom;
      }
      default:
        return delegate_->GetColumnBloomFilter(i);
    }
  }
  int calls = 0;

 private:
  std::shared_ptr<::parquet::RowGroupBloomFilterReader> delegate_;
  Mode mode_;
};

TEST_F(ParquetBloomFilterTest, CacheReadErrorsAndFalsePositives) {
  ASSERT_THAT(Write(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
  auto reader = ::parquet::ParquetFileReader::Open(input);
  ::parquet::arrow::SchemaManifest manifest;
  ASSERT_TRUE(::parquet::arrow::SchemaManifest::Make(
                  metadata_->schema(), nullptr,
                  ::parquet::default_arrow_reader_properties(), &manifest)
                  .ok());
  auto expr =
      Expressions::And(Expressions::Equal("key", Literal::Int(100)),
                       Expressions::In("key", {Literal::Int(101), Literal::Int(102)}));
  ICEBERG_UNWRAP_OR_FAIL(auto filter, ParquetBloomRowGroupFilter::Make(
                                          Options(expr).filter, *metadata_->schema()));
  auto delegate = reader->GetBloomFilterReader().RowGroup(0);
  for (auto mode :
       {CountingBloomReader::Mode::kNormal, CountingBloomReader::Mode::kMissing,
        CountingBloomReader::Mode::kThrow, CountingBloomReader::Mode::kSaturated}) {
    CountingBloomReader tracked(delegate, mode);
    auto result = filter->ShouldRead(manifest, *metadata_->RowGroup(0), &tracked);
    if (mode == CountingBloomReader::Mode::kMissing ||
        mode == CountingBloomReader::Mode::kThrow) {
      EXPECT_FALSE(result.has_value());
    } else {
      ASSERT_THAT(result, IsOk());
      EXPECT_EQ(*result, mode == CountingBloomReader::Mode::kSaturated);
    }
    EXPECT_EQ(tracked.calls, 1);
  }
  // Unsupported predicates must not load any Bloom payload.
  for (const auto& unsupported : std::vector<std::shared_ptr<Expression>>{
           Expressions::LessThan("key", Literal::Int(100)),
           Expressions::NotEqual("key", Literal::Int(100)), Expressions::IsNull("key"),
           Expressions::NotNull("key"), Expressions::NotIn("key", {Literal::Int(100)})}) {
    ICEBERG_UNWRAP_OR_FAIL(auto unsupported_filter,
                           ParquetBloomRowGroupFilter::Make(Options(unsupported).filter,
                                                            *metadata_->schema()));
    CountingBloomReader tracked(delegate, CountingBloomReader::Mode::kThrow);
    ICEBERG_UNWRAP_OR_FAIL(auto result, unsupported_filter->ShouldRead(
                                            manifest, *metadata_->RowGroup(0), &tracked));
    EXPECT_TRUE(result);
    EXPECT_EQ(tracked.calls, 0);
  }
}

TEST_F(ParquetBloomFilterTest, CorruptBloomPayloadFailsOpen) {
  ASSERT_THAT(Write(), IsOk());
  auto column = metadata_->RowGroup(0)->ColumnChunk(0);
  ASSERT_TRUE(column->bloom_filter_offset().has_value());
  ASSERT_TRUE(column->bloom_filter_length().has_value());
  auto& data = std::static_pointer_cast<MockFileIO>(io_)->FileData(path_);
  std::ranges::fill(std::span(data).subspan(*column->bloom_filter_offset(),
                                            *column->bloom_filter_length()),
                    std::byte{0});
  EXPECT_THAT(
      ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                  Options(Expressions::Equal("key", Literal::Int(100)))),
      HasErrorMessage("Cannot read Parquet Bloom filter"));
}

TEST_F(ParquetBloomFilterTest, FooterPruningAndDisabledOptionAvoidBloomReads) {
  ASSERT_THAT(Write(true), IsOk());
  auto io = std::static_pointer_cast<BloomRecordingFileIO>(io_);
  io->reads.clear();
  ICEBERG_UNWRAP_OR_FAIL(
      auto unfiltered,
      ReaderFactoryRegistry::Open(FileFormatType::kParquet, Options(nullptr)));
  auto footer_reads = io->reads;
  ASSERT_THAT(unfiltered->Close(), IsOk());
  auto pruned = Options(Expressions::Equal("key", Literal::Int(100)));
  auto disabled = Options(Expressions::Equal("key", Literal::Int(10)));
  disabled.properties.Set(ReaderProperties::kParquetRowGroupFilter, false);
  for (const auto& options : {pruned, disabled}) {
    io->reads.clear();
    ICEBERG_UNWRAP_OR_FAIL(
        auto reader, ReaderFactoryRegistry::Open(FileFormatType::kParquet, options));
    EXPECT_EQ(io->reads, footer_reads);
    ASSERT_THAT(reader->Close(), IsOk());
  }
}

TEST_F(ParquetBloomFilterTest, OpenReadsOnlyBloomPayload) {
  row_group_size_ = 768 * 1024;
  ::parquet::WriterProperties::Builder properties;
  writer_properties_ = properties.disable_dictionary()
                           ->enable_bloom_filter("key", {.ndv = 1000, .fpp = 0.0001})
                           ->compression(::parquet::Compression::UNCOMPRESSED)
                           ->data_pagesize(8 * 1024 * 1024)
                           ->max_row_group_length(row_group_size_)
                           ->max_rows_per_page(row_group_size_)
                           ->build();
  ::arrow::Int32Builder keys;
  ::arrow::Int64Builder positions;
  ASSERT_TRUE(keys.Reserve(row_group_size_).ok());
  ASSERT_TRUE(positions.Reserve(row_group_size_).ok());
  for (int64_t i = 0; i < row_group_size_; ++i) {
    keys.UnsafeAppend(static_cast<int32_t>(2 * (i % 256)));
    positions.UnsafeAppend(i);
  }
  ArrowSchema c_schema;
  ASSERT_THAT(ToArrowSchema(*schema_, &c_schema), IsOk());
  auto arrow_schema = ::arrow::ImportSchema(&c_schema).ValueOrDie();
  ASSERT_THAT(Write(::arrow::Table::Make(arrow_schema, {keys.Finish().ValueOrDie(),
                                                        positions.Finish().ValueOrDie()}),
                    false),
              IsOk());
  ASSERT_EQ(metadata_->num_row_groups(), 1);
  auto column = metadata_->RowGroup(0)->ColumnChunk(0);
  ASSERT_GT(column->total_compressed_size(), 2 * 1024 * 1024);
  ASSERT_TRUE(column->bloom_filter_offset().has_value());
  ASSERT_TRUE(column->bloom_filter_length().has_value());
  auto io = std::static_pointer_cast<BloomRecordingFileIO>(io_);
  io->reads.clear();
  ICEBERG_UNWRAP_OR_FAIL(
      auto unfiltered,
      ReaderFactoryRegistry::Open(FileFormatType::kParquet, Options(nullptr)));
  auto footer_reads = io->reads;
  ASSERT_THAT(unfiltered->Close(), IsOk());
  for (int value : {1, 2}) {
    io->reads.clear();
    ICEBERG_UNWRAP_OR_FAIL(auto reader,
                           ReaderFactoryRegistry::Open(
                               FileFormatType::kParquet,
                               Options(Expressions::Equal("key", Literal::Int(value)))));
    auto expected = footer_reads;
    expected.push_back({.offset = *column->bloom_filter_offset(),
                        .length = *column->bloom_filter_length()});
    EXPECT_EQ(io->reads, expected);
    ASSERT_THAT(reader->Close(), IsOk());
  }
}

TEST_F(ParquetBloomFilterTest, ReaderReuseResetsBloomFilter) {
  ASSERT_THAT(Write(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(
      auto reader,
      ReaderFactoryRegistry::Open(FileFormatType::kParquet,
                                  Options(Expressions::Equal("key", Literal::Int(100)))));
  ICEBERG_UNWRAP_OR_FAIL(auto empty, reader->Next());
  EXPECT_FALSE(empty.has_value());
  ASSERT_THAT(reader->Close(), IsOk());
  ASSERT_THAT(reader->Open(Options(nullptr)), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto batch, reader->Next());
  ASSERT_TRUE(batch.has_value());
  EXPECT_GT(batch->length, 0);
  batch->release(&*batch);
  ASSERT_THAT(reader->Close(), IsOk());
}

TEST_F(ParquetBloomFilterTest, NegativeAndPromotedDecimalsUseFileWidth) {
  for (int precision : {9, 18, 30}) {
    SetKeyType(decimal(precision, 2));
    ASSERT_THAT(
        Write(
            false,
            R"([["0.00",0],["10.00",1],["-5.00",2],["15.00",3],["0.00",4],["10.00",5]])"),
        IsOk());
    SetKeyType(decimal(38, 2));
    CheckBloom(Expressions::Equal("key", Literal::Decimal(-500, 38, 2)),
               {false, true, false});
    CheckBloom(Expressions::Equal("key", Literal::Decimal(-700, 38, 2)),
               {false, false, false});
    CheckBloom(Expressions::Equal("key", Literal::Decimal(100000000000LL, 38, 2)),
               {false, false, false});
    Check(Options(Expressions::Equal("key", Literal::Decimal(-500, 38, 2))), {2, 3});
  }
}

TEST_F(ParquetBloomFilterTest, BinaryDecimalsWithDifferentSignExtensionWidths) {
  SetKeyType(decimal(18, 2));
  for (int width : {2, 4, 8}) {
    // Decimal precision 8 normally uses four bytes in Java. A variable-length
    // binary can also use minimal bytes or additional sign extension.
    auto key_node = ::parquet::schema::PrimitiveNode::Make(
        "key", ::parquet::Repetition::REQUIRED, ::parquet::LogicalType::Decimal(8, 2),
        ::parquet::Type::BYTE_ARRAY, -1, 1);
    auto value_node = ::parquet::schema::PrimitiveNode::Make(
        "value", ::parquet::Repetition::REQUIRED, ::parquet::Type::INT64,
        ::parquet::ConvertedType::NONE, -1, -1, -1, 2);
    auto file_schema = std::static_pointer_cast<::parquet::schema::GroupNode>(
        ::parquet::schema::GroupNode::Make("schema", ::parquet::Repetition::REQUIRED,
                                           {key_node, value_node}));
    ::parquet::WriterProperties::Builder properties;
    auto props = properties.disable_dictionary()
                     ->disable_statistics()
                     ->enable_bloom_filter("key", {.ndv = 1000, .fpp = 0.0001})
                     ->build();
    ICEBERG_UNWRAP_OR_FAIL(auto out, arrow::OpenArrowOutputStream(io_, path_));
    auto writer = ::parquet::ParquetFileWriter::Open(out, file_schema, props);
    auto group = writer->AppendRowGroup();
    auto positive = Decimal(500).ToBigEndian();
    auto negative = Decimal(-500).ToBigEndian();
    positive.insert(positive.begin(), width - positive.size(), 0x00);
    negative.insert(negative.begin(), width - negative.size(), 0xff);
    std::array<::parquet::ByteArray, 2> values{
        {{static_cast<uint32_t>(positive.size()), positive.data()},
         {static_cast<uint32_t>(negative.size()), negative.data()}}};
    auto keys = static_cast<::parquet::ByteArrayWriter*>(group->NextColumn());
    keys->WriteBatch(2, nullptr, nullptr, values.data());
    keys->Close();
    std::array<int64_t, 2> positions{0, 1};
    auto data = static_cast<::parquet::Int64Writer*>(group->NextColumn());
    data->WriteBatch(2, nullptr, nullptr, positions.data());
    data->Close();
    group->Close();
    writer->Close();
    ASSERT_TRUE(out->Close().ok());
    ICEBERG_UNWRAP_OR_FAIL(auto input, arrow::OpenArrowInputStream(io_, path_));
    metadata_ = ::parquet::ParquetFileReader::Open(input)->metadata();
    CheckBloom(Expressions::Equal("key", Literal::Decimal(500, 18, 2)), {true});
    CheckBloom(Expressions::Equal("key", Literal::Decimal(-500, 18, 2)), {true});
    CheckBloom(Expressions::Equal("key", Literal::Decimal(700, 18, 2)), {false});
    Check(Options(Expressions::Equal("key", Literal::Decimal(-500, 18, 2))), {0, 1});
  }
}

}  // namespace
}  // namespace iceberg::parquet
