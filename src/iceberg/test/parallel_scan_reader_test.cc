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

#include "iceberg/data/parallel_scan_reader.h"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/data/parallel_scan_reader_internal.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/file_format.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/schema_internal.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/type.h"

namespace iceberg {

namespace {

using internal::OpenParallelScan;
using internal::ParallelScanMetrics;
using internal::ParallelScanMetricsSnapshot;
using ::testing::ElementsAreArray;
using ::testing::HasSubstr;
using ::testing::UnorderedElementsAreArray;

constexpr int32_t kIdFieldId = 1;
constexpr int32_t kNameFieldId = 2;

// Counts the root arrays created by fake readers that have not been released yet.
std::atomic<int64_t> g_live_arrays{0};

struct TrackedArray {
  ArrowArray inner;
};

void ReleaseTracked(ArrowArray* array) {
  auto* tracked = static_cast<TrackedArray*>(array->private_data);
  tracked->inner.release(&tracked->inner);
  delete tracked;
  array->release = nullptr;
  g_live_arrays.fetch_sub(1);
}

// Moves `array` behind a release callback that updates g_live_arrays.
ArrowArray Track(ArrowArray array) {
  auto* tracked = new TrackedArray{array};
  ArrowArray outer = array;
  outer.private_data = tracked;
  outer.release = ReleaseTracked;
  g_live_arrays.fetch_add(1);
  return outer;
}

// Describes what a fake reader produces for one file path.
struct FakeFile {
  // Row ids of each batch.
  std::vector<std::vector<int32_t>> batches;
  // Length of the `name` value in every row.
  int32_t name_length = 1;
  // Index of the batch whose Next() returns an error, or -1.
  int fail_at = -1;
  // Index of the batch whose Next() throws, or -1.
  int throw_at = -1;
  // Report `id` as int64 to provoke a schema mismatch.
  bool wrong_schema = false;
};

class FakeFiles {
 public:
  static FakeFiles& Get() {
    static FakeFiles files;
    return files;
  }

  void Clear() {
    std::lock_guard lock(mutex_);
    files_.clear();
  }

  void Add(const std::string& path, FakeFile file) {
    std::lock_guard lock(mutex_);
    files_[path] = std::move(file);
  }

  std::optional<FakeFile> Find(const std::string& path) {
    std::lock_guard lock(mutex_);
    auto it = files_.find(path);
    if (it == files_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

 private:
  std::mutex mutex_;
  std::unordered_map<std::string, FakeFile> files_;
};

class FakeReader : public Reader {
 public:
  Status Open(const ReaderOptions& options) override {
    auto file = FakeFiles::Get().Find(options.path);
    if (!file.has_value()) {
      return NotFound("No fake file: {}", options.path);
    }
    file_ = std::move(*file);
    projection_ = options.projection;
    return {};
  }

  Status Close() override { return {}; }

  Result<std::optional<ArrowArray>> Next() override {
    const int index = next_batch_++;
    if (index == file_.fail_at) {
      return IOError("injected read failure");
    }
    if (index == file_.throw_at) {
      throw std::runtime_error("injected reader exception");
    }
    if (std::cmp_greater_equal(index, file_.batches.size())) {
      return std::nullopt;
    }
    ICEBERG_ASSIGN_OR_RAISE(auto array, BuildBatch(file_.batches[index]));
    return Track(array);
  }

  Result<ArrowSchema> Schema() override {
    ArrowSchema schema{};
    if (!file_.wrong_schema) {
      ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*projection_, &schema));
      return schema;
    }
    std::vector<SchemaField> fields;
    for (const auto& field : projection_->fields()) {
      if (field.field_id() == kIdFieldId) {
        fields.push_back(SchemaField::MakeRequired(kIdFieldId, "id", int64()));
      } else {
        fields.push_back(field);
      }
    }
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(iceberg::Schema(std::move(fields)), &schema));
    return schema;
  }

  Result<std::unordered_map<std::string, std::string>> Metadata() override {
    return std::unordered_map<std::string, std::string>{};
  }

 private:
  Result<ArrowArray> BuildBatch(const std::vector<int32_t>& ids) const {
    ArrowSchema schema{};
    ICEBERG_RETURN_UNEXPECTED(ToArrowSchema(*projection_, &schema));
    ArrowArray array{};
    const int init = ArrowArrayInitFromSchema(&array, &schema, nullptr);
    schema.release(&schema);
    if (init != NANOARROW_OK) {
      return UnknownError("ArrowArrayInitFromSchema failed");
    }
    const std::string name(static_cast<size_t>(file_.name_length), 'x');
    if (ArrowArrayStartAppending(&array) != NANOARROW_OK) {
      array.release(&array);
      return UnknownError("ArrowArrayStartAppending failed");
    }
    for (int32_t id : ids) {
      for (size_t index = 0; index < projection_->fields().size(); ++index) {
        ArrowArray* child = array.children[index];
        const int32_t field_id = projection_->fields()[index].field_id();
        int code = NANOARROW_OK;
        if (field_id == kIdFieldId) {
          code = ArrowArrayAppendInt(child, id);
        } else if (field_id == kNameFieldId) {
          code = ArrowArrayAppendString(child, ArrowCharView(name.c_str()));
        }
        if (code != NANOARROW_OK) {
          array.release(&array);
          return UnknownError("Appending to a fake batch failed");
        }
      }
      if (ArrowArrayFinishElement(&array) != NANOARROW_OK) {
        array.release(&array);
        return UnknownError("ArrowArrayFinishElement failed");
      }
    }
    if (ArrowArrayFinishBuildingDefault(&array, nullptr) != NANOARROW_OK) {
      array.release(&array);
      return UnknownError("ArrowArrayFinishBuildingDefault failed");
    }
    return array;
  }

  FakeFile file_;
  std::shared_ptr<iceberg::Schema> projection_;
  int next_batch_ = 0;
};

// Installs FakeReader for Parquet and Avro and restores the previous factories.
class ScopedFakeReaders {
 public:
  ScopedFakeReaders()
      : previous_parquet_(ReaderFactoryRegistry::GetFactory(FileFormatType::kParquet)),
        previous_avro_(ReaderFactoryRegistry::GetFactory(FileFormatType::kAvro)) {
    auto factory = []() -> Result<std::unique_ptr<Reader>> {
      return std::make_unique<FakeReader>();
    };
    ReaderFactoryRegistry::GetFactory(FileFormatType::kParquet) = factory;
    ReaderFactoryRegistry::GetFactory(FileFormatType::kAvro) = factory;
  }

  ~ScopedFakeReaders() {
    ReaderFactoryRegistry::GetFactory(FileFormatType::kParquet) =
        std::move(previous_parquet_);
    ReaderFactoryRegistry::GetFactory(FileFormatType::kAvro) = std::move(previous_avro_);
  }

  ScopedFakeReaders(const ScopedFakeReaders&) = delete;
  ScopedFakeReaders& operator=(const ScopedFakeReaders&) = delete;

 private:
  ReaderFactory previous_parquet_;
  ReaderFactory previous_avro_;
};

class FakeGroupStream : public FileScanTaskGroupStream {
 public:
  explicit FakeGroupStream(std::vector<std::shared_ptr<FileScanTaskGroup>> groups)
      : groups_(std::move(groups)) {}

  ~FakeGroupStream() override {
    if (on_destroy) {
      on_destroy();
    }
  }

  // Index of the Next() call that returns an error, or -1.
  int fail_at = -1;
  // Index of the Next() call that throws, or -1.
  int throw_at = -1;
  std::function<void()> on_destroy;

 protected:
  Result<std::optional<std::shared_ptr<FileScanTaskGroup>>> NextImpl() override {
    const int index = calls_++;
    if (index == fail_at) {
      return IOError("injected planning failure");
    }
    if (index == throw_at) {
      throw std::runtime_error("injected planning exception");
    }
    if (static_cast<size_t>(index) >= groups_.size()) {
      return std::nullopt;
    }
    return groups_[static_cast<size_t>(index)];
  }

 private:
  std::vector<std::shared_ptr<FileScanTaskGroup>> groups_;
  int calls_ = 0;
};

std::vector<int32_t> Range(int32_t begin, int32_t end) {
  std::vector<int32_t> values;
  for (int32_t value = begin; value < end; ++value) {
    values.push_back(value);
  }
  return values;
}

// Owns an ArrowArrayStream and releases it unless the test already did.
struct OwnedStream {
  ArrowArrayStream stream{};

  OwnedStream() = default;
  explicit OwnedStream(ArrowArrayStream value) : stream(value) {}
  OwnedStream(OwnedStream&& other) noexcept : stream(other.stream) {
    other.stream.release = nullptr;
  }
  OwnedStream(const OwnedStream&) = delete;
  OwnedStream& operator=(const OwnedStream&) = delete;
  OwnedStream& operator=(OwnedStream&&) = delete;
  ~OwnedStream() { Release(); }

  void Release() {
    if (stream.release != nullptr) {
      stream.release(&stream);
    }
  }
};

struct ReadResult {
  // Ids of each batch, in the order the stream returned them.
  std::vector<std::vector<int32_t>> batches;
  int error_code = 0;
  std::string error_message;

  std::vector<int32_t> AllIds() const {
    std::vector<int32_t> ids;
    for (const auto& batch : batches) {
      ids.insert(ids.end(), batch.begin(), batch.end());
    }
    return ids;
  }
};

// Reads the `id` column of a batch described by `schema`.
std::vector<int32_t> ReadIds(const ArrowSchema& schema, const ArrowArray& array) {
  std::vector<int32_t> ids;
  ArrowArrayView view;
  EXPECT_EQ(ArrowArrayViewInitFromSchema(&view, &schema, nullptr), NANOARROW_OK);
  EXPECT_EQ(ArrowArrayViewSetArray(&view, &array, nullptr), NANOARROW_OK);
  for (int64_t row = 0; row < array.length; ++row) {
    ids.push_back(
        static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(view.children[0], row)));
  }
  ArrowArrayViewReset(&view);
  return ids;
}

ReadResult ReadAll(ArrowArrayStream& stream, int64_t max_batches = -1) {
  ReadResult result;
  ArrowSchema schema{};
  EXPECT_EQ(stream.get_schema(&stream, &schema), 0);
  while (max_batches < 0 || std::cmp_less(result.batches.size(), max_batches)) {
    ArrowArray array{};
    int code = stream.get_next(&stream, &array);
    if (code != 0) {
      result.error_code = code;
      const char* message = stream.get_last_error(&stream);
      result.error_message = message == nullptr ? "" : message;
      break;
    }
    if (array.release == nullptr) {
      break;
    }
    if (schema.n_children > 0) {
      result.batches.push_back(ReadIds(schema, array));
    } else {
      result.batches.emplace_back(static_cast<size_t>(array.length), 0);
    }
    array.release(&array);
  }
  schema.release(&schema);
  return result;
}

// Polls `predicate` until it holds or a generous timeout expires.
bool WaitFor(const std::function<bool()>& predicate) {
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return predicate();
}

}  // namespace

class ParallelScanReaderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    FakeFiles::Get().Clear();
    g_live_arrays.store(0);
    table_schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(kIdFieldId, "id", int32()),
        SchemaField::MakeOptional(kNameFieldId, "name", string())});
  }

  void TearDown() override {
    EXPECT_EQ(g_live_arrays.load(), 0)
        << "Fake reader arrays leaked or were not released";
    FakeFiles::Get().Clear();
  }

  std::shared_ptr<FileScanTask> AddFile(const std::string& path, FakeFile file,
                                        FileFormatType format = FileFormatType::kParquet,
                                        std::shared_ptr<Expression> residual = nullptr) {
    FakeFiles::Get().Add(path, std::move(file));
    return std::make_shared<FileScanTask>(MakeDataFile(path, format),
                                          std::vector<std::shared_ptr<DataFile>>{},
                                          std::move(residual));
  }

  static std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                                FileFormatType format) {
    auto data_file = std::make_shared<DataFile>();
    data_file->file_path = path;
    data_file->file_format = format;
    data_file->file_size_in_bytes = 100;
    data_file->record_count = 10;
    return data_file;
  }

  static std::shared_ptr<FileScanTaskGroup> Group(
      std::vector<std::shared_ptr<FileScanTask>> tasks) {
    return std::make_shared<FileScanTaskGroup>(std::move(tasks));
  }

  ParallelScanReader::Options MakeOptions(int32_t workers, int64_t max_batches,
                                          int64_t max_bytes) const {
    ParallelScanReader::Options options;
    options.reader_options.io = std::make_shared<MockFileIO>();
    options.reader_options.table_schema = table_schema_;
    options.reader_options.projected_schema = table_schema_;
    options.worker_count = workers;
    options.max_queued_batches = max_batches;
    options.max_queued_bytes = max_bytes;
    return options;
  }

  Result<OwnedStream> Open(std::vector<std::shared_ptr<FileScanTaskGroup>> groups,
                           ParallelScanReader::Options options,
                           std::shared_ptr<ParallelScanMetrics> metrics = nullptr) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto stream,
        OpenParallelScan(std::make_unique<FakeGroupStream>(std::move(groups)),
                         std::move(options), std::move(metrics)));
    return OwnedStream(stream);
  }

  // Builds `file_count` single-task groups with `batches` batches of 4 rows each.
  std::vector<std::shared_ptr<FileScanTaskGroup>> ManyFiles(int file_count, int batches,
                                                            int32_t name_length = 1) {
    std::vector<std::shared_ptr<FileScanTaskGroup>> groups;
    int32_t next_id = 0;
    for (int file = 0; file < file_count; ++file) {
      FakeFile spec;
      spec.name_length = name_length;
      for (int batch = 0; batch < batches; ++batch) {
        spec.batches.push_back(Range(next_id, next_id + 4));
        next_id += 4;
      }
      groups.push_back(Group({AddFile("file-" + std::to_string(file), spec)}));
    }
    return groups;
  }

  ScopedFakeReaders fake_readers_;
  std::shared_ptr<Schema> table_schema_;
};

TEST_F(ParallelScanReaderTest, RejectsInvalidOptions) {
  auto make_groups = [] {
    return std::make_unique<FakeGroupStream>(
        std::vector<std::shared_ptr<FileScanTaskGroup>>{});
  };
  EXPECT_THAT(ParallelScanReader::Open(make_groups(), MakeOptions(0, 1, 1)),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ParallelScanReader::Open(make_groups(), MakeOptions(1, 0, 1)),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ParallelScanReader::Open(make_groups(), MakeOptions(1, 1, 0)),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ParallelScanReader::Open(nullptr, MakeOptions(1, 1, 1)),
              IsError(ErrorKind::kInvalidArgument));

  auto options = MakeOptions(1, 1, 1);
  options.reader_options.io = nullptr;
  EXPECT_THAT(ParallelScanReader::Open(make_groups(), options),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(ParallelScanReaderTest, ZeroGroupsEndsImmediately) {
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({}, MakeOptions(4, 4, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0);
  EXPECT_TRUE(result.batches.empty());

  // End of stream is sticky.
  ArrowArray array{};
  EXPECT_EQ(owned.stream.get_next(&owned.stream, &array), 0);
  EXPECT_EQ(array.release, nullptr);
}

TEST_F(ParallelScanReaderTest, SchemaMatchesProjection) {
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({}, MakeOptions(1, 1, 1)));
  ArrowSchema schema{};
  ASSERT_EQ(owned.stream.get_schema(&owned.stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 2);
  EXPECT_STREQ(schema.children[0]->name, "id");
  EXPECT_STREQ(schema.children[0]->format, "i");
  EXPECT_STREQ(schema.children[1]->name, "name");
  EXPECT_STREQ(schema.children[1]->format, "u");
  schema.release(&schema);
}

TEST_F(ParallelScanReaderTest, OneGroup) {
  FakeFile file;
  file.batches = {Range(0, 3), Range(3, 5)};
  ICEBERG_UNWRAP_OR_FAIL(
      auto owned, Open({Group({AddFile("one", file)})}, MakeOptions(2, 4, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_THAT(result.AllIds(), ElementsAreArray(Range(0, 5)));
}

TEST_F(ParallelScanReaderTest, OneAndManyWorkersReturnTheSameRows) {
  for (int32_t workers : {1, 2, 8}) {
    SCOPED_TRACE(workers);
    FakeFiles::Get().Clear();
    auto groups = ManyFiles(/*file_count=*/20, /*batches=*/3);
    ICEBERG_UNWRAP_OR_FAIL(auto owned,
                           Open(std::move(groups), MakeOptions(workers, 3, 1 << 20)));
    auto result = ReadAll(owned.stream);
    EXPECT_EQ(result.error_code, 0) << result.error_message;
    EXPECT_EQ(result.batches.size(), 60U);
    EXPECT_THAT(result.AllIds(), UnorderedElementsAreArray(Range(0, 240)));
  }
}

TEST_F(ParallelScanReaderTest, GroupTasksAndMixedFormatsAreRead) {
  FakeFile parquet;
  parquet.batches = {Range(0, 2)};
  FakeFile avro;
  avro.batches = {Range(2, 4)};
  FakeFile second_parquet;
  second_parquet.batches = {Range(4, 6)};
  auto group = Group({AddFile("a.parquet", parquet),
                      AddFile("b.avro", avro, FileFormatType::kAvro),
                      AddFile("c.parquet", second_parquet)});
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({group}, MakeOptions(3, 2, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  // One worker reads a group's tasks in order.
  EXPECT_THAT(result.AllIds(), ElementsAreArray(Range(0, 6)));
}

TEST_F(ParallelScanReaderTest, KeepsTaskLocalOrderAcrossInterleavedGroups) {
  FakeFile ordered;
  for (int32_t batch = 0; batch < 50; ++batch) {
    ordered.batches.push_back(Range(batch * 2, batch * 2 + 2));
  }
  std::vector<std::shared_ptr<FileScanTaskGroup>> groups = {
      Group({AddFile("ordered", ordered)})};
  for (int file = 0; file < 10; ++file) {
    FakeFile other;
    for (int32_t batch = 0; batch < 5; ++batch) {
      const int32_t begin = 1000 + file * 100 + batch * 2;
      other.batches.push_back(Range(begin, begin + 2));
    }
    groups.push_back(Group({AddFile("other-" + std::to_string(file), other)}));
  }
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open(std::move(groups), MakeOptions(4, 2, 1 << 20)));
  auto result = ReadAll(owned.stream);
  ASSERT_EQ(result.error_code, 0) << result.error_message;

  std::vector<int32_t> ordered_ids;
  std::map<int32_t, std::vector<int32_t>> other_ids;
  for (const auto& batch : result.batches) {
    for (int32_t id : batch) {
      if (id < 1000) {
        ordered_ids.push_back(id);
      } else {
        other_ids[(id - 1000) / 100].push_back(id);
      }
    }
  }
  EXPECT_THAT(ordered_ids, ElementsAreArray(Range(0, 100)));
  ASSERT_EQ(other_ids.size(), 10U);
  for (const auto& [file, ids] : other_ids) {
    EXPECT_TRUE(std::ranges::is_sorted(ids)) << "file " << file;
  }
}

TEST_F(ParallelScanReaderTest, EnforcesResiduals) {
  FakeFile file;
  file.batches = {Range(0, 4), Range(4, 8), Range(8, 10)};
  auto residual = Expressions::GreaterThanOrEqual("id", Literal::Int(5));
  auto task = AddFile("filtered", file, FileFormatType::kParquet, residual);
  // apply_residuals is forced on even when the caller leaves it off.
  auto options = MakeOptions(2, 2, 1 << 20);
  options.reader_options.apply_residuals = false;
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({Group({task})}, std::move(options)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_THAT(result.AllIds(), ElementsAreArray(Range(5, 10)));
}

TEST_F(ParallelScanReaderTest, SchemaMismatchFailsWithContext) {
  FakeFile file;
  file.batches = {Range(0, 2)};
  file.wrong_schema = true;
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({Group({AddFile("mismatch.parquet", file)})},
                                          MakeOptions(1, 2, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("does not match the projected schema"));
  EXPECT_THAT(result.error_message, HasSubstr("mismatch.parquet"));
  EXPECT_THAT(result.error_message, HasSubstr("root.id"));
}

TEST_F(ParallelScanReaderTest, ReadFailureNamesTheFileRange) {
  FakeFile file;
  file.batches = {Range(0, 2), Range(2, 4), Range(4, 6)};
  file.fail_at = 1;
  FakeFiles::Get().Add("broken.parquet", file);
  ICEBERG_UNWRAP_OR_FAIL(
      auto task, FileScanTask::MakeSplit(
                     MakeDataFile("broken.parquet", FileFormatType::kParquet), 10, 40));
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({Group({task})}, MakeOptions(1, 8, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("broken.parquet [10, 50)"));
  EXPECT_THAT(result.error_message, HasSubstr("injected read failure"));

  // The error is sticky.
  ArrowArray array{};
  EXPECT_EQ(owned.stream.get_next(&owned.stream, &array), EIO);
}

TEST_F(ParallelScanReaderTest, OpenFailureIsReported) {
  auto task = std::make_shared<FileScanTask>(
      MakeDataFile("missing.parquet", FileFormatType::kParquet));
  ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({Group({task})}, MakeOptions(2, 2, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message,
              HasSubstr("Failed to open data file missing.parquet"));
}

TEST_F(ParallelScanReaderTest, WorkerExceptionIsReported) {
  FakeFile file;
  file.batches = {Range(0, 2), Range(2, 4)};
  file.throw_at = 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto owned, Open({Group({AddFile("throws", file)})}, MakeOptions(2, 8, 1 << 20)));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("injected reader exception"));
}

TEST_F(ParallelScanReaderTest, PlanningFailureIsReported) {
  auto groups = ManyFiles(/*file_count=*/3, /*batches=*/2);
  auto stream = std::make_unique<FakeGroupStream>(std::move(groups));
  stream->fail_at = 1;
  ICEBERG_UNWRAP_OR_FAIL(
      auto raw, ParallelScanReader::Open(std::move(stream), MakeOptions(2, 8, 1 << 20)));
  OwnedStream owned(raw);
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("Failed to plan task groups"));
  EXPECT_THAT(result.error_message, HasSubstr("injected planning failure"));
}

TEST_F(ParallelScanReaderTest, PlanningExceptionIsReported) {
  auto stream = std::make_unique<FakeGroupStream>(ManyFiles(2, 1));
  stream->throw_at = 0;
  ICEBERG_UNWRAP_OR_FAIL(
      auto raw, ParallelScanReader::Open(std::move(stream), MakeOptions(3, 8, 1 << 20)));
  OwnedStream owned(raw);
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("injected planning exception"));
}

TEST_F(ParallelScanReaderTest, BytesBindBeforeCount) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  // Measure one batch so the byte budget holds exactly two.
  auto groups = ManyFiles(/*file_count=*/1, /*batches=*/10, /*name_length=*/1000);
  int64_t charge = 0;
  {
    auto probe = std::make_shared<ParallelScanMetrics>();
    FakeFile single;
    single.batches = {Range(0, 4)};
    single.name_length = 1000;
    ICEBERG_UNWRAP_OR_FAIL(auto owned, Open({Group({AddFile("probe", single)})},
                                            MakeOptions(1, 1, 1 << 30), probe));
    ASSERT_EQ(ReadAll(owned.stream).batches.size(), 1U);
    charge = probe->Snapshot().bytes_admitted;
  }
  ASSERT_GT(charge, 4000);

  const int64_t budget = 2 * charge;
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(std::move(groups), MakeOptions(1, 100, budget), metrics));
  // The worker queues two batches and then holds a third.
  ASSERT_TRUE(WaitFor([&] {
    auto snapshot = metrics->Snapshot();
    return snapshot.batches_admitted == 2 && snapshot.peak_held_bytes == charge;
  }));
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  auto blocked = metrics->Snapshot();
  EXPECT_EQ(blocked.batches_admitted, 2);
  EXPECT_EQ(blocked.peak_queued_batches, 2);
  EXPECT_EQ(blocked.peak_queued_bytes, budget);

  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_EQ(result.batches.size(), 10U);
  auto done = metrics->Snapshot();
  EXPECT_LE(done.peak_queued_bytes, budget);
  EXPECT_EQ(done.peak_queued_batches, 2);
  EXPECT_EQ(done.oversized_admissions, 0);
}

TEST_F(ParallelScanReaderTest, CountBindsBeforeBytes) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(ManyFiles(4, 5), MakeOptions(4, 3, 1 << 30), metrics));
  ASSERT_TRUE(WaitFor([&] { return metrics->Snapshot().batches_admitted == 3; }));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_EQ(result.batches.size(), 20U);
  EXPECT_EQ(metrics->Snapshot().peak_queued_batches, 3);
}

TEST_F(ParallelScanReaderTest, ZeroByteBatchesCountTowardTheBatchLimit) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  auto groups = ManyFiles(/*file_count=*/2, /*batches=*/4);
  auto options = MakeOptions(2, 2, 1);
  options.reader_options.projected_schema =
      std::make_shared<Schema>(std::vector<SchemaField>{});
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(std::move(groups), std::move(options), metrics));
  ASSERT_TRUE(WaitFor([&] { return metrics->Snapshot().batches_admitted == 2; }));
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_EQ(metrics->Snapshot().batches_admitted, 2);

  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  ASSERT_EQ(result.batches.size(), 8U);
  for (const auto& batch : result.batches) {
    EXPECT_EQ(batch.size(), 4U);
  }
  auto snapshot = metrics->Snapshot();
  EXPECT_EQ(snapshot.bytes_admitted, 0);
  EXPECT_EQ(snapshot.peak_queued_batches, 2);
}

TEST_F(ParallelScanReaderTest, OversizedBatchesAreQueuedAlone) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(ManyFiles(3, 4, 100), MakeOptions(3, 10, 1), metrics));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_EQ(result.batches.size(), 12U);
  auto snapshot = metrics->Snapshot();
  EXPECT_EQ(snapshot.oversized_admissions, 12);
  EXPECT_EQ(snapshot.peak_queued_batches, 1);
}

TEST_F(ParallelScanReaderTest, OversizedBatchIsNotStarvedBySmallBatches) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  FakeFile big;
  big.batches = {Range(0, 4)};
  big.name_length = 100000;
  std::vector<std::shared_ptr<FileScanTaskGroup>> groups = {Group({AddFile("big", big)})};
  for (int file = 0; file < 4; ++file) {
    FakeFile small;
    for (int32_t batch = 0; batch < 25; ++batch) {
      const int32_t begin = 1000 + file * 1000 + batch * 4;
      small.batches.push_back(Range(begin, begin + 4));
    }
    groups.push_back(Group({AddFile("small-" + std::to_string(file), small)}));
  }
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(std::move(groups), MakeOptions(5, 100, 8192), metrics));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_EQ(result.batches.size(), 101U);
  auto snapshot = metrics->Snapshot();
  EXPECT_EQ(snapshot.oversized_admissions, 1);
}

TEST_F(ParallelScanReaderTest, EarlyReleaseWhileProducersWait) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  std::atomic<bool> group_stream_destroyed{false};
  auto groups = std::make_unique<FakeGroupStream>(ManyFiles(4, 20));
  groups->on_destroy = [&] {
    // A metrics reporter may query diagnostics while the stream is destroyed.
    std::ignore = metrics->Snapshot();
    group_stream_destroyed = true;
  };
  ICEBERG_UNWRAP_OR_FAIL(
      auto raw, OpenParallelScan(std::move(groups), MakeOptions(3, 1, 1 << 20), metrics));
  OwnedStream owned(raw);

  ArrowSchema schema{};
  ASSERT_EQ(owned.stream.get_schema(&owned.stream, &schema), 0);
  ArrowArray kept{};
  ASSERT_EQ(owned.stream.get_next(&owned.stream, &kept), 0);
  ASSERT_NE(kept.release, nullptr);

  // Wait until the queue is full again and other workers hold completed batches.
  ASSERT_TRUE(WaitFor([&] {
    auto snapshot = metrics->Snapshot();
    return snapshot.batches_admitted >= 2 && snapshot.peak_held_bytes > 0;
  }));
  owned.Release();
  EXPECT_TRUE(group_stream_destroyed.load());

  // The returned array outlives the stream.
  EXPECT_EQ(g_live_arrays.load(), 1);
  auto ids = ReadIds(schema, kept);
  EXPECT_EQ(ids.size(), 4U);
  kept.release(&kept);
  schema.release(&schema);
  EXPECT_EQ(g_live_arrays.load(), 0);
}

TEST_F(ParallelScanReaderTest, ReleaseWithoutReading) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(ManyFiles(8, 10), MakeOptions(4, 2, 1 << 20), metrics));
  ASSERT_TRUE(WaitFor([&] { return metrics->Snapshot().batches_admitted == 2; }));
  owned.Release();
  EXPECT_EQ(g_live_arrays.load(), 0);
}

TEST_F(ParallelScanReaderTest, GroupStreamIsDestroyedAtEndOfPlanning) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  std::atomic<int64_t> groups_seen{-1};
  auto groups = std::make_unique<FakeGroupStream>(ManyFiles(5, 2));
  groups->on_destroy = [&] { groups_seen = metrics->Snapshot().groups_claimed; };
  ICEBERG_UNWRAP_OR_FAIL(
      auto raw, OpenParallelScan(std::move(groups), MakeOptions(2, 4, 1 << 20), metrics));
  OwnedStream owned(raw);
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, 0) << result.error_message;
  EXPECT_EQ(result.batches.size(), 10U);
  // Destroyed when planning reached its end, before the output stream is released.
  EXPECT_EQ(groups_seen.load(), 5);
  auto snapshot = metrics->Snapshot();
  EXPECT_EQ(snapshot.groups_claimed, 5);
  EXPECT_EQ(snapshot.tasks_opened, 5);
}

TEST_F(ParallelScanReaderTest, ErrorReleasesQueuedBatches) {
  auto metrics = std::make_shared<ParallelScanMetrics>();
  auto groups = ManyFiles(/*file_count=*/2, /*batches=*/10);
  FakeFile broken;
  broken.batches = {Range(5000, 5004), Range(5004, 5008)};
  broken.fail_at = 1;
  groups.push_back(Group({AddFile("broken", broken)}));
  ICEBERG_UNWRAP_OR_FAIL(auto owned,
                         Open(std::move(groups), MakeOptions(3, 4, 1 << 20), metrics));
  auto result = ReadAll(owned.stream);
  EXPECT_EQ(result.error_code, EIO);
  EXPECT_THAT(result.error_message, HasSubstr("broken"));
  owned.Release();
  EXPECT_EQ(g_live_arrays.load(), 0);
}

}  // namespace iceberg
