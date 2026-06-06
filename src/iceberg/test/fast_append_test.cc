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

#include "iceberg/update/fast_append.h"

#include <format>
#include <limits>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <variant>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/metrics/commit_report.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/metrics_reporters.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

class TestSnapshotUpdate : public SnapshotUpdate {
 public:
  explicit TestSnapshotUpdate(std::shared_ptr<TransactionContext> ctx)
      : SnapshotUpdate(std::move(ctx)) {}

  using SnapshotUpdate::ManifestPath;

  Status CleanUncommitted(const std::unordered_set<std::string>&) override { return {}; }
  std::string operation() override { return "test"; }
  Result<std::vector<ManifestFile>> Apply(const TableMetadata&,
                                          const std::shared_ptr<Snapshot>&) override {
    return std::vector<ManifestFile>{};
  }
  std::unordered_map<std::string, std::string> Summary() override { return {}; }
};

}  // namespace

class FastAppendTest : public UpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  std::string MetadataResource() const override {
    return "TableMetadataV2ValidMinimal.json";
  }

  void SetUp() override {
    UpdateTestBase::SetUp();

    // Get partition spec and schema from the base table
    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());

    // Create test data files
    file_a_ =
        CreateDataFile("/data/file_a.parquet", /*size=*/100, /*partition_value=*/1024);
    file_b_ =
        CreateDataFile("/data/file_b.parquet", /*size=*/200, /*partition_value=*/2048);
  }

  std::shared_ptr<DataFile> CreateDataFile(const std::string& path, int64_t record_count,
                                           int64_t size, int64_t partition_value = 0) {
    auto data_file = std::make_shared<DataFile>();
    data_file->content = DataFile::Content::kData;
    data_file->file_path = table_location_ + path;
    data_file->file_format = FileFormatType::kParquet;
    // The base table has partition spec with identity(x), so we need 1 partition value
    data_file->partition =
        PartitionValues(std::vector<Literal>{Literal::Long(partition_value)});
    data_file->file_size_in_bytes = size;
    data_file->record_count = record_count;
    data_file->partition_spec_id = spec_->spec_id();
    return data_file;
  }

  Result<ManifestFile> WriteManifest(
      const std::string& path, const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto writer, ManifestWriter::MakeWriter(table_->metadata()->format_version,
                                                kInvalidSnapshotId, path, file_io_, spec_,
                                                schema_, ManifestContent::kData));
    for (const auto& file : files) {
      ManifestEntry entry;
      entry.status = ManifestStatus::kAdded;
      entry.snapshot_id = std::nullopt;
      entry.data_file = file;
      ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
    }
    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    return writer->ToManifestFile();
  }

  void SetManifestTargetSizeBytes(int64_t size_bytes) {
    ICEBERG_UNWRAP_OR_FAIL(auto props, table_->NewUpdateProperties());
    props->Set(std::string(TableProperties::kManifestTargetSizeBytes.key()),
               std::to_string(size_bytes));
    EXPECT_THAT(props->Commit(), IsOk());
    EXPECT_THAT(table_->Refresh(), IsOk());
  }

  Result<std::vector<ManifestFile>> CurrentDataManifests() {
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot, table_->current_snapshot());
    SnapshotCache snapshot_cache(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_cache.DataManifests(file_io_));
    return std::vector<ManifestFile>(manifests.begin(), manifests.end());
  }

  Result<std::vector<ManifestEntry>> ReadEntries(const ManifestFile& manifest) {
    ICEBERG_ASSIGN_OR_RAISE(
        auto spec, table_->metadata()->PartitionSpecById(manifest.partition_spec_id));
    ICEBERG_ASSIGN_OR_RAISE(auto reader,
                            ManifestReader::Make(manifest, file_io_, schema_, spec));
    return reader->Entries();
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;
};

class SnapshotUpdateTest : public UpdateTestBase {};

TEST_F(FastAppendTest, AppendDataFile) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_a_);

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "1");
  EXPECT_EQ(snapshot->summary.at("added-records"), "100");
  EXPECT_EQ(snapshot->summary.at("added-files-size"), "1024");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsCreated), "1");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsKept), "0");
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kManifestsReplaced), "0");
}

TEST_F(FastAppendTest, AppendMultipleDataFiles) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_a_);
  fast_append->AppendFile(file_b_);

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "2");
  EXPECT_EQ(snapshot->summary.at("added-records"), "300");
  EXPECT_EQ(snapshot->summary.at("added-files-size"), "3072");
}

TEST_F(FastAppendTest, AppendManyFiles) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());

  int64_t total_records = 0;
  int64_t total_size = 0;
  constexpr int kFileCount = 10;
  for (int index = 0; index < kFileCount; ++index) {
    auto data_file = CreateDataFile(std::format("/data/file_{}.parquet", index),
                                    /*record_count=*/10 + index,
                                    /*size=*/100 + index * 10,
                                    /*partition_value=*/index % 2);
    total_records += data_file->record_count;
    total_size += data_file->file_size_in_bytes;
    fast_append->AppendFile(std::move(data_file));
  }

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), std::to_string(kFileCount));
  EXPECT_EQ(snapshot->summary.at("added-records"), std::to_string(total_records));
  EXPECT_EQ(snapshot->summary.at("added-files-size"), std::to_string(total_size));
}

TEST_F(FastAppendTest, WriteManifestGroups) {
  SetManifestTargetSizeBytes(std::numeric_limits<int64_t>::max());

  test::ThreadExecutor executor;
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->WriteManifestsWith(executor, 3);

  constexpr size_t kFileCount = 15'000;
  constexpr size_t kGroupSize = 7'500;
  std::vector<std::shared_ptr<DataFile>> files;
  files.reserve(kFileCount);
  for (size_t index = 0; index < kFileCount; ++index) {
    auto data_file =
        CreateDataFile(std::format("/data/group_{}.parquet", index),
                       /*record_count=*/1, /*size=*/1, static_cast<int64_t>(index % 2));
    fast_append->AppendFile(data_file);
    files.push_back(std::move(data_file));
  }

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  EXPECT_EQ(executor.submit_count(), 2);
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentDataManifests());
  ASSERT_EQ(manifests.size(), 2U);

  for (size_t group_index = 0; group_index < manifests.size(); ++group_index) {
    ASSERT_TRUE(manifests[group_index].added_files_count.has_value());
    EXPECT_EQ(manifests[group_index].added_files_count.value(), kGroupSize);

    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(manifests[group_index]));
    ASSERT_EQ(entries.size(), kGroupSize);
    const size_t offset = group_index * kGroupSize;
    for (size_t entry_index = 0; entry_index < entries.size(); ++entry_index) {
      ASSERT_NE(entries[entry_index].data_file, nullptr);
      EXPECT_EQ(entries[entry_index].data_file->file_path,
                files[offset + entry_index]->file_path);
    }
  }
}

TEST_F(FastAppendTest, InvalidManifestParallelism) {
  test::ThreadExecutor executor;
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->WriteManifestsWith(executor, 0);
  fast_append->AppendFile(file_a_);

  auto result = fast_append->Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(
      result,
      HasErrorMessage("Manifest write parallelism must be greater than 0, but was: 0"));
  EXPECT_EQ(executor.submit_count(), 0);
}

TEST_F(FastAppendTest, EmptyTableAppendUpdatesSequenceNumbers) {
  EXPECT_THAT(table_->current_snapshot(), HasErrorMessage("No current snapshot"));
  const int64_t base_sequence_number = table_->metadata()->last_sequence_number;

  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_a_);

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->sequence_number, base_sequence_number + 1);
  EXPECT_EQ(table_->metadata()->last_sequence_number, base_sequence_number + 1);
}

TEST_F(FastAppendTest, AppendNullFile) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(nullptr);

  auto result = fast_append->Commit();
  EXPECT_FALSE(result.has_value());
  EXPECT_THAT(result, HasErrorMessage("Invalid data file: null"));
  EXPECT_THAT(table_->current_snapshot(), HasErrorMessage("No current snapshot"));
}

TEST_F(FastAppendTest, FinalizeIgnoresCleanupDeleteFailure) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_a_);
  fast_append->DeleteWith([](const std::string&) { return IOError("delete failed"); });

  EXPECT_THAT(static_cast<SnapshotUpdate&>(*fast_append).Apply(), IsOk());
  EXPECT_THAT(fast_append->Finalize(Result<const TableMetadata*>(
                  std::unexpected(CommitFailed("commit failed").error()))),
              IsOk());
}

TEST_F(FastAppendTest, RetryCopiesAppendManifestAgain) {
  table_->metadata()->format_version = 1;
  const auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_}));

  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  std::vector<std::string> deleted_paths;
  fast_append->DeleteWith([&](const std::string& deleted_path) {
    deleted_paths.push_back(deleted_path);
    return file_io_->DeleteFile(deleted_path);
  });
  fast_append->AppendManifest(manifest);

  auto& update = static_cast<SnapshotUpdate&>(*fast_append);
  // First Apply() copies the input manifest because v1 cannot inherit snapshot IDs.
  ICEBERG_UNWRAP_OR_FAIL(auto first_apply, update.Apply());
  SnapshotCache first_cache(first_apply.snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto first_manifests, first_cache.Manifests(file_io_));
  ASSERT_EQ(first_manifests.size(), 1U);
  const auto first_rewritten_path = first_manifests[0].manifest_path;
  EXPECT_NE(first_rewritten_path, path);

  // Second Apply() simulates retry cleanup, then copies the original manifest again.
  ICEBERG_UNWRAP_OR_FAIL(auto second_apply, update.Apply());
  EXPECT_THAT(deleted_paths, testing::Contains(first_rewritten_path));

  SnapshotCache second_cache(second_apply.snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto second_manifests, second_cache.Manifests(file_io_));
  ASSERT_EQ(second_manifests.size(), 1U);
  EXPECT_NE(second_manifests[0].manifest_path, path);
  EXPECT_NE(second_manifests[0].manifest_path, first_rewritten_path);
}

TEST_F(FastAppendTest, AppendDuplicateFile) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(file_a_);
  fast_append->AppendFile(file_a_);  // Add same file twice

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-data-files"), "1");
  EXPECT_EQ(snapshot->summary.at("added-records"), "100");
}

TEST_F(FastAppendTest, SetSnapshotProperty) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->Set("custom-property", "custom-value");
  fast_append->AppendFile(file_a_);

  EXPECT_THAT(fast_append->Commit(), IsOk());

  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("custom-property"), "custom-value");
}

TEST_F(SnapshotUpdateTest, ConcurrentManifestPaths) {
  ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                         TransactionContext::Make(table_, TransactionKind::kUpdate));
  TestSnapshotUpdate update(std::move(ctx));

  constexpr int kThreadCount = 8;
  constexpr int kPathsPerThread = 32;
  std::vector<std::string> paths(kThreadCount * kPathsPerThread);
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);

  for (int thread_index = 0; thread_index < kThreadCount; ++thread_index) {
    threads.emplace_back([&, thread_index] {
      for (int path_index = 0; path_index < kPathsPerThread; ++path_index) {
        paths[thread_index * kPathsPerThread + path_index] = update.ManifestPath();
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  std::unordered_set<std::string> unique_paths(paths.begin(), paths.end());
  ASSERT_EQ(unique_paths.size(), paths.size());
  for (const auto& path : paths) {
    EXPECT_THAT(path, ::testing::HasSubstr("/metadata/"));
    EXPECT_THAT(path, ::testing::HasSubstr("-m"));
  }
}

// ---------------------------------------------------------------------------
// Metrics integration tests
// ---------------------------------------------------------------------------

namespace {

class CapturingReporter final : public MetricsReporter {
 public:
  Status Report(const MetricsReport& report) override {
    reports_.push_back(report);
    return {};
  }
  const std::vector<MetricsReport>& reports() const { return reports_; }
  void clear() { reports_.clear(); }

 private:
  std::vector<MetricsReport> reports_;
};

CapturingReporter* g_capturing_reporter = nullptr;

void RegisterCapturingReporter() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    (void)MetricsReporters::Register(
        "fast.append.test.reporter",
        [](const auto&) -> Result<std::unique_ptr<MetricsReporter>> {
          auto ptr = std::make_unique<CapturingReporter>();
          g_capturing_reporter = ptr.get();
          return ptr;
        });
  });
}

}  // namespace

// Test fixture that creates an InMemoryCatalog with a CapturingReporter so
// CommitReports emitted by Transaction::Commit() are observable.
class FastAppendMetricsTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    avro::RegisterAll();
    RegisterCapturingReporter();
  }

  void SetUp() override {
    table_ident_ = TableIdentifier{.name = "metrics_test_table"};
    table_location_ = "/warehouse/metrics_test_table";

    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    catalog_ = InMemoryCatalog::Make(
        "metrics_test_catalog", file_io_, "/warehouse/",
        {{std::string(kMetricsReporterImpl), "fast.append.test.reporter"}});

    auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
        static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
    ASSERT_TRUE(arrow_fs != nullptr);
    ASSERT_TRUE(arrow_fs->CreateDir(table_location_ + "/metadata").ok());

    auto metadata_location = std::format("{}/metadata/00001-{}.metadata.json",
                                         table_location_, Uuid::GenerateV7().ToString());
    ICEBERG_UNWRAP_OR_FAIL(
        auto metadata, ReadTableMetadataFromResource("TableMetadataV2ValidMinimal.json"));
    metadata->location = table_location_;
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());
    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t record_count,
                                         int64_t size, int64_t partition_value = 0) {
    auto data_file = std::make_shared<DataFile>();
    data_file->content = DataFile::Content::kData;
    data_file->file_path = table_location_ + path;
    data_file->file_format = FileFormatType::kParquet;
    data_file->partition =
        PartitionValues(std::vector<Literal>{Literal::Long(partition_value)});
    data_file->file_size_in_bytes = size;
    data_file->record_count = record_count;
    data_file->partition_spec_id = spec_->spec_id();
    return data_file;
  }

  TableIdentifier table_ident_;
  std::string table_location_;
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  std::shared_ptr<Table> table_;
  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
};

// A CommitReport must be emitted once for each FastAppend commit that creates a
// new snapshot.  Validate table_name, snapshot_id, operation, and attempt count.
TEST_F(FastAppendMetricsTest, CommitReportFiredAfterFastAppend) {
  ASSERT_NE(g_capturing_reporter, nullptr);

  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet", 100, 1024, 1024));
  ASSERT_THAT(fast_append->Commit(), IsOk());

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  const auto& reports = g_capturing_reporter->reports();
  ASSERT_EQ(reports.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(reports[0]));

  const auto& report = std::get<CommitReport>(reports[0]);
  EXPECT_EQ(report.table_name, table_->FullyQualifiedName());
  EXPECT_EQ(report.table_name, "metrics_test_catalog." + table_ident_.ToString());
  EXPECT_EQ(report.snapshot_id, snapshot->snapshot_id);
  EXPECT_EQ(report.operation, "append");
  ASSERT_TRUE(report.commit_metrics.attempts.has_value());
  EXPECT_EQ(report.commit_metrics.attempts->value, 1);
}

// A reporter set directly on the FastAppend via ReportWith() must take precedence
// over the table's configured reporter, mirroring Java's SnapshotProducer.reportWith().
TEST_F(FastAppendMetricsTest, ReportWithOverridesTableReporter) {
  ASSERT_NE(g_capturing_reporter, nullptr);

  auto override_reporter = std::make_shared<CapturingReporter>();

  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->ReportWith(override_reporter);
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet", 100, 1024, 1024));
  ASSERT_THAT(fast_append->Commit(), IsOk());

  // The override reporter receives the CommitReport.
  ASSERT_EQ(override_reporter->reports().size(), 1u);
  EXPECT_TRUE(std::holds_alternative<CommitReport>(override_reporter->reports()[0]));

  // The table's own reporter must not receive it.
  EXPECT_TRUE(g_capturing_reporter->reports().empty());
}

// A property-only commit must NOT emit a CommitReport because it does not
// create a new snapshot.  This covers the original bug where comparing a
// pre-commit snapshot ID of -1 against the existing snapshot ID would be
// skipped by the has_value() guard.
TEST_F(FastAppendMetricsTest, CommitReportNotFiredForPropertyOnlyCommit) {
  ASSERT_NE(g_capturing_reporter, nullptr);

  // First do a FastAppend to create a snapshot, then clear the recorder.
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet", 100, 1024, 1024));
  ASSERT_THAT(fast_append->Commit(), IsOk());
  ASSERT_EQ(g_capturing_reporter->reports().size(), 1u);
  g_capturing_reporter->clear();

  // Property-only commit on a table that already has a snapshot.
  ASSERT_THAT(table_->Refresh(), IsOk());
  std::shared_ptr<UpdateProperties> update_props;
  ICEBERG_UNWRAP_OR_FAIL(update_props, table_->NewUpdateProperties());
  update_props->Set("test-key", "test-value");
  ASSERT_THAT(update_props->Commit(), IsOk());

  // No new snapshot was created, so no CommitReport must be emitted.
  EXPECT_TRUE(g_capturing_reporter->reports().empty());
}

}  // namespace iceberg
