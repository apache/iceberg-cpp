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
#include <functional>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
#include <variant>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/exception.h"
#include "iceberg/expression/expressions.h"
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
#include "iceberg/table_update.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"
#include "iceberg/update/delete_files.h"
#include "iceberg/update/merge_append.h"
#include "iceberg/update/overwrite_files.h"
#include "iceberg/update/rewrite_files.h"
#include "iceberg/update/update_properties.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

namespace {

class TestSnapshotUpdate : public SnapshotUpdate {
 public:
  explicit TestSnapshotUpdate(std::shared_ptr<TransactionContext> ctx)
      : SnapshotUpdate(std::move(ctx)) {}

  using SnapshotUpdate::ManifestPath;
  using SnapshotUpdate::SnapshotId;

  std::function<Status(int)> apply_callback;
  std::function<Status()> finalize_callback;
  std::function<Status()> report_callback;
  int applies = 0;
  int finalizes = 0;
  int reports = 0;
  bool write_partial = false;
  std::vector<std::string> partial_paths;

 protected:
  Status CleanUncommitted(const std::unordered_set<std::string>&) override { return {}; }
  std::string operation() override { return "append"; }
  Result<std::vector<ManifestFile>> Apply(const TableMetadata&,
                                          const std::shared_ptr<Snapshot>&) override {
    ++applies;
    if (write_partial) {
      auto path = ManifestPath();
      partial_paths.push_back(path);
      ICEBERG_RETURN_UNEXPECTED(ctx_->table->io()->WriteFile(path, "partial manifest"));
    }
    if (apply_callback) {
      ICEBERG_RETURN_UNEXPECTED(apply_callback(applies));
    }
    return std::vector<ManifestFile>{};
  }
  Status Finalize(const TableMetadata& committed) override {
    ++finalizes;
    if (finalize_callback) {
      ICEBERG_RETURN_UNEXPECTED(finalize_callback());
    }
    return SnapshotUpdate::Finalize(committed);
  }
  Status ReportCommitted() override {
    ++reports;
    return report_callback ? report_callback() : Status{};
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

TEST_F(FastAppendTest, AbortIgnoresCleanupDeleteFailure) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, txn->NewFastAppend());
  fast_append->AppendFile(file_a_);
  int deletes = 0;
  fast_append->DeleteWith([&](const std::string&) {
    ++deletes;
    return IOError("delete failed");
  });
  EXPECT_THAT(fast_append->Commit(), IsOk());
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_EQ(deletes, 2);
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_EQ(deletes, 2);
}

TEST_F(FastAppendTest, TransactionApplyFailureCleansUpStagedFiles) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, txn->NewFastAppend());
  std::vector<std::string> deleted_paths;
  fast_append->DeleteWith([&](const std::string& path) {
    deleted_paths.push_back(path);
    return file_io_->DeleteFile(path);
  });
  fast_append->AppendFile(file_a_);
  EXPECT_THAT(fast_append->Commit(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto failed_append, txn->NewFastAppend());
  failed_append->AppendFile(nullptr);
  EXPECT_THAT(failed_append->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(deleted_paths, ::testing::SizeIs(2U));
  EXPECT_EQ(txn->state(), TransactionState::kFailed);
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_THAT(deleted_paths, ::testing::SizeIs(2U));
}

TEST_F(FastAppendTest, RetryCopiesAppendManifestAgain) {
  table_->metadata()->format_version = 1;
  ASSERT_THAT(
      TableMetadataUtil::Write(*file_io_, std::string(table_->metadata_file_location()),
                               *table_->metadata()),
      IsOk());
  const auto path = table_location_ + "/metadata/input.avro";
  ICEBERG_UNWRAP_OR_FAIL(auto manifest, WriteManifest(path, {file_a_}));
  FailCommits(2);
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, table_->NewFastAppend());
  std::vector<std::string> deleted_paths;
  fast_append->DeleteWith([&](const std::string& deleted_path) {
    deleted_paths.push_back(deleted_path);
    if (deleted_paths.size() == 1) {
      throw std::runtime_error("cleanup callback threw");
    }
    if (deleted_paths.size() == 2) {
      return Status(IOError("cleanup returned an error"));
    }
    return file_io_->DeleteFile(deleted_path);
  });
  fast_append->AppendManifest(manifest);
  EXPECT_THAT(fast_append->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentDataManifests());
  ASSERT_EQ(manifests.size(), 1U);
  EXPECT_NE(manifests[0].manifest_path, path);
  EXPECT_THAT(deleted_paths, ::testing::SizeIs(4U));
  EXPECT_THAT(deleted_paths, ::testing::Not(::testing::Contains(path)));
  EXPECT_THAT(deleted_paths,
              ::testing::Not(::testing::Contains(manifests[0].manifest_path)));
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

void RegisterCapturingReporter() {
  static std::once_flag flag;
  std::call_once(flag, [] {
    (void)MetricsReporters::Register(
        "fast.append.test.reporter",
        [](const auto&) -> Result<std::unique_ptr<MetricsReporter>> {
          return std::make_unique<CapturingReporter>();
        });
  });
}

}  // namespace

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
    ICEBERG_UNWRAP_OR_FAIL(
        catalog_, InMemoryCatalog::Make("metrics_test_catalog", file_io_, "/warehouse/",
                                        {{std::string(kMetricsReporterImpl),
                                          "fast.append.test.reporter"}}));

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

    reporter_ = std::dynamic_pointer_cast<CapturingReporter>(table_->reporter());
    ASSERT_NE(reporter_, nullptr);

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         int64_t partition_value = 1024) {
    auto data_file = std::make_shared<DataFile>();
    data_file->content = DataFile::Content::kData;
    data_file->file_path = table_location_ + path;
    data_file->file_format = FileFormatType::kParquet;
    data_file->partition =
        PartitionValues(std::vector<Literal>{Literal::Long(partition_value)});
    data_file->file_size_in_bytes = 1024;
    data_file->record_count = 100;
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
  std::shared_ptr<CapturingReporter> reporter_;
};

TEST_F(FastAppendMetricsTest, CommitReportFiredAfterFastAppend) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(fast_append->Commit(), IsOk());

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());

  const auto& reports = reporter_->reports();
  ASSERT_EQ(reports.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(reports[0]));

  const auto& report = std::get<CommitReport>(reports[0]);
  EXPECT_EQ(report.table_name, table_->full_name());
  EXPECT_EQ(report.table_name, "metrics_test_catalog." + table_ident_.ToString());
  EXPECT_EQ(report.snapshot_id, snapshot->snapshot_id);
  EXPECT_EQ(report.sequence_number, snapshot->sequence_number);
  EXPECT_EQ(report.operation, "append");
  const auto& metrics = report.commit_metrics;
  ASSERT_TRUE(metrics.attempts.has_value());
  EXPECT_EQ(metrics.attempts->value, 1);
  ASSERT_TRUE(metrics.added_data_files.has_value());
  EXPECT_EQ(metrics.added_data_files->value, 1);
  ASSERT_TRUE(metrics.total_data_files.has_value());
  EXPECT_EQ(metrics.total_data_files->value, 1);
  ASSERT_TRUE(metrics.added_records.has_value());
  EXPECT_EQ(metrics.added_records->value, 100);
  ASSERT_TRUE(metrics.total_records.has_value());
  EXPECT_EQ(metrics.total_records->value, 100);
  ASSERT_TRUE(metrics.added_files_size_bytes.has_value());
  EXPECT_EQ(metrics.added_files_size_bytes->value, 1024);
  ASSERT_TRUE(metrics.total_files_size_bytes.has_value());
  EXPECT_EQ(metrics.total_files_size_bytes->value, 1024);
  ASSERT_TRUE(metrics.created_manifest_count.has_value());
  EXPECT_EQ(metrics.created_manifest_count->value, 1);
}

TEST_F(FastAppendMetricsTest, ReportWithOverridesTableReporter) {
  auto override_reporter = std::make_shared<CapturingReporter>();

  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->ReportWith(override_reporter);
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(fast_append->Commit(), IsOk());

  ASSERT_EQ(override_reporter->reports().size(), 1u);
  EXPECT_TRUE(std::holds_alternative<CommitReport>(override_reporter->reports()[0]));
  EXPECT_TRUE(reporter_->reports().empty());
}

TEST_F(FastAppendMetricsTest, CapturesTableReporterWhenUpdateIsCreated) {
  auto replacement_reporter = std::make_shared<CapturingReporter>();
  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([this, &mock_catalog, &replacement_reporter](
                         const TableIdentifier&,
                         const std::vector<std::unique_ptr<TableRequirement>>&,
                         const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return Table::Make(table_->name(), table_->metadata(),
                           std::string(table_->metadata_file_location()), table_->io(),
                           mock_catalog, table_->full_name(), replacement_reporter);
      });

  ICEBERG_UNWRAP_OR_FAIL(
      auto mock_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), table_->io(),
                  mock_catalog, table_->full_name(), reporter_));
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, mock_table->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));

  ASSERT_THAT(fast_append->Commit(), IsOk());
  ASSERT_EQ(reporter_->reports().size(), 1u);
  EXPECT_TRUE(replacement_reporter->reports().empty());
}

// An existing snapshot must not be reused as the report for a non-snapshot update.
TEST_F(FastAppendMetricsTest, PropertyOnlyCommitOnTableWithSnapshotDoesNotReport) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(fast_append->Commit(), IsOk());
  ASSERT_EQ(reporter_->reports().size(), 1u);
  reporter_->clear();

  ASSERT_THAT(table_->Refresh(), IsOk());
  std::shared_ptr<UpdateProperties> update_props;
  ICEBERG_UNWRAP_OR_FAIL(update_props, table_->NewUpdateProperties());
  update_props->Set("test-key", "test-value");
  ASSERT_THAT(update_props->Commit(), IsOk());

  EXPECT_TRUE(reporter_->reports().empty());
}

// StageOnly() adds a snapshot to table metadata without making it current. The
// CommitReport must still be fired for the staged snapshot itself.
TEST_F(FastAppendMetricsTest, CommitReportFiredForStageOnlyCommit) {
  std::shared_ptr<FastAppend> fast_append;
  ICEBERG_UNWRAP_OR_FAIL(fast_append, table_->NewFastAppend());
  fast_append->StageOnly();
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(fast_append->Commit(), IsOk());

  ASSERT_THAT(table_->Refresh(), IsOk());

  // The staged snapshot never became current.
  EXPECT_EQ(table_->metadata()->current_snapshot_id, kInvalidSnapshotId);

  const auto& reports = reporter_->reports();
  ASSERT_EQ(reports.size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(reports[0]));

  const auto& report = std::get<CommitReport>(reports[0]);
  EXPECT_NE(report.snapshot_id, kInvalidSnapshotId);
  EXPECT_TRUE(table_->metadata()->SnapshotById(report.snapshot_id).has_value());
}

TEST_F(FastAppendMetricsTest, ReporterOverrideAppliesOnlyToItsOwnUpdate) {
  auto override_reporter = std::make_shared<CapturingReporter>();

  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());

  ICEBERG_UNWRAP_OR_FAIL(auto first_append, txn->NewFastAppend());
  first_append->ReportWith(override_reporter);
  first_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(first_append->Commit(), IsOk());
  EXPECT_TRUE(override_reporter->reports().empty());
  EXPECT_TRUE(reporter_->reports().empty());

  ICEBERG_UNWRAP_OR_FAIL(auto second_append, txn->NewFastAppend());
  second_append->AppendFile(MakeDataFile("/data/file_b.parquet", 2048));
  ASSERT_THAT(second_append->Commit(), IsOk());
  EXPECT_TRUE(override_reporter->reports().empty());
  EXPECT_TRUE(reporter_->reports().empty());

  ASSERT_THAT(txn->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ASSERT_EQ(override_reporter->reports().size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(override_reporter->reports()[0]));
  ASSERT_EQ(reporter_->reports().size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(reporter_->reports()[0]));

  const auto& first_report = std::get<CommitReport>(override_reporter->reports()[0]);
  const auto& second_report = std::get<CommitReport>(reporter_->reports()[0]);

  ICEBERG_UNWRAP_OR_FAIL(auto current_snapshot, table_->current_snapshot());
  EXPECT_EQ(second_report.snapshot_id, current_snapshot->snapshot_id);
  ASSERT_TRUE(current_snapshot->parent_snapshot_id.has_value());
  EXPECT_EQ(first_report.snapshot_id, current_snapshot->parent_snapshot_id.value());

  EXPECT_EQ(second_report.table_name, table_->full_name());

  ASSERT_TRUE(first_report.commit_metrics.attempts.has_value());
  EXPECT_EQ(first_report.commit_metrics.attempts->value, 1);
  ASSERT_TRUE(second_report.commit_metrics.attempts.has_value());
  EXPECT_EQ(second_report.commit_metrics.attempts->value, 1);
}

TEST_F(FastAppendMetricsTest, TransactionRetryReportsOnceAfterSuccess) {
  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  const std::string refreshed_metadata_location =
      table_location_ + "/metadata/refreshed.metadata.json";

  ON_CALL(*mock_catalog, LoadTable(::testing::_))
      .WillByDefault([this, &mock_catalog, &refreshed_metadata_location](
                         const TableIdentifier&) -> Result<std::shared_ptr<Table>> {
        ICEBERG_ASSIGN_OR_RAISE(
            auto metadata,
            TableMetadataUtil::Read(*table_->io(),
                                    std::string(table_->metadata_file_location())));
        return Table::Make(table_->name(), std::move(metadata),
                           refreshed_metadata_location, table_->io(), mock_catalog,
                           table_->full_name(), reporter_);
      });

  int update_call_count = 0;
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([this, &mock_catalog, &update_call_count](
                         const TableIdentifier&,
                         const std::vector<std::unique_ptr<TableRequirement>>&,
                         const std::vector<std::unique_ptr<TableUpdate>>& updates)
                         -> Result<std::shared_ptr<Table>> {
        ++update_call_count;
        EXPECT_TRUE(reporter_->reports().empty());
        if (update_call_count == 1) {
          return CommitFailed("conflict on first attempt");
        }

        EXPECT_FALSE(updates.empty());
        return Table::Make(table_->name(), table_->metadata(),
                           std::string(table_->metadata_file_location()), table_->io(),
                           mock_catalog, table_->full_name(), reporter_);
      });

  ICEBERG_UNWRAP_OR_FAIL(
      auto mock_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), table_->io(),
                  mock_catalog, table_->full_name(), reporter_));
  ICEBERG_UNWRAP_OR_FAIL(auto txn, mock_table->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, txn->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));
  ASSERT_THAT(fast_append->Commit(), IsOk());
  ASSERT_TRUE(reporter_->reports().empty());

  ASSERT_THAT(txn->Commit(), IsOk());
  EXPECT_EQ(update_call_count, 2);
  ASSERT_EQ(reporter_->reports().size(), 1u);
  ASSERT_TRUE(std::holds_alternative<CommitReport>(reporter_->reports()[0]));
  const auto& report = std::get<CommitReport>(reporter_->reports()[0]);
  ASSERT_TRUE(report.commit_metrics.attempts.has_value());
  EXPECT_EQ(report.commit_metrics.attempts->value, 2);
}

TEST_F(FastAppendMetricsTest, CommitStateUnknownDoesNotReport) {
  auto mock_catalog = std::make_shared<::testing::NiceMock<MockCatalog>>();
  ON_CALL(*mock_catalog, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillByDefault([](const TableIdentifier&,
                        const std::vector<std::unique_ptr<TableRequirement>>&,
                        const std::vector<std::unique_ptr<TableUpdate>>&)
                         -> Result<std::shared_ptr<Table>> {
        return CommitStateUnknown("unknown commit state");
      });

  ICEBERG_UNWRAP_OR_FAIL(
      auto mock_table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), table_->io(),
                  mock_catalog, table_->full_name(), reporter_));
  ICEBERG_UNWRAP_OR_FAIL(auto fast_append, mock_table->NewFastAppend());
  fast_append->AppendFile(MakeDataFile("/data/file_a.parquet"));

  EXPECT_THAT(fast_append->Commit(), IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_TRUE(reporter_->reports().empty());
}

TEST_F(FastAppendTest, ReplayApplyFailureStopsRetryAndCleansPartialFiles) {
  for (auto replay_kind :
       {ErrorKind::kRetryableValidationFailed, ErrorKind::kCommitStateUnknown}) {
    SCOPED_TRACE(static_cast<int>(replay_kind));
    auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
    EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .Times(1)
        .WillOnce(::testing::Return(CommitFailed("first conflict")));
    EXPECT_CALL(*mock, LoadTable(::testing::_)).Times(1).WillOnce([&](const auto& name) {
      return catalog_->LoadTable(name);
    });
    ICEBERG_UNWRAP_OR_FAIL(
        auto table,
        Table::Make(table_->name(), table_->metadata(),
                    std::string(table_->metadata_file_location()), file_io_, mock));
    ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                           TransactionContext::Make(table, TransactionKind::kUpdate));
    auto update = std::make_shared<TestSnapshotUpdate>(ctx);
    update->write_partial = true;
    update->apply_callback = [replay_kind](int attempt) -> Status {
      if (attempt == 2) {
        return std::unexpected(
            Error{.kind = replay_kind, .message = "replay validation failed"});
      }
      return {};
    };
    std::vector<std::string> deleted;
    update->DeleteWith([&](const std::string& path) {
      deleted.push_back(path);
      EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
      EXPECT_THROW(update->Set("reenter", "value"), IcebergError);
      return file_io_->DeleteFile(path);
    });
    EXPECT_THAT(update->Commit(),
                ::testing::AllOf(IsError(replay_kind),
                                 HasErrorMessage("replay validation failed")));
    EXPECT_EQ(update->applies, 2);
    EXPECT_EQ(update->finalizes, 0);
    EXPECT_EQ(update->reports, 0);
    EXPECT_THAT(deleted, ::testing::SizeIs(3U));
    for (const auto& path : update->partial_paths) {
      EXPECT_THAT(deleted, ::testing::Contains(path));
      EXPECT_FALSE(file_io_->ReadFile(path, std::nullopt).has_value());
    }
    EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_FALSE(ctx->transaction.has_value());
    EXPECT_THAT(deleted, ::testing::SizeIs(3U));
  }
}

TEST_F(FastAppendTest, StandaloneHooksAreTerminalAndIsolated) {
  ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                         TransactionContext::Make(table_, TransactionKind::kUpdate));
  auto update = std::make_shared<TestSnapshotUpdate>(ctx);
  update->finalize_callback = [&]() -> Status {
    EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THROW(update->DeleteWith({}), IcebergError);
    throw std::runtime_error("finalize failure");
  };
  update->report_callback = [&]() -> Status {
    EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
    throw std::runtime_error("report failure");
  };
  ASSERT_THAT(update->Commit(), IsOk());
  EXPECT_EQ(update->finalizes, 1);
  EXPECT_EQ(update->reports, 1);
  EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(update->finalizes, 1);
  EXPECT_EQ(update->reports, 1);
}

class CallbackReporter : public MetricsReporter {
 public:
  explicit CallbackReporter(std::function<Status()> callback)
      : callback_(std::move(callback)) {}
  Status Report(const MetricsReport&) override { return callback_(); }

 private:
  std::function<Status()> callback_;
};

TEST_F(FastAppendTest, ExplicitSuccessPublishesAllTerminalMarkersBeforeReporting) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto first, txn->NewFastAppend());
  std::shared_ptr<FastAppend> second;
  int reports = 0;
  first->AppendFile(file_a_).ReportWith(
      std::make_shared<CallbackReporter>([&]() -> Status {
        ++reports;
        EXPECT_EQ(txn->state(), TransactionState::kCommitted);
        EXPECT_THAT(first->Commit(), IsError(ErrorKind::kValidationFailed));
        EXPECT_THAT(second->Commit(), IsError(ErrorKind::kValidationFailed));
        EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
        EXPECT_THAT(txn->NewFastAppend(), IsError(ErrorKind::kValidationFailed));
        EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
        throw std::runtime_error("report failure");
      }));
  ASSERT_THAT(first->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(second, txn->NewFastAppend());
  second->AppendFile(file_b_).ReportWith(
      std::make_shared<CallbackReporter>([&]() -> Status {
        ++reports;
        return IOError("another report failure");
      }));
  ASSERT_THAT(second->Commit(), IsOk());
  ASSERT_THAT(txn->Commit(), IsOk());
  EXPECT_EQ(reports, 2);
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(reports, 2);
}

TEST_F(FastAppendTest, FrozenFileAliasesCannotChangeReplay) {
  FailCommits(2);
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewFastAppend());
  append->AppendFile(file_a_);
  const auto original_path = file_a_->file_path;
  const auto original_records = file_a_->record_count;
  ASSERT_THAT(append->Commit(), IsOk());
  EXPECT_THROW(append->AppendFile(file_b_), IcebergError);
  EXPECT_THROW(append->DeleteWith({}), IcebergError);
  file_a_->file_path = "/changed.parquet";
  file_a_->record_count = 123456;
  file_a_->partition = PartitionValues({Literal::Long(42)});
  ASSERT_THAT(txn->Commit(), IsOk());
  EXPECT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, CurrentDataManifests());
  ASSERT_EQ(manifests.size(), 1U);
  ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadEntries(manifests[0]));
  ASSERT_EQ(entries.size(), 1U);
  EXPECT_EQ(entries[0].data_file->file_path, original_path);
  EXPECT_EQ(entries[0].data_file->record_count, original_records);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("added-records"), std::to_string(original_records));
}

TEST_F(FastAppendTest, ConflictThenUnknownPreservesLastAttempt) {
  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .WillOnce(::testing::Return(CommitFailed("conflict")))
      .WillOnce(::testing::Return(CommitStateUnknown("unknown")));
  EXPECT_CALL(*mock, LoadTable(::testing::_)).WillOnce([&](const auto& name) {
    return catalog_->LoadTable(name);
  });
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), file_io_, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto append, txn->NewFastAppend());
  int deletes = 0;
  append->AppendFile(file_a_).DeleteWith([&](const std::string& path) {
    ++deletes;
    EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(txn->NewFastAppend(), IsError(ErrorKind::kValidationFailed));
    return file_io_->DeleteFile(path);
  });
  ASSERT_THAT(append->Commit(), IsOk());
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kCommitStateUnknown));
  EXPECT_EQ(txn->state(), TransactionState::kCommitStateUnknown);
  EXPECT_EQ(deletes, 2);
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, txn->current().Snapshot());
  EXPECT_THAT(file_io_->ReadFile(snapshot->manifest_list, std::nullopt), IsOk());
  SnapshotCache cache(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, cache.Manifests(file_io_));
  ASSERT_EQ(manifests.size(), 1U);
  EXPECT_THAT(file_io_->ReadFile(manifests[0].manifest_path, std::nullopt), IsOk());
  EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(append->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_EQ(deletes, 2);
}

TEST_F(FastAppendTest, ApplyFailureAfterWritingCleansEveryUpdate) {
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  int deletes = 0;
  auto delete_file = [&](const std::string& path) -> Status {
    ++deletes;
    EXPECT_EQ(txn->state(), TransactionState::kFailed);
    EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
    if (deletes == 1) {
      throw std::runtime_error("delete callback threw");
    }
    return IOError("delete failure");
  };
  ICEBERG_UNWRAP_OR_FAIL(auto first, txn->NewFastAppend());
  first->AppendFile(file_a_).DeleteWith(delete_file);
  ASSERT_THAT(first->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto failed, txn->NewRewriteFiles());
  failed->DeleteDataFile(file_a_).AddDataFile(file_b_).DeleteWith(delete_file);
  EXPECT_THAT(failed->Commit(), HasErrorMessage("Invalid REPLACE operation"));
  EXPECT_EQ(txn->state(), TransactionState::kFailed);
  EXPECT_GE(deletes, 4);
  const int cleanup_attempts = deletes;
  EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_EQ(deletes, cleanup_attempts);
}

TEST_F(FastAppendTest, ReplayBecomingNoopCleansStagingWithoutReporting) {
  for (bool other_update : {false, true}) {
    SCOPED_TRACE(other_update);
    auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
    std::shared_ptr<Table> refreshed;
    std::shared_ptr<TableMetadata> refreshed_metadata;
    int attempts = 0;
    EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .Times(other_update ? 2 : 1)
        .WillRepeatedly([&](const auto&, const auto&,
                            const auto& changes) -> Result<std::shared_ptr<Table>> {
          const bool first_attempt = ++attempts == 1;
          auto builder = TableMetadataBuilder::BuildFrom(
              first_attempt ? table_->metadata().get() : refreshed_metadata.get());
          for (const auto& change : changes) {
            if (first_attempt && change->kind() == TableUpdate::Kind::kSetProperties) {
              continue;
            }
            if (!first_attempt) {
              EXPECT_EQ(change->kind(), TableUpdate::Kind::kSetProperties);
            }
            change->ApplyTo(*builder);
          }
          ICEBERG_ASSIGN_OR_RAISE(std::shared_ptr<TableMetadata> metadata,
                                  builder->Build());
          if (first_attempt) {
            // Use independent files for the refreshed snapshot; generated paths are
            // owned by the update that created them.
            metadata->snapshots.back() =
                std::make_shared<Snapshot>(*metadata->snapshots.back());
            auto snapshot = metadata->snapshots.back();
            snapshot->manifest_list = table_location_ + "/metadata/concurrent-list.avro";
            ICEBERG_ASSIGN_OR_RAISE(
                auto writer, ManifestListWriter::MakeWriter(
                                 metadata->format_version, snapshot->snapshot_id,
                                 snapshot->parent_snapshot_id, snapshot->manifest_list,
                                 file_io_, snapshot->sequence_number));
            ICEBERG_RETURN_UNEXPECTED(writer->Close());
          }
          // Simulate the same logical snapshot already becoming current on refresh.
          refreshed_metadata = metadata;
          ICEBERG_ASSIGN_OR_RAISE(
              refreshed,
              Table::Make(table_->name(), std::move(metadata),
                          std::string(table_->metadata_file_location()) + ".refreshed",
                          file_io_, mock));
          if (first_attempt) {
            return CommitFailed("conflict");
          }
          return refreshed;
        });
    EXPECT_CALL(*mock, LoadTable(::testing::_))
        .WillOnce(
            [&](const auto&) -> Result<std::shared_ptr<Table>> { return refreshed; });
    ICEBERG_UNWRAP_OR_FAIL(
        auto table,
        Table::Make(table_->name(), table_->metadata(),
                    std::string(table_->metadata_file_location()), file_io_, mock));
    std::shared_ptr<Transaction> txn;
    if (other_update) {
      ICEBERG_UNWRAP_OR_FAIL(txn, table->NewTransaction());
    }
    ICEBERG_UNWRAP_OR_FAIL(auto append,
                           txn ? txn->NewFastAppend() : table->NewFastAppend());
    int reports = 0;
    int deletes = 0;
    append->AppendFile(file_a_)
        .ReportWith(std::make_shared<CallbackReporter>([&]() -> Status {
          ++reports;
          return {};
        }))
        .DeleteWith([&](const std::string& path) {
          ++deletes;
          return file_io_->DeleteFile(path);
        });
    ASSERT_THAT(append->Commit(), IsOk());
    if (txn) {
      ICEBERG_UNWRAP_OR_FAIL(auto props, txn->NewUpdateProperties());
      props->Set("effective", "value");
      ASSERT_THAT(props->Commit(), IsOk());
      ICEBERG_UNWRAP_OR_FAIL(auto committed, txn->Commit());
      EXPECT_EQ(committed->properties().configs().at("effective"), "value");
      EXPECT_EQ(txn->state(), TransactionState::kCommitted);
    }
    EXPECT_EQ(deletes, 4);
    EXPECT_EQ(reports, 0);
    EXPECT_THAT(append->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_EQ(deletes, 4);
  }
}

TEST_F(FastAppendTest, FrozenFilterAliasesCannotChangeReplay) {
  file_a_->partition = PartitionValues({Literal::Long(1)});
  file_b_->partition = PartitionValues({Literal::Long(2)});
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewFastAppend());
  first->AppendFile(file_a_).AppendFile(file_b_);
  ASSERT_THAT(first->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  FailCommits(2);
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table_->NewTransaction());
  ICEBERG_UNWRAP_OR_FAIL(auto update, txn->NewDeleteFiles());
  auto reference = Expressions::Ref("x");
  auto predicate = Expressions::Equal<BoundReference>(reference, Literal::Long(1));
  update->DeleteFromRowFilter(predicate);
  ASSERT_THAT(update->Commit(), IsOk());
  *reference = *Expressions::Ref("missing");
  ASSERT_THAT(txn->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->summary.at("deleted-data-files"), "1");
}

TEST_F(FastAppendTest, StagedNoopIsCleanedAndCanBecomeEffectiveOnRebase) {
  for (bool rebase : {false, true}) {
    SCOPED_TRACE(rebase);
    auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
    ON_CALL(*mock, LoadTable(::testing::_)).WillByDefault([&](const auto& name) {
      return catalog_->LoadTable(name);
    });
    ON_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .WillByDefault(
            [&](const auto& name, const auto& requirements, const auto& changes) {
              return catalog_->UpdateTable(name, requirements, changes);
            });
    EXPECT_CALL(*mock, LoadTable(::testing::_)).Times(rebase ? 1 : 0);
    EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
        .Times(rebase ? 1 : 0);
    ICEBERG_UNWRAP_OR_FAIL(auto ctx,
                           TransactionContext::Make(table_, TransactionKind::kUpdate));
    auto update = std::make_shared<TestSnapshotUpdate>(ctx);
    update->write_partial = true;
    auto builder = TableMetadataBuilder::BuildFrom(table_->metadata().get());
    auto existing = std::make_shared<Snapshot>(Snapshot{
        .snapshot_id = update->SnapshotId(),
        .sequence_number = table_->metadata()->NextSequenceNumber(),
        .timestamp_ms = CurrentTimePointMs(),
        .manifest_list = table_location_ + "/metadata/existing-noop-list.avro",
        .summary = {{SnapshotSummaryFields::kOperation, DataOperation::kAppend}},
        .schema_id = table_->metadata()->current_schema_id,
    });
    ICEBERG_UNWRAP_OR_FAIL(
        auto writer, ManifestListWriter::MakeWriter(table_->metadata()->format_version,
                                                    existing->snapshot_id, std::nullopt,
                                                    existing->manifest_list, file_io_,
                                                    existing->sequence_number));
    ASSERT_THAT(writer->Close(), IsOk());
    builder->SetBranchSnapshot(existing, std::string(SnapshotRef::kMainBranch));
    ICEBERG_UNWRAP_OR_FAIL(std::shared_ptr<TableMetadata> metadata, builder->Build());
    // Prepare a base whose current snapshot has this update's logical ID.
    ICEBERG_UNWRAP_OR_FAIL(
        ctx->table,
        Table::Make(table_->name(), metadata,
                    std::string(table_->metadata_file_location()) + ".synthetic",
                    file_io_, mock));
    ctx->metadata_builder = TableMetadataBuilder::BuildFrom(metadata.get());
    std::vector<std::string> deleted;
    update->DeleteWith([&](const std::string& path) {
      deleted.push_back(path);
      return file_io_->DeleteFile(path);
    });
    if (rebase) {
      // The table refreshes before the update commits its original builder.
      // Initial Apply is a no-op; internal replay sees the real empty table.
      ASSERT_THAT(ctx->table->Refresh(), IsOk());
    }
    ASSERT_THAT(update->Commit(), IsOk());
    EXPECT_EQ(update->applies, rebase ? 2 : 1);
    EXPECT_EQ(update->finalizes, rebase ? 1 : 0);
    EXPECT_EQ(update->reports, rebase ? 1 : 0);
    EXPECT_THAT(deleted, ::testing::SizeIs(rebase ? 3U : 2U));
    EXPECT_THAT(file_io_->ReadFile(existing->manifest_list, std::nullopt), IsOk());
    EXPECT_THAT(update->Commit(), IsError(ErrorKind::kValidationFailed));
  }
}

TEST_F(FastAppendTest, ReplayFailurePartwayThroughCleansAllUncommittedGenerations) {
  file_a_->partition = PartitionValues({Literal::Long(1)});
  file_b_->partition = PartitionValues({Literal::Long(3)});
  ICEBERG_UNWRAP_OR_FAIL(auto initial, table_->NewFastAppend());
  initial->AppendFile(file_a_);
  ASSERT_THAT(initial->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto original_snapshot, table_->current_snapshot());
  auto concurrent_file = CreateDataFile("/data/concurrent.parquet", 1, 10, 1);
  auto replacement = CreateDataFile("/data/replacement.parquet", 100, 50, 1);
  auto mock = std::make_shared<::testing::NiceMock<MockCatalog>>();
  EXPECT_CALL(*mock, UpdateTable(::testing::_, ::testing::_, ::testing::_))
      .Times(1)
      .WillOnce(
          [&](const auto&, const auto&, const auto&) -> Result<std::shared_ptr<Table>> {
            ICEBERG_ASSIGN_OR_RAISE(auto latest, catalog_->LoadTable(table_ident_));
            ICEBERG_ASSIGN_OR_RAISE(auto concurrent, latest->NewFastAppend());
            concurrent->AppendFile(concurrent_file);
            ICEBERG_RETURN_UNEXPECTED(concurrent->Commit());
            return CommitFailed("concurrent append");
          });
  EXPECT_CALL(*mock, LoadTable(::testing::_)).Times(1).WillOnce([&](const auto& name) {
    return catalog_->LoadTable(name);
  });
  ICEBERG_UNWRAP_OR_FAIL(
      auto table,
      Table::Make(table_->name(), table_->metadata(),
                  std::string(table_->metadata_file_location()), file_io_, mock));
  ICEBERG_UNWRAP_OR_FAIL(auto txn, table->NewTransaction());
  std::vector<std::string> deleted;
  std::shared_ptr<FastAppend> append;
  auto delete_file = [&](const std::string& path) {
    deleted.push_back(path);
    const auto state = txn->state();
    EXPECT_THAT(append->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(txn->Commit(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(txn->NewFastAppend(), IsError(ErrorKind::kValidationFailed));
    EXPECT_THAT(txn->Abort(), IsError(ErrorKind::kValidationFailed));
    EXPECT_EQ(txn->state(), state);
    return file_io_->DeleteFile(path);
  };
  ICEBERG_UNWRAP_OR_FAIL(append, txn->NewFastAppend());
  append->AppendFile(file_b_).DeleteWith(delete_file);
  ASSERT_THAT(append->Commit(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto overwrite, txn->NewOverwrite());
  overwrite->DeleteFile(file_a_)
      .AddFile(replacement)
      .ValidateFromSnapshot(original_snapshot->snapshot_id)
      .ConflictDetectionFilter(Expressions::Equal("x", Literal::Long(1)))
      .ValidateNoConflictingData()
      .DeleteWith(delete_file);
  ASSERT_THAT(overwrite->Commit(), IsOk());
  EXPECT_THAT(txn->Commit(), HasErrorMessage("Found conflicting files"));
  EXPECT_EQ(txn->state(), TransactionState::kFailed);
  EXPECT_GE(deleted.size(), 6U);
  std::unordered_set<std::string> unique_deletes(deleted.begin(), deleted.end());
  EXPECT_EQ(unique_deletes.size(), deleted.size());
  EXPECT_THAT(txn->Abort(), IsOk());
  EXPECT_EQ(deleted.size(), unique_deletes.size());

  std::unordered_set<std::string> committed_paths;
  auto metadata = ReloadMetadata();
  for (const auto& snapshot : metadata->snapshots) {
    committed_paths.insert(snapshot->manifest_list);
    SnapshotCache cache(snapshot.get());
    ICEBERG_UNWRAP_OR_FAIL(auto manifests, cache.Manifests(file_io_));
    for (const auto& manifest : manifests) {
      committed_paths.insert(manifest.manifest_path);
    }
  }
  auto& io = static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_);
  ::arrow::fs::FileSelector selector;
  selector.base_dir = table_location_ + "/metadata";
  selector.recursive = true;
  auto files = io.fs()->GetFileInfo(selector);
  ASSERT_TRUE(files.ok()) << files.status();
  std::unordered_set<std::string> remaining;
  for (const auto& file : *files) {
    if (file.path().ends_with(".avro")) {
      remaining.insert(file.path());
    }
  }
  EXPECT_EQ(remaining, committed_paths);
}

}  // namespace iceberg
