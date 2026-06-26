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
#include <optional>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transaction.h"

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

}  // namespace iceberg
