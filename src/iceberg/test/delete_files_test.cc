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

#include "iceberg/update/delete_files.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/avro/avro_register.h"
#include "iceberg/expression/expressions.h"
#include "iceberg/expression/literal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/partition_field.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/test/executor.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/update_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/update/fast_append.h"
#include "iceberg/update/row_delta.h"
#include "iceberg/update/snapshot_manager.h"

namespace iceberg {

class DeleteFilesTest : public MinimalUpdateTestBase {
 protected:
  static void SetUpTestSuite() { avro::RegisterAll(); }

  void SetUp() override {
    MinimalUpdateTestBase::SetUp();

    ICEBERG_UNWRAP_OR_FAIL(spec_, table_->spec());
    ICEBERG_UNWRAP_OR_FAIL(schema_, table_->schema());
    file_a_ = MakeDataFile("/data/file_a.parquet", /*partition_x=*/1L);
    file_b_ = MakeDataFile("/data/file_b.parquet", /*partition_x=*/2L);
  }

  std::shared_ptr<DataFile> MakeDataFile(const std::string& path, int64_t partition_x) {
    auto file = std::make_shared<DataFile>();
    file->content = DataFile::Content::kData;
    file->file_path = table_location_ + path;
    file->file_format = FileFormatType::kParquet;
    file->partition = PartitionValues(std::vector<Literal>{Literal::Long(partition_x)});
    file->file_size_in_bytes = 1024;
    file->record_count = 100;
    file->partition_spec_id = spec_->spec_id();
    return file;
  }

  void CreateTypedPartitionTable(std::shared_ptr<PrimitiveType> source_type,
                                 std::shared_ptr<Transform> transform) {
    schema_ = std::make_shared<Schema>(std::vector<SchemaField>{
        SchemaField::MakeRequired(1, "x", std::move(source_type))});
    ICEBERG_UNWRAP_OR_FAIL(
        spec_,
        PartitionSpec::Make(
            *schema_, 0, {PartitionField(1, 1000, "part", std::move(transform))}, false));
    ASSERT_THAT(catalog_->DropTable(table_ident_, /*purge=*/false), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(
        table_, catalog_->CreateTable(table_ident_, schema_, spec_, SortOrder::Unsorted(),
                                      table_location_, {}));
  }

  void SetLongBounds(const std::shared_ptr<DataFile>& file, int32_t field_id,
                     int64_t lower, int64_t upper) {
    ASSERT_NE(file, nullptr);
    ICEBERG_UNWRAP_OR_FAIL(auto lower_bound, Literal::Long(lower).Serialize());
    ICEBERG_UNWRAP_OR_FAIL(auto upper_bound, Literal::Long(upper).Serialize());
    file->value_counts[field_id] = file->record_count;
    file->null_value_counts[field_id] = 0;
    file->lower_bounds[field_id] = lower_bound;
    file->upper_bounds[field_id] = upper_bound;
  }

  void CommitFiles(const std::vector<std::shared_ptr<DataFile>>& files) {
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->NewFastAppend());
    for (const auto& file : files) {
      append->AppendFile(file);
    }
    ASSERT_THAT(append->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
  }

  void CommitInitialFiles() { CommitFiles({file_a_, file_b_}); }

  void ExpectOneFileDeleted() {
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
              DataOperation::kDelete);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedDataFiles), "1");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kDeletedRecords), "100");
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedFileSize), "1024");
  }

  void ExpectDataManifest(const std::shared_ptr<Snapshot>& snapshot,
                          std::vector<std::string> paths,
                          std::vector<ManifestStatus> statuses,
                          std::vector<int64_t> snapshot_ids) {
    SnapshotReader reader(snapshot.get());
    ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DataManifests(file_io_));
    ASSERT_EQ(manifests.size(), 1U);
    ExpectManifestEntriesWithSnapshotIds(manifests[0], std::move(paths),
                                         std::move(statuses), std::move(snapshot_ids));
  }

  void ExpectManifestCounts(const Snapshot& snapshot, int created, int kept,
                            int replaced) {
    EXPECT_EQ(snapshot.summary.at(SnapshotSummaryFields::kManifestsCreated),
              std::to_string(created));
    EXPECT_EQ(snapshot.summary.at(SnapshotSummaryFields::kManifestsKept),
              std::to_string(kept));
    EXPECT_EQ(snapshot.summary.at(SnapshotSummaryFields::kManifestsReplaced),
              std::to_string(replaced));
  }

  std::shared_ptr<PartitionSpec> spec_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<DataFile> file_a_;
  std::shared_ptr<DataFile> file_b_;

  static constexpr int32_t kYFieldId = 2;
};

TEST_F(DeleteFilesTest, DeleteFileByPath) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_->file_path);

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFileByDataFile) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(file_a_);

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFromRowFilterCaseInsensitive) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->CaseSensitive(false).DeleteFromRowFilter(
      Expressions::Equal("X", Literal::Long(1L)));

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteCaseSensitivityBindsMetricsField) {
  SetLongBounds(file_a_, kYFieldId, 0, 2);
  CommitFiles({file_a_});
  auto filter = Expressions::LessThan("Y", Literal::Long(5));
  ICEBERG_UNWRAP_OR_FAIL(auto default_case, table_->NewDeleteFiles());
  EXPECT_THAT(default_case->DeleteFromRowFilter(filter).Commit(), HasErrorMessage("Y"));
  ICEBERG_UNWRAP_OR_FAIL(auto strict_case, table_->NewDeleteFiles());
  EXPECT_THAT(strict_case->DeleteFromRowFilter(filter).CaseSensitive(true).Commit(),
              HasErrorMessage("Y"));
  ICEBERG_UNWRAP_OR_FAIL(auto ignore_case, table_->NewDeleteFiles());
  ASSERT_THAT(ignore_case->DeleteFromRowFilter(filter).CaseSensitive(false).Commit(),
              IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, CannotDeleteFileWhereNotAllRowsMatchTruncatedPartition) {
  CreateTypedPartitionTable(string(), Transform::Truncate(2));
  file_a_->partition = PartitionValues(Literal::String("aa"));
  CommitFiles({file_a_});
  ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
  auto result =
      op->DeleteFromRowFilter(Expressions::Equal("x", Literal::String("aa"))).Commit();
  EXPECT_THAT(result, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(
      result,
      HasErrorMessage("Cannot delete file where some, but not all, rows match filter"));
  EXPECT_THAT(result, HasErrorMessage(file_a_->file_path));
}

TEST_F(DeleteFilesTest, DistinctPartitionKeysWithTheSameHashUseSeparateResiduals) {
  CreateTypedPartitionTable(float64(), Transform::Identity());
  // IEEE signed zero supplies distinct Iceberg values with the same C++ std::hash.
  file_a_->partition = PartitionValues(Literal::Double(-0.0));
  file_b_->partition = PartitionValues(Literal::Double(+0.0));
  ASSERT_NE(file_a_->partition, file_b_->partition);
  ASSERT_EQ(PartitionValuesHash{}(file_a_->partition),
            PartitionValuesHash{}(file_b_->partition));
  CommitInitialFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
  ASSERT_THAT(
      op->DeleteFromRowFilter(Expressions::Equal("x", Literal::Double(+0.0))).Commit(),
      IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ExpectDataManifest(snapshot, {file_a_->file_path, file_b_->file_path},
                     {ManifestStatus::kExisting, ManifestStatus::kDeleted},
                     {append->snapshot_id, snapshot->snapshot_id});
}

TEST_F(DeleteFilesTest, EmptyDeleteCommit) {
  CommitInitialFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto previous_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());

  EXPECT_THAT(delete_files->Commit(), IsOk());

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ASSERT_TRUE(snapshot->parent_snapshot_id.has_value());
  EXPECT_EQ(snapshot->parent_snapshot_id.value(), previous_snapshot->snapshot_id);
  EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kOperation),
            DataOperation::kDelete);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kDeletedDataFiles), 0U);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kDeletedRecords), 0U);
  EXPECT_EQ(snapshot->summary.count(SnapshotSummaryFields::kRemovedFileSize), 0U);
}

TEST_F(DeleteFilesTest, DeleteFromRowFilter) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFromRowFilter(Expressions::Equal("x", Literal::Long(1L)));

  EXPECT_THAT(delete_files->Commit(), IsOk());
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, DeleteFromRowFilterRejectsPartialMatchFile) {
  auto partial_match_file = MakeDataFile("/data/partial_match.parquet",
                                         /*partition_x=*/1L);
  SetLongBounds(partial_match_file, kYFieldId, /*lower=*/0L, /*upper=*/10L);
  CommitFiles({partial_match_file});
  ICEBERG_UNWRAP_OR_FAIL(auto previous_snapshot, table_->current_snapshot());

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFromRowFilter(Expressions::Equal("y", Literal::Long(5L)));

  auto status = delete_files->Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(status,
              HasErrorMessage("Cannot delete file where some, but not all, rows match "
                              "filter"));
  EXPECT_THAT(status, HasErrorMessage(partial_match_file->file_path));

  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  EXPECT_EQ(snapshot->snapshot_id, previous_snapshot->snapshot_id);
}

TEST_F(DeleteFilesTest, ValidateFilesExistRejectsMissingPath) {
  CommitInitialFiles();

  ICEBERG_UNWRAP_OR_FAIL(auto delete_files, table_->NewDeleteFiles());
  delete_files->DeleteFile(table_location_ + "/data/missing.parquet")
      .ValidateFilesExist();

  auto status = delete_files->Commit();
  EXPECT_THAT(status, IsError(ErrorKind::kValidationFailed));
  EXPECT_THAT(status, HasErrorMessage("Missing required files to delete"));
}

class DeleteFilesVersionTest
    : public DeleteFilesTest,
      public ::testing::WithParamInterface<std::tuple<int8_t, std::string>> {
 protected:
  int8_t format_version() const override { return std::get<0>(GetParam()); }
};

TEST_P(DeleteFilesVersionTest, MultipleDeletesRewriteOnlyLiveEntries) {
  const auto& branch = std::get<1>(GetParam());
  auto head = [&] {
    return table_->metadata()->SnapshotById(
        table_->metadata()->refs.at(branch)->snapshot_id);
  };
  auto file_c = MakeDataFile("/data/file_c.parquet", 3);
  ICEBERG_UNWRAP_OR_FAIL(auto add, table_->NewFastAppend());
  add->AppendFile(file_a_).AppendFile(file_b_).AppendFile(file_c).ToBranch(branch);
  ASSERT_THAT(add->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto append, head());
  ExpectManifestCounts(*append, 1, 0, 0);

  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewDeleteFiles());
  first->DeleteFile(file_a_).ToBranch(branch);
  ASSERT_THAT(first->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto deleted_a, head());
  ExpectManifestCounts(*deleted_a, 1, 0, 1);
  ExpectDataManifest(
      deleted_a, {file_a_->file_path, file_b_->file_path, file_c->file_path},
      {ManifestStatus::kDeleted, ManifestStatus::kExisting, ManifestStatus::kExisting},
      {deleted_a->snapshot_id, append->snapshot_id, append->snapshot_id});

  ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewDeleteFiles());
  second->DeleteFile(file_b_).ToBranch(branch);
  ASSERT_THAT(second->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto deleted_b, head());
  ExpectManifestCounts(*deleted_b, 1, 0, 1);
  ExpectDataManifest(deleted_b, {file_b_->file_path, file_c->file_path},
                     {ManifestStatus::kDeleted, ManifestStatus::kExisting},
                     {deleted_b->snapshot_id, append->snapshot_id});
}

INSTANTIATE_TEST_SUITE_P(TableVersionsAndBranches, DeleteFilesVersionTest,
                         ::testing::Combine(::testing::Values<int8_t>(1, 2, 3),
                                            ::testing::Values(std::string("main"),
                                                              std::string("audit"))));

TEST_F(DeleteFilesTest, AlreadyDeletedFilesAreIgnoredDuringDeletesByRowFilter) {
  SetLongBounds(file_a_, kYFieldId, 0, 10);
  SetLongBounds(file_b_, kYFieldId, 0, 4);
  CommitInitialFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewDeleteFiles());
  first->DeleteFile(file_a_);
  ASSERT_THAT(first->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto second, table_->NewDeleteFiles());
  second->DeleteFromRowFilter(Expressions::LessThan("y", Literal::Long(7)));
  ASSERT_THAT(second->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ExpectManifestCounts(*snapshot, 1, 0, 1);
  ExpectDataManifest(snapshot, {file_b_->file_path}, {ManifestStatus::kDeleted},
                     {snapshot->snapshot_id});
}

TEST_F(DeleteFilesTest, DeleteByMetricsWithinOnePartition) {
  file_b_->partition = file_a_->partition;
  SetLongBounds(file_a_, kYFieldId, 0, 2);
  SetLongBounds(file_b_, kYFieldId, 8, 10);
  CommitInitialFiles();
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
  op->DeleteFromRowFilter(Expressions::GreaterThan("y", Literal::Long(5)));
  ASSERT_THAT(op->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ExpectManifestCounts(*snapshot, 1, 0, 1);
  ExpectDataManifest(snapshot, {file_a_->file_path, file_b_->file_path},
                     {ManifestStatus::kExisting, ManifestStatus::kDeleted},
                     {append->snapshot_id, snapshot->snapshot_id});
}

TEST_F(DeleteFilesTest, DeleteByCombinedPartitionAndMetricsPredicates) {
  file_b_->partition = file_a_->partition;
  auto other_partition = MakeDataFile("/data/other.parquet", 2);
  SetLongBounds(file_a_, kYFieldId, 0, 2);
  SetLongBounds(file_b_, kYFieldId, 8, 10);
  SetLongBounds(other_partition, kYFieldId, 0, 10);
  CommitFiles({file_a_, file_b_, other_partition});
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
  op->DeleteFromRowFilter(
      Expressions::And(Expressions::Equal("x", Literal::Long(1)),
                       Expressions::GreaterThan("y", Literal::Long(5))));
  ASSERT_THAT(op->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ExpectDataManifest(
      snapshot, {file_a_->file_path, file_b_->file_path, other_partition->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kDeleted, ManifestStatus::kExisting},
      {append->snapshot_id, snapshot->snapshot_id, append->snapshot_id});
}

TEST_F(DeleteFilesTest, AlreadyDeletedFileWithAndWithoutExistenceValidation) {
  CommitFiles({file_a_});
  ICEBERG_UNWRAP_OR_FAIL(auto first, table_->NewDeleteFiles());
  first->DeleteFile(file_a_).ValidateFilesExist();
  ASSERT_THAT(first->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto required, table_->NewDeleteFiles());
  required->DeleteFile(file_a_).ValidateFilesExist();
  EXPECT_THAT(required->Commit(),
              HasErrorMessage("Missing required files to delete: " + file_a_->file_path));

  ICEBERG_UNWRAP_OR_FAIL(auto optional, table_->NewDeleteFiles());
  optional->DeleteFile(file_a_);
  ASSERT_THAT(optional->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
  ExpectManifestCounts(*snapshot, 0, 0, 0);
  SnapshotReader reader(snapshot.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.Manifests(file_io_));
  EXPECT_TRUE(manifests.empty());
}

TEST_F(DeleteFilesTest, DeleteFilesOnIndependentBranches) {
  auto file_c = MakeDataFile("/data/file_c.parquet", 3);
  CommitFiles({file_a_, file_b_, file_c});
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto refs, table_->NewSnapshotManager());
  refs->CreateBranch("audit", append->snapshot_id);
  ASSERT_THAT(refs->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());

  ICEBERG_UNWRAP_OR_FAIL(auto audit_delete, table_->NewDeleteFiles());
  audit_delete->DeleteFile(file_a_).ToBranch("audit");
  ASSERT_THAT(audit_delete->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto main_delete, table_->NewDeleteFiles());
  main_delete->DeleteFile(file_b_).DeleteFile(file_c);
  ASSERT_THAT(main_delete->Commit(), IsOk());
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto main, table_->current_snapshot());
  ICEBERG_UNWRAP_OR_FAIL(auto audit,
                         table_->metadata()->SnapshotById(
                             table_->metadata()->refs.at("audit")->snapshot_id));
  ExpectManifestCounts(*audit, 1, 0, 1);
  ExpectManifestCounts(*main, 1, 0, 1);
  ExpectDataManifest(
      audit, {file_a_->file_path, file_b_->file_path, file_c->file_path},
      {ManifestStatus::kDeleted, ManifestStatus::kExisting, ManifestStatus::kExisting},
      {audit->snapshot_id, append->snapshot_id, append->snapshot_id});
  ExpectDataManifest(
      main, {file_a_->file_path, file_b_->file_path, file_c->file_path},
      {ManifestStatus::kExisting, ManifestStatus::kDeleted, ManifestStatus::kDeleted},
      {append->snapshot_id, main->snapshot_id, main->snapshot_id});
}

namespace {

// A FileIO with one input-stream permit, as in Java's limited I/O pool test.
class SingleStreamFileIO : public FileIO {
 public:
  explicit SingleStreamFileIO(std::shared_ptr<FileIO> delegate)
      : delegate_(std::move(delegate)) {}

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path,
                                                  size_t length) override {
    ICEBERG_ASSIGN_OR_RAISE(auto input, delegate_->NewInputFile(std::move(path), length));
    return std::make_unique<LimitedInputFile>(*this, std::move(input));
  }

  Result<std::unique_ptr<InputFile>> NewInputFile(std::string path) override {
    ICEBERG_ASSIGN_OR_RAISE(auto input, delegate_->NewInputFile(std::move(path)));
    return std::make_unique<LimitedInputFile>(*this, std::move(input));
  }

  Result<std::unique_ptr<OutputFile>> NewOutputFile(std::string path) override {
    return delegate_->NewOutputFile(std::move(path));
  }

  Status DeleteFile(const std::string& path) override {
    return delegate_->DeleteFile(path);
  }

  std::unordered_map<std::string, int> opens;

 private:
  class LimitedStream : public SeekableInputStream {
   public:
    LimitedStream(std::unique_ptr<SeekableInputStream> stream,
                  std::shared_ptr<int> permit)
        : stream_(std::move(stream)), permit_(std::move(permit)) {}
    Result<int64_t> Position() const override { return stream_->Position(); }
    Status Seek(int64_t pos) override { return stream_->Seek(pos); }
    Result<int64_t> Read(std::span<std::byte> out) override { return stream_->Read(out); }
    Status ReadFully(int64_t pos, std::span<std::byte> out) override {
      return stream_->ReadFully(pos, out);
    }
    Status Close() override {
      ICEBERG_RETURN_UNEXPECTED(stream_->Close());
      permit_.reset();
      return {};
    }

   private:
    std::unique_ptr<SeekableInputStream> stream_;
    std::shared_ptr<int> permit_;
  };

  class LimitedInputFile : public InputFile {
   public:
    LimitedInputFile(SingleStreamFileIO& owner, std::unique_ptr<InputFile> file)
        : owner_(owner), file_(std::move(file)) {}
    std::string_view location() const override { return file_->location(); }
    Result<int64_t> Size() const override { return file_->Size(); }
    Result<std::unique_ptr<SeekableInputStream>> Open() override {
      if (!owner_.permit_.expired()) return IOError("One input stream is already open");
      ICEBERG_ASSIGN_OR_RAISE(auto stream, file_->Open());
      auto permit = std::make_shared<int>(0);
      owner_.permit_ = permit;
      ++owner_.opens[std::string(location())];
      return std::make_unique<LimitedStream>(std::move(stream), std::move(permit));
    }

   private:
    SingleStreamFileIO& owner_;
    std::unique_ptr<InputFile> file_;
  };

  std::weak_ptr<int> permit_;
  std::shared_ptr<FileIO> delegate_;
};

}  // namespace

TEST_F(DeleteFilesTest, FilterManifestsWithOneInputStream) {
  CommitFiles({file_a_});
  ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
  SnapshotReader reader(append.get());
  ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DataManifests(file_io_));
  ASSERT_EQ(manifests.size(), 1U);

  auto limited = std::make_shared<SingleStreamFileIO>(file_io_);
  ICEBERG_UNWRAP_OR_FAIL(
      auto table, Table::Make(table_->name(), table_->metadata(),
                              std::string(table_->metadata_file_location()), limited,
                              catalog_, table_->full_name(), table_->reporter()));
  ICEBERG_UNWRAP_OR_FAIL(auto op, table->NewDeleteFiles());
  op->DeleteFile(file_a_);
  ASSERT_THAT(op->Commit(), IsOk());
  EXPECT_EQ(limited->opens[append->manifest_list], 1);
  EXPECT_EQ(limited->opens[manifests[0].manifest_path], 2);
  ExpectOneFileDeleted();
}

TEST_F(DeleteFilesTest, ScanExecutorFiltersManifestsInSnapshotOrder) {
  CommitFiles({file_a_});
  CommitFiles({file_b_});
  ICEBERG_UNWRAP_OR_FAIL(auto before, table_->current_snapshot());
  SnapshotReader before_reader(before.get());
  ICEBERG_UNWRAP_OR_FAIL(auto originals, before_reader.DataManifests(file_io_));
  ASSERT_EQ(originals.size(), 2U);
  test::ThreadExecutor executor;
  ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
  op->ScanManifestsWith(executor);
  op->DeleteFromRowFilter(Expressions::AlwaysTrue());
  ASSERT_THAT(op->Commit(), IsOk());
  EXPECT_GE(executor.submit_count(), 2);
  ASSERT_THAT(table_->Refresh(), IsOk());
  ICEBERG_UNWRAP_OR_FAIL(auto after, table_->current_snapshot());
  SnapshotReader after_reader(after.get());
  ICEBERG_UNWRAP_OR_FAIL(auto filtered, after_reader.DataManifests(file_io_));
  ASSERT_EQ(filtered.size(), originals.size());
  for (size_t i = 0; i < originals.size(); ++i) {
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(originals[i]));
    ASSERT_EQ(entries.size(), 1U);
    ExpectManifestEntriesWithSnapshotIds(filtered[i], {entries[0].data_file->file_path},
                                         {ManifestStatus::kDeleted},
                                         {after->snapshot_id});
  }
}

class DeleteFilesV3Test : public DeleteFilesTest {
 protected:
  int8_t format_version() const override { return 3; }

  void CheckDanglingDVs(bool truncate, bool by_path) {
    file_b_->partition = file_a_->partition;
    SetLongBounds(file_a_, kYFieldId, 0, 2);
    SetLongBounds(file_b_, kYFieldId, 8, 10);
    // Both vectors share a Puffin file. Each has its own offset and data file.
    auto make_dv = [&](const std::shared_ptr<DataFile>& target, int64_t offset) {
      auto dv = MakeDataFile("/data/vectors.puffin", 1);
      dv->content = DataFile::Content::kPositionDeletes;
      dv->file_format = FileFormatType::kPuffin;
      dv->referenced_data_file = target->file_path;
      dv->content_offset = offset;
      dv->content_size_in_bytes = 6;
      dv->record_count = 5;
      return dv;
    };
    auto dv_a = make_dv(file_a_, 4);
    auto dv_b = make_dv(file_b_, 10);
    ICEBERG_UNWRAP_OR_FAIL(auto delta, table_->NewRowDelta());
    delta->AddRows(file_a_).AddRows(file_b_).AddDeletes(dv_a).AddDeletes(dv_b);
    ASSERT_THAT(delta->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto append, table_->current_snapshot());
    ICEBERG_UNWRAP_OR_FAIL(auto op, table_->NewDeleteFiles());
    if (by_path) {
      op->DeleteFile(file_a_->file_path);
    } else if (truncate) {
      op->DeleteFromRowFilter(Expressions::AlwaysTrue());
    } else {
      op->DeleteFromRowFilter(Expressions::LessThan("y", Literal::Long(5)));
    }
    ASSERT_THAT(op->Commit(), IsOk());
    ASSERT_THAT(table_->Refresh(), IsOk());
    ICEBERG_UNWRAP_OR_FAIL(auto snapshot, table_->current_snapshot());
    ExpectManifestCounts(*snapshot, 2, 0, 2);
    const auto expected_count = truncate ? "2" : "1";
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedDVs), expected_count);
    EXPECT_EQ(snapshot->summary.at(SnapshotSummaryFields::kRemovedDeleteFiles),
              expected_count);
    SnapshotReader reader(snapshot.get());
    ICEBERG_UNWRAP_OR_FAIL(auto manifests, reader.DeleteManifests(file_io_));
    ASSERT_EQ(manifests.size(), 1U);
    ICEBERG_UNWRAP_OR_FAIL(auto entries, ReadManifestEntries(manifests.front()));
    ASSERT_EQ(entries.size(), 2U);
    for (const auto& entry : entries) {
      EXPECT_EQ(entry.sequence_number, append->sequence_number);
      EXPECT_EQ(entry.file_sequence_number, append->sequence_number);
      const auto& file = *entry.data_file;
      const bool is_a = file.content_offset == dv_a->content_offset;
      EXPECT_EQ(file.content_offset, is_a ? dv_a->content_offset : dv_b->content_offset);
      EXPECT_EQ(file.referenced_data_file,
                is_a ? file_a_->file_path : file_b_->file_path);
      const bool deleted = truncate || is_a;
      EXPECT_EQ(entry.status,
                deleted ? ManifestStatus::kDeleted : ManifestStatus::kExisting);
      EXPECT_EQ(entry.snapshot_id, deleted ? snapshot->snapshot_id : append->snapshot_id);
    }
  }
};

TEST_F(DeleteFilesV3Test, RemovingDataFileByExpressionAlsoRemovesDV) {
  CheckDanglingDVs(/*truncate=*/false, /*by_path=*/false);
}

TEST_F(DeleteFilesV3Test, RemovingDataFileByPathAlsoRemovesDV) {
  CheckDanglingDVs(/*truncate=*/false, /*by_path=*/true);
}

TEST_F(DeleteFilesV3Test, TruncatingAlsoRemovesDVs) {
  CheckDanglingDVs(/*truncate=*/true, /*by_path=*/false);
}

}  // namespace iceberg
