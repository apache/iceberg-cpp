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

/// \file scan_planning_metrics_test.cc
/// End-to-end tests for scan planning metrics, mirroring Java's
/// ScanPlanningAndReportingTestBase.

#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/expression/expressions.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metrics/metrics_reporter.h"
#include "iceberg/metrics/scan_report.h"
#include "iceberg/result.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_scan.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/transform.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

namespace {

/// Reporter that captures the most recent ScanReport for assertions.
class CapturingReporter final : public MetricsReporter {
 public:
  Status Report(const MetricsReport& report) override {
    if (std::holds_alternative<ScanReport>(report)) {
      last_ = std::get<ScanReport>(report);
    }
    return {};
  }

  const std::optional<ScanReport>& last() const { return last_; }
  void clear() { last_.reset(); }

 private:
  std::optional<ScanReport> last_;
};

}  // namespace

class ScanPlanningMetricsTest : public ScanTestBase {
 protected:
  void SetUp() override {
    ScanTestBase::SetUp();
    reporter_ = std::make_shared<CapturingReporter>();

    ICEBERG_UNWRAP_OR_FAIL(
        id_identity_spec_,
        PartitionSpec::Make(/*spec_id=*/2,
                            {PartitionField(/*source_id=*/1, /*field_id=*/1001, "id",
                                            Transform::Identity())}));
  }

  /// \brief Build a DataFile with optional lower/upper bounds on the "id" field.
  std::shared_ptr<DataFile> MakeDataFile(const std::string& path,
                                         const PartitionValues& partition,
                                         int32_t spec_id, int64_t record_count = 1,
                                         std::optional<int32_t> lower_id = std::nullopt,
                                         std::optional<int32_t> upper_id = std::nullopt) {
    auto file = std::make_shared<DataFile>(DataFile{
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = record_count,
        .file_size_in_bytes = 10,
        .sort_order_id = 0,
        .partition_spec_id = spec_id,
    });
    if (lower_id.has_value()) {
      file->lower_bounds[1] = Literal::Int(lower_id.value()).Serialize().value();
    }
    if (upper_id.has_value()) {
      file->upper_bounds[1] = Literal::Int(upper_id.value()).Serialize().value();
    }
    return file;
  }

  /// \brief Build a positional-delete DataFile.
  std::shared_ptr<DataFile> MakePositionDeleteFile(
      const std::string& path, const PartitionValues& partition, int32_t spec_id,
      std::optional<std::string> referenced_file = std::nullopt) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kPositionDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .referenced_data_file = std::move(referenced_file),
        .partition_spec_id = spec_id,
    });
  }

  /// \brief Build a global (unpartitioned) equality-delete DataFile.
  std::shared_ptr<DataFile> MakeEqualityDeleteFile(const std::string& path,
                                                   const PartitionValues& partition,
                                                   int32_t spec_id,
                                                   std::vector<int> equality_ids = {1}) {
    return std::make_shared<DataFile>(DataFile{
        .content = DataFile::Content::kEqualityDeletes,
        .file_path = path,
        .file_format = FileFormatType::kParquet,
        .partition = partition,
        .record_count = 1,
        .file_size_in_bytes = 10,
        .equality_ids = std::move(equality_ids),
        .partition_spec_id = spec_id,
    });
  }

  /// \brief Build a single-snapshot TableMetadata from a manifest list path.
  std::shared_ptr<TableMetadata> BuildMetadata(
      int64_t snapshot_id, const std::string& manifest_list_path,
      std::shared_ptr<PartitionSpec> spec = nullptr) {
    if (!spec) spec = partitioned_spec_;
    const auto ts = TimePointMsFromUnixMs(1609459200000L);
    auto snapshot =
        std::make_shared<Snapshot>(Snapshot{.snapshot_id = snapshot_id,
                                            .parent_snapshot_id = std::nullopt,
                                            .sequence_number = 1L,
                                            .timestamp_ms = ts,
                                            .manifest_list = manifest_list_path,
                                            .schema_id = schema_->schema_id()});
    return std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2,
                      .table_uuid = "test-table-uuid",
                      .location = "/tmp/table",
                      .last_sequence_number = 1L,
                      .last_updated_ms = ts,
                      .last_column_id = 2,
                      .schemas = {schema_},
                      .current_schema_id = schema_->schema_id(),
                      .partition_specs = {spec, unpartitioned_spec_},
                      .default_spec_id = spec->spec_id(),
                      .last_partition_id = 1001,
                      .current_snapshot_id = snapshot_id,
                      .snapshots = {snapshot},
                      .snapshot_log = {SnapshotLogEntry{.timestamp_ms = ts,
                                                        .snapshot_id = snapshot_id}},
                      .default_sort_order_id = 0,
                      .refs = {{"main", std::make_shared<SnapshotRef>(SnapshotRef{
                                            .snapshot_id = snapshot_id,
                                            .retention = SnapshotRef::Branch{}})}}});
  }

  /// \brief Wrapper matching WriteManifestList(format_version, snap_id, seq, manifests).
  std::string WriteManifestList(int8_t format_version, int64_t snapshot_id,
                                int64_t sequence_number,
                                const std::vector<ManifestFile>& manifests) {
    return ScanTestBase::WriteManifestList(format_version, snapshot_id,
                                           /*parent_snapshot_id=*/0L, sequence_number,
                                           manifests);
  }

  std::shared_ptr<CapturingReporter> reporter_;
  std::shared_ptr<PartitionSpec> id_identity_spec_;
};

// ---------------------------------------------------------------------------
// Test 1: Verify a ScanReport is fired and contains basic accurate fields.
// Mirrors Java's scanningWithMultipleReporters().
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanReportFiredAfterPlanFiles) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2000L;
  const auto part = PartitionValues({Literal::Int(0)});

  auto data_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/file_a.parquet", part, partitioned_spec_->spec_id(),
                              /*record_count=*/100,
                              /*lower_id=*/1, /*upper_id=*/50))},
      partitioned_spec_);
  auto manifest_list =
      WriteManifestList(version, kSnapshotId, /*sequence_number=*/1, {data_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1u);

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& report = *reporter_->last();
  EXPECT_EQ(report.table_name, "test.table");
  EXPECT_EQ(report.snapshot_id, kSnapshotId);

  const auto& m = report.scan_metrics;
  ASSERT_TRUE(m.total_planning_duration.has_value());
  EXPECT_EQ(m.total_planning_duration->count, 1);
  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 1);
}

// ---------------------------------------------------------------------------
// Test 2: Two manifests, 3 total data files — verify all 12 counters.
// Mirrors Java's scanningWithMultipleDataManifests() (unfiltered sub-scan).
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithMultipleDataManifests) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2001L;
  const auto part = PartitionValues({Literal::Int(0)});

  auto manifest1 = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
           ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
           MakeDataFile("/data/file_a.parquet", part, partitioned_spec_->spec_id())),
       MakeEntry(
           ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
           MakeDataFile("/data/file_b.parquet", part, partitioned_spec_->spec_id()))},
      partitioned_spec_);

  auto manifest2 = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
          MakeDataFile("/data/file_c.parquet", part, partitioned_spec_->spec_id()))},
      partitioned_spec_);

  auto manifest_list = WriteManifestList(version, kSnapshotId, /*sequence_number=*/1,
                                         {manifest1, manifest2});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 3u);

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 3);
  ASSERT_TRUE(m.result_delete_files.has_value());
  EXPECT_EQ(m.result_delete_files->value, 0);
  ASSERT_TRUE(m.scanned_data_manifests.has_value());
  EXPECT_EQ(m.scanned_data_manifests->value, 2);
  ASSERT_TRUE(m.scanned_delete_manifests.has_value());
  EXPECT_EQ(m.scanned_delete_manifests->value, 0);
  ASSERT_TRUE(m.skipped_data_manifests.has_value());
  EXPECT_EQ(m.skipped_data_manifests->value, 0);
  ASSERT_TRUE(m.skipped_delete_manifests.has_value());
  EXPECT_EQ(m.skipped_delete_manifests->value, 0);
  ASSERT_TRUE(m.total_data_manifests.has_value());
  EXPECT_EQ(m.total_data_manifests->value, 2);
  ASSERT_TRUE(m.total_delete_manifests.has_value());
  EXPECT_EQ(m.total_delete_manifests->value, 0);
  ASSERT_TRUE(m.skipped_data_files.has_value());
  EXPECT_EQ(m.skipped_data_files->value, 0);
  ASSERT_TRUE(m.skipped_delete_files.has_value());
  EXPECT_EQ(m.skipped_delete_files->value, 0);
}

// ---------------------------------------------------------------------------
// Test 3: Partition filter prunes one of two manifests.
// Uses an identity(id) partition so the manifest evaluator can prune by
// the id range recorded in each manifest's partition field summary.
// Mirrors Java's scanningWithMultipleDataManifests() (filtered sub-scan).
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithManifestPruning) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2002L;

  // Manifest 1: id partition = 1 (files with id=1)
  const auto part1 = PartitionValues({Literal::Int(1)});
  auto manifest1 = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
          MakeDataFile("/data/file_a.parquet", part1, id_identity_spec_->spec_id()))},
      id_identity_spec_);

  // Manifest 2: id partition = 2 (files with id=2)
  const auto part2 = PartitionValues({Literal::Int(2)});
  auto manifest2 = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
          MakeDataFile("/data/file_b.parquet", part2, id_identity_spec_->spec_id()))},
      id_identity_spec_);

  auto manifest_list = WriteManifestList(version, kSnapshotId, /*sequence_number=*/1,
                                         {manifest1, manifest2});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list, id_identity_spec_);

  // Filter id = 1: only manifest 1 survives the manifest-level evaluator.
  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_)
      .TableName("test.table")
      .Filter(Expressions::Equal("id", Literal::Int(1)));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1u);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "/data/file_a.parquet");

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  ASSERT_TRUE(m.total_data_manifests.has_value());
  EXPECT_EQ(m.total_data_manifests->value, 2);
  ASSERT_TRUE(m.scanned_data_manifests.has_value());
  EXPECT_EQ(m.scanned_data_manifests->value, 1);
  ASSERT_TRUE(m.skipped_data_manifests.has_value());
  EXPECT_EQ(m.skipped_data_manifests->value, 1);
  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 1);
  ASSERT_TRUE(m.skipped_data_files.has_value());
  EXPECT_EQ(m.skipped_data_files->value, 0);
}

// ---------------------------------------------------------------------------
// Test 4: Row-stats filter skips one entry inside a scanned manifest.
// Both files live in the same manifest; only the inclusive metrics evaluator
// (lower/upper bounds on "id") can distinguish them.
// Mirrors Java's scanningWithSkippedDataFiles().
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithSkippedDataFiles) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2003L;
  const auto part = PartitionValues({Literal::Int(0)});

  // Both files share the same bucket partition so the manifest is not pruned.
  // file_a covers id [1, 50]; file_b covers id [51, 100].
  auto data_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/file_a.parquet", part, partitioned_spec_->spec_id(),
                              /*record_count=*/50, /*lower_id=*/1, /*upper_id=*/50)),
       MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/file_b.parquet", part, partitioned_spec_->spec_id(),
                              /*record_count=*/50, /*lower_id=*/51, /*upper_id=*/100))},
      partitioned_spec_);
  auto manifest_list =
      WriteManifestList(version, kSnapshotId, /*sequence_number=*/1, {data_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list);

  // Filter id = 25: within file_a's range, outside file_b's range.
  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_)
      .TableName("test.table")
      .Filter(Expressions::Equal("id", Literal::Int(25)));
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1u);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "/data/file_a.parquet");

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  ASSERT_TRUE(m.total_data_manifests.has_value());
  EXPECT_EQ(m.total_data_manifests->value, 1);
  ASSERT_TRUE(m.scanned_data_manifests.has_value());
  EXPECT_EQ(m.scanned_data_manifests->value, 1);
  ASSERT_TRUE(m.skipped_data_manifests.has_value());
  EXPECT_EQ(m.skipped_data_manifests->value, 0);
  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 1);
  ASSERT_TRUE(m.skipped_data_files.has_value());
  EXPECT_EQ(m.skipped_data_files->value, 1);
}

// ---------------------------------------------------------------------------
// Test 5: Scan with positional delete files — verify delete file counters.
// Mirrors Java's scanningWithDeletes().
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithDeleteFiles) {
  auto version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files are only supported in format version 2+";
  }
  constexpr int64_t kSnapshotId = 2004L;
  const auto part = PartitionValues({Literal::Int(0)});

  auto data_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
           ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
           MakeDataFile("/data/file_a.parquet", part, partitioned_spec_->spec_id())),
       MakeEntry(
           ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
           MakeDataFile("/data/file_b.parquet", part, partitioned_spec_->spec_id()))},
      partitioned_spec_);

  // One positional-delete file covering file_a.
  auto delete_manifest = WriteDeleteManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/2,
          MakePositionDeleteFile("/data/pos_delete.parquet", part,
                                 partitioned_spec_->spec_id(), "/data/file_a.parquet"))},
      partitioned_spec_);

  auto manifest_list = WriteManifestList(version, kSnapshotId, /*sequence_number=*/2,
                                         {data_manifest, delete_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2u);

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 2);
  ASSERT_TRUE(m.result_delete_files.has_value());
  EXPECT_EQ(m.result_delete_files->value, 1);
  ASSERT_TRUE(m.scanned_data_manifests.has_value());
  EXPECT_EQ(m.scanned_data_manifests->value, 1);
  ASSERT_TRUE(m.scanned_delete_manifests.has_value());
  EXPECT_EQ(m.scanned_delete_manifests->value, 1);
  ASSERT_TRUE(m.total_data_manifests.has_value());
  EXPECT_EQ(m.total_data_manifests->value, 1);
  ASSERT_TRUE(m.total_delete_manifests.has_value());
  EXPECT_EQ(m.total_delete_manifests->value, 1);
  ASSERT_TRUE(m.indexed_delete_files.has_value());
  EXPECT_EQ(m.indexed_delete_files->value, 1);
  ASSERT_TRUE(m.positional_delete_files.has_value());
  EXPECT_EQ(m.positional_delete_files->value, 1);
  ASSERT_TRUE(m.equality_delete_files.has_value());
  EXPECT_EQ(m.equality_delete_files->value, 0);
  ASSERT_TRUE(m.dvs.has_value());
  EXPECT_EQ(m.dvs->value, 0);
}

// ---------------------------------------------------------------------------
// Test 6: IgnoreDeleted manifest-level skip.
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithIgnoreDeletedManifest) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2006L;
  const auto part = PartitionValues({Literal::Int(0)});

  // Manifest 1: the single entry is kDeleted,
  // IgnoreDeleted() will skip this manifest at the manifest level.
  auto deleted_only_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(ManifestStatus::kDeleted, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/deleted_file.parquet", part,
                              partitioned_spec_->spec_id()))},
      partitioned_spec_);

  // Manifest 2: one kAdded entry — survives IgnoreDeleted and contributes
  // one task to the result.
  auto added_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
          MakeDataFile("/data/live_file.parquet", part, partitioned_spec_->spec_id()))},
      partitioned_spec_);

  auto manifest_list = WriteManifestList(version, kSnapshotId, /*sequence_number=*/1,
                                         {deleted_only_manifest, added_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1u);
  EXPECT_EQ(tasks[0]->data_file()->file_path, "/data/live_file.parquet");

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  ASSERT_TRUE(m.total_data_manifests.has_value());
  EXPECT_EQ(m.total_data_manifests->value, 2);
  // The deleted-only manifest is skipped at the manifest level, so it must
  // appear in skipped_data_manifests.
  ASSERT_TRUE(m.scanned_data_manifests.has_value());
  EXPECT_EQ(m.scanned_data_manifests->value, 1);
  ASSERT_TRUE(m.skipped_data_manifests.has_value());
  EXPECT_EQ(m.skipped_data_manifests->value, 1);
  ASSERT_TRUE(m.skipped_data_files.has_value());
  EXPECT_EQ(m.skipped_data_files->value, 0);
  ASSERT_TRUE(m.result_data_files.has_value());
  EXPECT_EQ(m.result_data_files->value, 1);
}

// ---------------------------------------------------------------------------
// Test: a single delete file applying to multiple data files is deduplicated in
// indexed_delete_files (counted once, at index-build time), but result_delete_files and
// total_delete_file_size_in_bytes are counted once per FileScanTask/data file, mirroring
// Java's ScanMetricsUtil.fileTask() vs. ScanMetricsUtil.indexedDeleteFile() split.
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanningWithDeleteFileSharedAcrossDataFiles) {
  auto version = GetParam();
  if (version < 2) {
    GTEST_SKIP() << "Delete files are only supported in format version 2+";
  }
  constexpr int64_t kSnapshotId = 2005L;
  const auto empty_partition = PartitionValues(std::vector<Literal>{});

  auto data_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/file_a.parquet", empty_partition,
                              unpartitioned_spec_->spec_id())),
       MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
                 MakeDataFile("/data/file_b.parquet", empty_partition,
                              unpartitioned_spec_->spec_id()))},
      unpartitioned_spec_);

  // A single global equality-delete file applies to every unpartitioned data file.
  auto delete_manifest = WriteDeleteManifest(
      version, kSnapshotId,
      {MakeEntry(ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/2,
                 MakeEqualityDeleteFile("/data/global-eq-delete.parquet", empty_partition,
                                        unpartitioned_spec_->spec_id()))},
      unpartitioned_spec_);

  auto manifest_list = WriteManifestList(version, kSnapshotId, /*sequence_number=*/2,
                                         {data_manifest, delete_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list, unpartitioned_spec_);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 2u);

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& m = reporter_->last()->scan_metrics;

  // Deduplicated: the delete file is indexed once, regardless of how many data files it
  // matches.
  ASSERT_TRUE(m.indexed_delete_files.has_value());
  EXPECT_EQ(m.indexed_delete_files->value, 1);
  ASSERT_TRUE(m.equality_delete_files.has_value());
  EXPECT_EQ(m.equality_delete_files->value, 1);

  // Not deduplicated: counted once per FileScanTask, so the shared delete file
  // contributes to both data files' tasks.
  ASSERT_TRUE(m.result_delete_files.has_value());
  EXPECT_EQ(m.result_delete_files->value, 2);
}

// ---------------------------------------------------------------------------
// Test: ScanReport includes nested projected field ids/names, not just the
// top-level struct field. Mirrors Java's use of TypeUtil.getProjectedIds().
// ---------------------------------------------------------------------------
TEST_P(ScanPlanningMetricsTest, ScanReportIncludesNestedProjectedFields) {
  auto version = GetParam();
  constexpr int64_t kSnapshotId = 2020L;

  // Override the base flat schema with one that has a nested struct column.
  schema_ = std::make_shared<Schema>(
      std::vector<SchemaField>{
          SchemaField::MakeRequired(1, "id", int32()),
          SchemaField::MakeRequired(
              2, "location",
              struct_({SchemaField::MakeRequired(3, "lat", float64()),
                       SchemaField::MakeRequired(4, "long", float64())}))},
      /*schema_id=*/0);

  auto data_manifest = WriteDataManifest(
      version, kSnapshotId,
      {MakeEntry(
          ManifestStatus::kAdded, kSnapshotId, /*sequence_number=*/1,
          MakeDataFile("/data/file_a.parquet", PartitionValues(std::vector<Literal>{}),
                       unpartitioned_spec_->spec_id()))},
      unpartitioned_spec_);
  auto manifest_list =
      WriteManifestList(version, kSnapshotId, /*sequence_number=*/1, {data_manifest});
  auto metadata = BuildMetadata(kSnapshotId, manifest_list, unpartitioned_spec_);

  reporter_->clear();
  ICEBERG_UNWRAP_OR_FAIL(auto builder, DataTableScanBuilder::Make(metadata, file_io_));
  builder->MetricsReporter(reporter_).TableName("test.table");
  ICEBERG_UNWRAP_OR_FAIL(auto scan, builder->Build());
  ICEBERG_UNWRAP_OR_FAIL(auto tasks, scan->PlanFiles());
  ASSERT_EQ(tasks.size(), 1u);

  ASSERT_TRUE(reporter_->last().has_value());
  const auto& report = *reporter_->last();

  EXPECT_THAT(report.projected_field_ids, ::testing::UnorderedElementsAre(1, 2, 3, 4));
  EXPECT_THAT(
      report.projected_field_names,
      ::testing::UnorderedElementsAre("id", "location", "location.lat", "location.long"));
}

INSTANTIATE_TEST_SUITE_P(ScanPlanningMetricsVersions, ScanPlanningMetricsTest,
                         testing::Values(2, 3));

}  // namespace iceberg
