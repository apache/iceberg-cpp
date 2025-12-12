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

#pragma once

#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "iceberg/iceberg_export.h"
#include "iceberg/util/config.h"

namespace iceberg {

/// \brief Table properties for Iceberg tables.
///
/// This class provides configuration entries for various Iceberg table properties
/// including format settings, commit behavior, file formats, compression settings,
/// and other table-level configurations.
class ICEBERG_EXPORT TableProperties : public ConfigBase<TableProperties> {
 public:
  template <typename T>
  using Entry = const ConfigBase<TableProperties>::Entry<T>;

  // Reserved table properties

  /// \brief Reserved table property for table format version.
  ///
  /// Iceberg will default a new table's format version to the latest stable and
  /// recommended version. This reserved property keyword allows users to override the
  /// Iceberg format version of the table metadata.
  ///
  /// If this table property exists when creating a table, the table will use the
  /// specified format version. If a table updates this property, it will try to upgrade
  /// to the specified format version.
  ///
  /// \note incomplete or unstable versions cannot be selected using this property.
  inline static Entry<std::string> kFormatVersion{"format-version", ""};
  /// \brief Reserved table property for table UUID.
  inline static Entry<std::string> kUuid{"uuid", ""};
  /// \brief Reserved table property for the total number of snapshots.
  inline static Entry<std::string> kSnapshotCount{"snapshot-count", ""};
  /// \brief Reserved table property for current snapshot summary.
  inline static Entry<std::string> kCurrentSnapshotSummary{"current-snapshot-summary",
                                                           ""};
  /// \brief Reserved table property for current snapshot id.
  inline static Entry<std::string> kCurrentSnapshotId{"current-snapshot-id", ""};
  /// \brief Reserved table property for current snapshot timestamp.
  inline static Entry<std::string> kCurrentSnapshotTimestamp{
      "current-snapshot-timestamp-ms", ""};
  /// \brief Reserved table property for the JSON representation of current schema.
  inline static Entry<std::string> kCurrentSchema{"current-schema", ""};
  /// \brief Reserved table property for the JSON representation of current(default)
  /// partition spec.
  inline static Entry<std::string> kDefaultPartitionSpec{"default-partition-spec", ""};
  /// \brief Reserved table property for the JSON representation of current(default) sort
  /// order.
  inline static Entry<std::string> kDefaultSortOrder{"default-sort-order", ""};

  // Commit properties

  inline static Entry<int32_t> kCommitNumRetries{"commit.retry.num-retries", 4};
  inline static Entry<int32_t> kCommitMinRetryWaitMs{"commit.retry.min-wait-ms", 100};
  inline static Entry<int32_t> kCommitMaxRetryWaitMs{"commit.retry.max-wait-ms",
                                                     60 * 1000};  // 1 minute
  inline static Entry<int32_t> kCommitTotalRetryTimeMs{"commit.retry.total-timeout-ms",
                                                       30 * 60 * 1000};  // 30 minutes
  inline static Entry<int32_t> kCommitNumStatusChecks{"commit.status-check.num-retries",
                                                      3};
  inline static Entry<int64_t> kCommitStatusChecksMinWaitMs{
      "commit.status-check.min-wait-ms", int64_t{1000}};  // 1 second
  inline static Entry<int64_t> kCommitStatusChecksMaxWaitMs{
      "commit.status-check.max-wait-ms", int64_t{60 * 1000}};  // 1 minute
  inline static Entry<int64_t> kCommitStatusChecksTotalWaitMs{
      "commit.status-check.total-timeout-ms", int64_t{30 * 60 * 1000}};  // 30 minutes

  // Manifest properties

  inline static Entry<int64_t> kManifestTargetSizeBytes{
      "commit.manifest.target-size-bytes", int64_t{8 * 1024 * 1024}};  // 8 MB
  inline static Entry<int32_t> kManifestMinMergeCount{
      "commit.manifest.min-count-to-merge", 100};
  inline static Entry<bool> kManifestMergeEnabled{"commit.manifest-merge.enabled", true};

  // File format properties

  inline static Entry<std::string> kDefaultFileFormat{"write.format.default", "parquet"};
  inline static Entry<std::string> kDeleteDefaultFileFormat{"write.delete.format.default",
                                                            "parquet"};

  // Parquet properties

  inline static Entry<int32_t> kParquetRowGroupSizeBytes{
      "write.parquet.row-group-size-bytes", 128 * 1024 * 1024};  // 128 MB
  inline static Entry<int32_t> kDeleteParquetRowGroupSizeBytes{
      "write.delete.parquet.row-group-size-bytes", 128 * 1024 * 1024};  // 128 MB
  inline static Entry<int32_t> kParquetPageSizeBytes{"write.parquet.page-size-bytes",
                                                     1024 * 1024};  // 1 MB
  inline static Entry<int32_t> kDeleteParquetPageSizeBytes{
      "write.delete.parquet.page-size-bytes", 1024 * 1024};  // 1 MB
  inline static Entry<int32_t> kParquetPageRowLimit{"write.parquet.page-row-limit",
                                                    20'000};
  inline static Entry<int32_t> kDeleteParquetPageRowLimit{
      "write.delete.parquet.page-row-limit", 20'000};
  inline static Entry<int32_t> kParquetDictSizeBytes{"write.parquet.dict-size-bytes",
                                                     2 * 1024 * 1024};  // 2 MB
  inline static Entry<int32_t> kDeleteParquetDictSizeBytes{
      "write.delete.parquet.dict-size-bytes", 2 * 1024 * 1024};  // 2 MB
  inline static Entry<std::string> kParquetCompression{"write.parquet.compression-codec",
                                                       "zstd"};
  inline static Entry<std::string> kDeleteParquetCompression{
      "write.delete.parquet.compression-codec", "zstd"};
  inline static Entry<std::string> kParquetCompressionLevel{
      "write.parquet.compression-level", ""};
  inline static Entry<std::string> kDeleteParquetCompressionLevel{
      "write.delete.parquet.compression-level", ""};
  inline static Entry<int32_t> kParquetRowGroupCheckMinRecordCount{
      "write.parquet.row-group-check-min-record-count", 100};
  inline static Entry<int32_t> kDeleteParquetRowGroupCheckMinRecordCount{
      "write.delete.parquet.row-group-check-min-record-count", 100};
  inline static Entry<int32_t> kParquetRowGroupCheckMaxRecordCount{
      "write.parquet.row-group-check-max-record-count", 10'000};
  inline static Entry<int32_t> kDeleteParquetRowGroupCheckMaxRecordCount{
      "write.delete.parquet.row-group-check-max-record-count", 10'000};
  inline static Entry<int32_t> kParquetBloomFilterMaxBytes{
      "write.parquet.bloom-filter-max-bytes", 1024 * 1024};  // 1 MB
  inline static std::string_view kParquetBloomFilterColumnFppPrefix{
      "write.parquet.bloom-filter-fpp.column."};
  inline static std::string_view kParquetBloomFilterColumnEnabledPrefix{
      "write.parquet.bloom-filter-enabled.column."};
  inline static std::string_view kParquetColumnStatsEnabledPrefix{
      "write.parquet.stats-enabled.column."};

  // Avro properties
  inline static Entry<std::string> kAvroCompression{"write.avro.compression-codec",
                                                    "gzip"};
  inline static Entry<std::string> kDeleteAvroCompression{
      "write.delete.avro.compression-codec", "gzip"};
  inline static Entry<std::string> kAvroCompressionLevel{"write.avro.compression-level",
                                                         ""};
  inline static Entry<std::string> kDeleteAvroCompressionLevel{
      "write.delete.avro.compression-level", ""};

  // ORC properties
  inline static Entry<int64_t> kOrcStripeSizeBytes{"write.orc.stripe-size-bytes",
                                                   int64_t{64} * 1024 * 1024};
  inline static Entry<std::string> kOrcBloomFilterColumns{
      "write.orc.bloom.filter.columns", ""};
  inline static Entry<double> kOrcBloomFilterFpp{"write.orc.bloom.filter.fpp", 0.05};
  inline static Entry<int64_t> kDeleteOrcStripeSizeBytes{
      "write.delete.orc.stripe-size-bytes", int64_t{64} * 1024 * 1024};  // 64 MB
  inline static Entry<int64_t> kOrcBlockSizeBytes{"write.orc.block-size-bytes",
                                                  int64_t{256} * 1024 * 1024};  // 256 MB
  inline static Entry<int64_t> kDeleteOrcBlockSizeBytes{
      "write.delete.orc.block-size-bytes", int64_t{256} * 1024 * 1024};  // 256 MB
  inline static Entry<int32_t> kOrcWriteBatchSize{"write.orc.vectorized.batch-size",
                                                  1024};
  inline static Entry<int32_t> kDeleteOrcWriteBatchSize{
      "write.delete.orc.vectorized.batch-size", 1024};
  inline static Entry<std::string> kOrcCompression{"write.orc.compression-codec", "zlib"};
  inline static Entry<std::string> kDeleteOrcCompression{
      "write.delete.orc.compression-codec", "zlib"};
  inline static Entry<std::string> kOrcCompressionStrategy{
      "write.orc.compression-strategy", "speed"};
  inline static Entry<std::string> kDeleteOrcCompressionStrategy{
      "write.delete.orc.compression-strategy", "speed"};

  // Read properties

  inline static Entry<int64_t> kSplitSize{"read.split.target-size",
                                          int64_t{128} * 1024 * 1024};  // 128 MB
  inline static Entry<int64_t> kMetadataSplitSize{"read.split.metadata-target-size",
                                                  int64_t{32} * 1024 * 1024};  // 32 MB
  inline static Entry<int32_t> kSplitLookback{"read.split.planning-lookback", 10};
  inline static Entry<int64_t> kSplitOpenFileCost{"read.split.open-file-cost",
                                                  int64_t{4} * 1024 * 1024};  // 4 MB
  inline static Entry<bool> kAdaptiveSplitSizeEnabled{"read.split.adaptive-size.enabled",
                                                      true};
  inline static Entry<bool> kParquetVectorizationEnabled{
      "read.parquet.vectorization.enabled", true};
  inline static Entry<int32_t> kParquetBatchSize{"read.parquet.vectorization.batch-size",
                                                 5000};
  inline static Entry<bool> kOrcVectorizationEnabled{"read.orc.vectorization.enabled",
                                                     false};
  inline static Entry<int32_t> kOrcBatchSize{"read.orc.vectorization.batch-size", 5000};
  inline static Entry<std::string> kDataPlanningMode{"read.data-planning-mode", "auto"};
  inline static Entry<std::string> kDeletePlanningMode{"read.delete-planning-mode",
                                                       "auto"};

  // Write properties

  inline static Entry<bool> kObjectStoreEnabled{"write.object-storage.enabled", false};
  /// \brief Excludes the partition values in the path when set to true and object store
  /// is enabled.
  inline static Entry<bool> kWriteObjectStorePartitionedPaths{
      "write.object-storage.partitioned-paths", true};
  /// \brief This only applies to files written after this property is set. Files
  /// previously written aren't relocated to reflect this parameter. If not set, defaults
  /// to a "data" folder underneath the root path of the table.
  inline static Entry<std::string> kWriteDataLocation{"write.data.path", ""};
  /// \brief This only applies to files written after this property is set. Files
  /// previously written aren't relocated to reflect this parameter. If not set, defaults
  /// to a "metadata" folder underneath the root path of the table.
  inline static Entry<std::string> kWriteMetadataLocation{"write.metadata.path", ""};
  inline static Entry<int32_t> kWritePartitionSummaryLimit{
      "write.summary.partition-limit", 0};
  inline static Entry<std::string> kMetadataCompression{
      "write.metadata.compression-codec", "none"};
  inline static Entry<int32_t> kMetadataPreviousVersionsMax{
      "write.metadata.previous-versions-max", 100};
  /// \brief This enables to delete the oldest metadata file after commit.
  inline static Entry<bool> kMetadataDeleteAfterCommitEnabled{
      "write.metadata.delete-after-commit.enabled", false};
  inline static Entry<int32_t> kMetricsMaxInferredColumnDefaults{
      "write.metadata.metrics.max-inferred-column-defaults", 100};
  inline static constexpr std::string_view kMetricModeColumnConfPrefix =
      "write.metadata.metrics.column.";
  inline static Entry<std::string> kDefaultWriteMetricsMode{
      "write.metadata.metrics.default", "truncate(16)"};

  inline static std::string_view kDefaultNameMapping{"schema.name-mapping.default"};

  inline static Entry<bool> kWriteAuditPublishEnabled{"write.wap.enabled", false};
  inline static Entry<int64_t> kWriteTargetFileSizeBytes{
      "write.target-file-size-bytes", int64_t{512} * 1024 * 1024};  // 512 MB
  inline static Entry<int64_t> kDeleteTargetFileSizeBytes{
      "write.delete.target-file-size-bytes", int64_t{64} * 1024 * 1024};  // 64 MB

  // Garbage collection properties

  inline static Entry<bool> kGcEnabled{"gc.enabled", true};
  inline static Entry<int64_t> kMaxSnapshotAgeMs{
      "history.expire.max-snapshot-age-ms", int64_t{5} * 24 * 60 * 60 * 1000};  // 5 days
  inline static Entry<int32_t> kMinSnapshotsToKeep{"history.expire.min-snapshots-to-keep",
                                                   1};
  inline static Entry<int64_t> kMaxRefAgeMs{"history.expire.max-ref-age-ms",
                                            std::numeric_limits<int64_t>::max()};

  // Delete/Update/Merge properties

  inline static Entry<std::string> kDeleteGranularity{"write.delete.granularity",
                                                      "partition"};
  inline static Entry<std::string> kDeleteIsolationLevel{"write.delete.isolation-level",
                                                         "serializable"};
  inline static Entry<std::string> kDeleteMode{"write.delete.mode", "copy-on-write"};

  inline static Entry<std::string> kUpdateIsolationLevel{"write.update.isolation-level",
                                                         "serializable"};
  inline static Entry<std::string> kUpdateMode{"write.update.mode", "copy-on-write"};

  inline static Entry<std::string> kMergeIsolationLevel{"write.merge.isolation-level",
                                                        "serializable"};
  inline static Entry<std::string> kMergeMode{"write.merge.mode", "copy-on-write"};

  inline static Entry<bool> kUpsertEnabled{"write.upsert.enabled", false};

  // Encryption properties

  inline static Entry<std::string> kEncryptionTableKey{"encryption.key-id", ""};
  inline static Entry<int32_t> kEncryptionDekLength{"encryption.data-key-length", 16};

  /// \brief Get the set of reserved table property keys.
  ///
  /// Reserved table properties are only used to control behaviors when creating
  /// or updating a table. The values of these properties are not persisted as
  /// part of the table metadata.
  ///
  /// \return The set of reserved property keys
  static const std::unordered_set<std::string>& reserved_properties();

  /// \brief Create a default TableProperties instance.
  ///
  /// \return A unique pointer to a TableProperties instance with default values
  static std::unique_ptr<TableProperties> default_properties();

  /// \brief Create a TableProperties instance from a map of key-value pairs.
  ///
  /// \param properties The map containing property key-value pairs
  /// \return A unique pointer to a TableProperties instance
  static std::unique_ptr<TableProperties> FromMap(
      std::unordered_map<std::string, std::string> properties);

 private:
  TableProperties() = default;
};

}  // namespace iceberg
