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

#define STRING_ENTRY(name, key, value) inline static Entry<std::string> name{key, value};
#define INT32_ENTRY(name, key, value) inline static Entry<int32_t> name{key, value};
#define INT64_ENTRY(name, key, value) inline static Entry<int64_t> name{key, value};
#define BOOL_ENTRY(name, key, value) inline static Entry<bool> name{key, value};
#define DOUBLE_ENTRY(name, key, value) inline static Entry<double> name{key, value};

  // Reserved table properties
  STRING_ENTRY(kFormatVersion, "format-version", "");
  STRING_ENTRY(kUuid, "uuid", "");
  STRING_ENTRY(kSnapshotCount, "snapshot-count", "");
  STRING_ENTRY(kCurrentSnapshotSummary, "current-snapshot-summary", "");
  STRING_ENTRY(kCurrentSnapshotId, "current-snapshot-id", "");
  STRING_ENTRY(kCurrentSnapshotTimestamp, "current-snapshot-timestamp-ms", "");
  STRING_ENTRY(kCurrentSchema, "current-schema", "");
  STRING_ENTRY(kDefaultPartitionSpec, "default-partition-spec", "");
  STRING_ENTRY(kDefaultSortOrder, "default-sort-order", "");

  // Commit properties
  INT32_ENTRY(kCommitNumRetries, "commit.retry.num-retries", 4);
  INT32_ENTRY(kCommitMinRetryWaitMs, "commit.retry.min-wait-ms", 100);
  INT32_ENTRY(kCommitMaxRetryWaitMs, "commit.retry.max-wait-ms", 60 * 1000);
  INT32_ENTRY(kCommitTotalRetryTimeMs, "commit.retry.total-timeout-ms", 30 * 60 * 1000);
  INT32_ENTRY(kCommitNumStatusChecks, "commit.status-check.num-retries", 3);
  INT64_ENTRY(kCommitStatusChecksMinWaitMs, "commit.status-check.min-wait-ms", 1000);
  INT64_ENTRY(kCommitStatusChecksMaxWaitMs, "commit.status-check.max-wait-ms", 60 * 1000);
  INT64_ENTRY(kCommitStatusChecksTotalWaitMs, "commit.status-check.total-timeout-ms",
              30 * 60 * 1000);

  // Manifest properties
  INT64_ENTRY(kManifestTargetSizeBytes, "commit.manifest.target-size-bytes",
              8 * 1024 * 1024);
  INT32_ENTRY(kManifestMinMergeCount, "commit.manifest.min-count-to-merge", 100);
  BOOL_ENTRY(kManifestMergeEnabled, "commit.manifest-merge.enabled", true);

  // File format properties
  STRING_ENTRY(kDefaultFileFormat, "write.format.default", "parquet");
  STRING_ENTRY(kDeleteDefaultFileFormat, "write.delete.format.default", "parquet");

  // Parquet properties
  INT32_ENTRY(kParquetRowGroupSizeBytes, "write.parquet.row-group-size-bytes",
              128 * 1024 * 1024);
  INT32_ENTRY(kDeleteParquetRowGroupSizeBytes,
              "write.delete.parquet.row-group-size-bytes", 128 * 1024 * 1024);
  INT32_ENTRY(kParquetPageSizeBytes, "write.parquet.page-size-bytes", 1024 * 1024);
  INT32_ENTRY(kDeleteParquetPageSizeBytes, "write.delete.parquet.page-size-bytes",
              1024 * 1024);
  INT32_ENTRY(kParquetPageRowLimit, "write.parquet.page-row-limit", 20000);
  INT32_ENTRY(kDeleteParquetPageRowLimit, "write.delete.parquet.page-row-limit", 20000);
  INT32_ENTRY(kParquetDictSizeBytes, "write.parquet.dict-size-bytes", 2 * 1024 * 1024);
  INT32_ENTRY(kDeleteParquetDictSizeBytes, "write.delete.parquet.dict-size-bytes",
              2 * 1024 * 1024);
  STRING_ENTRY(kParquetCompression, "write.parquet.compression-codec", "zstd");
  STRING_ENTRY(kDeleteParquetCompression, "write.delete.parquet.compression-codec",
               "zstd");
  STRING_ENTRY(kParquetCompressionLevel, "write.parquet.compression-level", "");
  STRING_ENTRY(kDeleteParquetCompressionLevel, "write.delete.parquet.compression-level",
               "");
  INT32_ENTRY(kParquetRowGroupCheckMinRecordCount,
              "write.parquet.row-group-check-min-record-count", 100);
  INT32_ENTRY(kDeleteParquetRowGroupCheckMinRecordCount,
              "write.delete.parquet.row-group-check-min-record-count", 100);
  INT32_ENTRY(kParquetRowGroupCheckMaxRecordCount,
              "write.parquet.row-group-check-max-record-count", 10000);
  INT32_ENTRY(kDeleteParquetRowGroupCheckMaxRecordCount,
              "write.delete.parquet.row-group-check-max-record-count", 10000);
  INT32_ENTRY(kParquetBloomFilterMaxBytes, "write.parquet.bloom-filter-max-bytes",
              1024 * 1024);
  DOUBLE_ENTRY(kParquetBloomFilterColumnFppDefault,
               "write.parquet.bloom-filter-fpp.column", 0.01);

  // Avro properties
  STRING_ENTRY(kAvroCompression, "write.avro.compression-codec", "gzip");
  STRING_ENTRY(kDeleteAvroCompression, "write.delete.avro.compression-codec", "gzip");
  STRING_ENTRY(kAvroCompressionLevel, "write.avro.compression-level", "");
  STRING_ENTRY(kDeleteAvroCompressionLevel, "write.delete.avro.compression-level", "");

  // ORC properties
  INT64_ENTRY(kOrcStripeSizeBytes, "write.orc.stripe-size-bytes", 64L * 1024 * 1024);
  STRING_ENTRY(kOrcBloomFilterColumns, "write.orc.bloom.filter.columns", "");
  DOUBLE_ENTRY(kOrcBloomFilterFpp, "write.orc.bloom.filter.fpp", 0.05);
  INT64_ENTRY(kDeleteOrcStripeSizeBytes, "write.delete.orc.stripe-size-bytes",
              64L * 1024 * 1024);
  INT64_ENTRY(kOrcBlockSizeBytes, "write.orc.block-size-bytes", 256L * 1024 * 1024);
  INT64_ENTRY(kDeleteOrcBlockSizeBytes, "write.delete.orc.block-size-bytes",
              256L * 1024 * 1024);
  INT32_ENTRY(kOrcWriteBatchSize, "write.orc.vectorized.batch-size", 1024);
  INT32_ENTRY(kDeleteOrcWriteBatchSize, "write.delete.orc.vectorized.batch-size", 1024);
  STRING_ENTRY(kOrcCompression, "write.orc.compression-codec", "zlib");
  STRING_ENTRY(kDeleteOrcCompression, "write.delete.orc.compression-codec", "zlib");
  STRING_ENTRY(kOrcCompressionStrategy, "write.orc.compression-strategy", "speed");
  STRING_ENTRY(kDeleteOrcCompressionStrategy, "write.delete.orc.compression-strategy",
               "speed");

  // Read properties
  INT64_ENTRY(kSplitSize, "read.split.target-size", 128 * 1024 * 1024);
  INT64_ENTRY(kMetadataSplitSize, "read.split.metadata-target-size", 32 * 1024 * 1024);
  INT32_ENTRY(kSplitLookback, "read.split.planning-lookback", 10);
  INT64_ENTRY(kSplitOpenFileCost, "read.split.open-file-cost", 4 * 1024 * 1024);
  BOOL_ENTRY(kAdaptiveSplitSizeEnabled, "read.split.adaptive-size.enabled", true);
  BOOL_ENTRY(kParquetVectorizationEnabled, "read.parquet.vectorization.enabled", true);
  INT32_ENTRY(kParquetBatchSize, "read.parquet.vectorization.batch-size", 5000);
  BOOL_ENTRY(kOrcVectorizationEnabled, "read.orc.vectorization.enabled", false);
  INT32_ENTRY(kOrcBatchSize, "read.orc.vectorization.batch-size", 5000);
  STRING_ENTRY(kDataPlanningMode, "read.data-planning-mode", "auto");
  STRING_ENTRY(kDeletePlanningMode, "read.delete-planning-mode", "auto");

  // Write properties
  BOOL_ENTRY(kObjectStoreEnabled, "write.object-storage.enabled", false);
  BOOL_ENTRY(kWriteObjectStorePartitionedPaths, "write.object-storage.partitioned-paths",
             true);
  STRING_ENTRY(kObjectStorePath, "write.object-storage.path", "");
  STRING_ENTRY(kWriteLocationProviderImpl, "write.location-provider.impl", "");
  STRING_ENTRY(kWriteFolderStorageLocation, "write.folder-storage.path", "");
  STRING_ENTRY(kWriteDataLocation, "write.data.path", "");
  STRING_ENTRY(kWriteMetadataLocation, "write.metadata.path", "");
  INT32_ENTRY(kWritePartitionSummaryLimit, "write.summary.partition-limit", 0);
  BOOL_ENTRY(kManifestListsEnabled, "write.manifest-lists.enabled", true);
  STRING_ENTRY(kMetadataCompression, "write.metadata.compression-codec", "none");
  INT32_ENTRY(kMetadataPreviousVersionsMax, "write.metadata.previous-versions-max", 100);
  BOOL_ENTRY(kMetadataDeleteAfterCommitEnabled,
             "write.metadata.delete-after-commit.enabled", false);
  INT32_ENTRY(kMetricsMaxInferredColumnDefaults,
              "write.metadata.metrics.max-inferred-column-defaults", 100);
  STRING_ENTRY(kDefaultWriteMetricsMode, "write.metadata.metrics.default",
               "truncate(16)");
  STRING_ENTRY(kDefaultNameMapping, "schema.name-mapping.default", "");
  STRING_ENTRY(kWriteAuditPublishEnabled, "write.wap.enabled", "false");
  INT64_ENTRY(kWriteTargetFileSizeBytes, "write.target-file-size-bytes",
              512 * 1024 * 1024);
  INT64_ENTRY(kDeleteTargetFileSizeBytes, "write.delete.target-file-size-bytes",
              64 * 1024 * 1024);
  BOOL_ENTRY(kSparkWritePartitionedFanoutEnabled, "write.spark.fanout.enabled", false);
  BOOL_ENTRY(kSparkWriteAcceptAnySchema, "write.spark.accept-any-schema", false);
  STRING_ENTRY(kSparkWriteAdvisoryPartitionSizeBytes,
               "write.spark.advisory-partition-size-bytes", "");
  BOOL_ENTRY(kSnapshotIdInheritanceEnabled,
             "compatibility.snapshot-id-inheritance.enabled", false);
  BOOL_ENTRY(kEngineHiveEnabled, "engine.hive.enabled", false);
  BOOL_ENTRY(kHiveLockEnabled, "engine.hive.lock-enabled", true);
  STRING_ENTRY(kWriteDistributionMode, "write.distribution-mode", "");

  // Garbage collection properties
  BOOL_ENTRY(kGcEnabled, "gc.enabled", true);
  INT64_ENTRY(kMaxSnapshotAgeMs, "history.expire.max-snapshot-age-ms",
              5 * 24 * 60 * 60 * 1000L);
  INT32_ENTRY(kMinSnapshotsToKeep, "history.expire.min-snapshots-to-keep", 1);
  INT64_ENTRY(kMaxRefAgeMs, "history.expire.max-ref-age-ms",
              std::numeric_limits<int64_t>::max());

  // Delete/Update/Merge properties
  STRING_ENTRY(kDeleteGranularity, "write.delete.granularity", "partition");
  STRING_ENTRY(kDeleteIsolationLevel, "write.delete.isolation-level", "serializable");
  STRING_ENTRY(kDeleteMode, "write.delete.mode", "copy-on-write");
  STRING_ENTRY(kDeleteDistributionMode, "write.delete.distribution-mode", "");
  STRING_ENTRY(kUpdateIsolationLevel, "write.update.isolation-level", "serializable");
  STRING_ENTRY(kUpdateMode, "write.update.mode", "copy-on-write");
  STRING_ENTRY(kUpdateDistributionMode, "write.update.distribution-mode", "");
  STRING_ENTRY(kMergeIsolationLevel, "write.merge.isolation-level", "serializable");
  STRING_ENTRY(kMergeMode, "write.merge.mode", "copy-on-write");
  STRING_ENTRY(kMergeDistributionMode, "write.merge.distribution-mode", "");
  BOOL_ENTRY(kUpsertEnabled, "write.upsert.enabled", false);

  // Encryption properties
  STRING_ENTRY(kEncryptionTableKey, "encryption.key-id", "");
  INT32_ENTRY(kEncryptionDekLength, "encryption.data-key-length", 16);

#undef STRING_ENTRY
#undef INT32_ENTRY
#undef INT64_ENTRY
#undef BOOL_ENTRY
#undef DOUBLE_ENTRY

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
      const std::unordered_map<std::string, std::string>& properties);
};

}  // namespace iceberg
