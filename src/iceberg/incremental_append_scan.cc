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

#include "iceberg/incremental_append_scan.h"

#include <algorithm>
#include <iterator>
#include <unordered_set>
#include <utility>

#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_group.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/snapshot_util.h"

namespace iceberg {

Result<std::unique_ptr<IncrementalAppendScan>> IncrementalAppendScan::Make(
    std::shared_ptr<TableMetadata> metadata, std::shared_ptr<Schema> schema,
    std::shared_ptr<FileIO> io, internal::TableScanContext context) {
  ICEBERG_PRECHECK(metadata != nullptr, "Table metadata cannot be null");
  ICEBERG_PRECHECK(schema != nullptr, "Schema cannot be null");
  ICEBERG_PRECHECK(io != nullptr, "FileIO cannot be null");
  return std::unique_ptr<IncrementalAppendScan>(new IncrementalAppendScan(
      std::move(metadata), std::move(schema), std::move(io), std::move(context)));
}

Result<std::vector<std::shared_ptr<FileScanTask>>> IncrementalAppendScan::PlanFiles()
    const {
  return ResolvePlanFiles<FileScanTask>(*this);
}

Result<std::vector<std::shared_ptr<FileScanTask>>> IncrementalAppendScan::PlanFiles(
    std::optional<int64_t> from_snapshot_id_exclusive,
    int64_t to_snapshot_id_inclusive) const {
  ICEBERG_ASSIGN_OR_RAISE(
      auto ancestors_snapshots,
      SnapshotUtil::AncestorsBetween(*metadata_, to_snapshot_id_inclusive,
                                     from_snapshot_id_exclusive));

  std::vector<std::shared_ptr<Snapshot>> append_snapshots;
  std::ranges::copy_if(ancestors_snapshots, std::back_inserter(append_snapshots),
                       [](const auto& snapshot) {
                         return snapshot != nullptr &&
                                snapshot->Operation().has_value() &&
                                snapshot->Operation().value() == DataOperation::kAppend;
                       });
  if (append_snapshots.empty()) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  std::unordered_set<int64_t> snapshot_ids;
  std::ranges::transform(append_snapshots,
                         std::inserter(snapshot_ids, snapshot_ids.end()),
                         [](const auto& snapshot) { return snapshot->snapshot_id; });

  std::unordered_set<ManifestFile> data_manifests;
  for (const auto& snapshot : append_snapshots) {
    SnapshotReader snapshot_reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto manifests, snapshot_reader.DataManifests(io_));
    std::ranges::copy_if(
        manifests, std::inserter(data_manifests, data_manifests.end()),
        [&snapshot_ids](const ManifestFile& manifest) {
          return manifest.added_snapshot_id.has_value() &&
                 snapshot_ids.contains(manifest.added_snapshot_id.value());
        });
  }
  if (data_manifests.empty()) {
    return std::vector<std::shared_ptr<FileScanTask>>{};
  }

  TableMetadataCache metadata_cache(metadata_.get());
  ICEBERG_ASSIGN_OR_RAISE(auto specs_by_id, metadata_cache.GetPartitionSpecsById());

  ICEBERG_ASSIGN_OR_RAISE(
      auto manifest_group,
      ManifestGroup::Make(
          io_, schema_, specs_by_id,
          std::vector<ManifestFile>(data_manifests.begin(), data_manifests.end()), {}));

  manifest_group->CaseSensitive(context_.case_sensitive)
      .Select(ScanColumns())
      .FilterData(filter())
      .FilterManifestEntries([&snapshot_ids](const ManifestEntry& entry) {
        return entry.snapshot_id.has_value() &&
               snapshot_ids.contains(entry.snapshot_id.value()) &&
               entry.status == ManifestStatus::kAdded;
      })
      .IgnoreDeleted()
      .ColumnsToKeepStats(context_.columns_to_keep_stats)
      .PlanWith(context_.plan_executor);

  if (context_.ignore_residuals) {
    manifest_group->IgnoreResiduals();
  }

  return std::move(*manifest_group).PlanFiles();
}

}  // namespace iceberg
