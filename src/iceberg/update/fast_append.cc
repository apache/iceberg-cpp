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

#include <iterator>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"  // IWYU pragma: keep
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/transaction.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<std::shared_ptr<FastAppend>> FastAppend::Make(
    std::string table_name, std::shared_ptr<TransactionContext> ctx) {
  ICEBERG_PRECHECK(!table_name.empty(), "Table name cannot be empty");
  ICEBERG_PRECHECK(ctx != nullptr, "Cannot create FastAppend without a context");
  return std::shared_ptr<FastAppend>(
      new FastAppend(std::move(table_name), std::move(ctx)));
}

FastAppend::FastAppend(std::string table_name, std::shared_ptr<TransactionContext> ctx)
    : SnapshotUpdate(std::move(ctx)), table_name_(std::move(table_name)) {}

FastAppend& FastAppend::AppendFile(const std::shared_ptr<DataFile>& file) {
  ICEBERG_BUILDER_CHECK(file != nullptr, "Invalid data file: null");
  ICEBERG_BUILDER_CHECK(file->partition_spec_id.has_value(),
                        "Data file must have partition spec ID");

  int32_t spec_id = file->partition_spec_id.value();
  ICEBERG_BUILDER_ASSIGN_OR_RETURN(auto spec, Spec(spec_id));

  auto& data_files = new_data_files_by_spec_[spec_id];
  auto [iter, inserted] = data_files.insert(file);
  if (inserted) {
    has_new_files_ = true;
    ICEBERG_BUILDER_RETURN_IF_ERROR(added_data_files_summary_.AddedFile(*spec, *file));
  }

  return *this;
}

FastAppend& FastAppend::AppendManifest(const ManifestFile& manifest) {
  ICEBERG_BUILDER_CHECK(!manifest.has_existing_files(),
                        "Cannot append manifest with existing files");
  ICEBERG_BUILDER_CHECK(!manifest.has_deleted_files(),
                        "Cannot append manifest with deleted files");
  ICEBERG_BUILDER_CHECK(!manifest.added_snapshot_id.has_value() ||
                            manifest.added_snapshot_id == kInvalidSnapshotId,
                        "Snapshot id must be assigned during commit");
  ICEBERG_BUILDER_CHECK(manifest.sequence_number == kInvalidSequenceNumber,
                        "Sequence number must be assigned during commit");

  // Only a missing ID is inheritance-ready; an explicit -1 must be rewritten.
  if (can_inherit_snapshot_id() && !manifest.added_snapshot_id.has_value()) {
    append_manifests_.push_back(manifest);
  } else {
    // The manifest must be rewritten with this update's snapshot ID
    append_manifests_to_copy_.push_back(manifest);
  }

  return *this;
}

std::string FastAppend::operation() { return DataOperation::kAppend; }

Result<std::vector<ManifestFile>> FastAppend::Apply(
    const TableMetadata& metadata_to_update, const std::shared_ptr<Snapshot>& snapshot) {
  std::vector<ManifestFile> manifests;
  appended_manifests_summary_.Clear();
  for (const auto& manifest : append_manifests_) {
    appended_manifests_summary_.AddedManifest(manifest);
  }

  ICEBERG_ASSIGN_OR_RAISE(auto new_written_manifests, WriteNewManifests());
  // Immutable copies and their summaries are reused across replay attempts.
  for (size_t i = rewritten_append_manifests_.size();
       i < append_manifests_to_copy_.size(); ++i) {
    ICEBERG_ASSIGN_OR_RAISE(auto copied_manifest,
                            CopyManifest(append_manifests_to_copy_[i]));
    rewritten_append_manifests_.push_back(std::move(copied_manifest));
  }

  manifests.reserve(new_written_manifests.size() + append_manifests_.size() +
                    rewritten_append_manifests_.size());
  if (!new_written_manifests.empty()) {
    manifests.insert(manifests.end(),
                     std::make_move_iterator(new_written_manifests.begin()),
                     std::make_move_iterator(new_written_manifests.end()));
  }

  // Transform append manifests and rewritten append manifests with snapshot ID
  int64_t snapshot_id = SnapshotId();
  for (auto& manifest : append_manifests_) {
    manifest.added_snapshot_id = snapshot_id;
  }
  for (auto& manifest : rewritten_append_manifests_) {
    manifest.added_snapshot_id = snapshot_id;
  }
  manifests.insert(manifests.end(), append_manifests_.begin(), append_manifests_.end());
  manifests.insert(manifests.end(), rewritten_append_manifests_.begin(),
                   rewritten_append_manifests_.end());

  // Add all manifests from the snapshot
  if (snapshot != nullptr) {
    auto snapshot_reader = SnapshotReader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests,
                            snapshot_reader.Manifests(ctx_->table->io()));
    manifests.insert(manifests.end(), snapshot_manifests.begin(),
                     snapshot_manifests.end());
  }

  manifest_count_summary_ =
      BuildManifestCountSummary(manifests, /*replaced_manifests_count=*/0);

  return manifests;
}

std::unordered_map<std::string, std::string> FastAppend::Summary() {
  summary_.Clear();
  summary_.SetPartitionSummaryLimit(
      base().properties.Get(TableProperties::kWritePartitionSummaryLimit));
  summary_.Merge(added_data_files_summary_);
  summary_.Merge(appended_manifests_summary_);
  summary_.Merge(copied_manifests_summary_);
  for (const auto& [property, value] : custom_summary_properties_) {
    summary_.Set(property, value);
  }
  summary_.Merge(manifest_count_summary_);
  return summary_.Build();
}

void FastAppend::SetSummaryProperty(const std::string& property,
                                    const std::string& value) {
  custom_summary_properties_[property] = value;
  SnapshotUpdate::SetSummaryProperty(property, value);
}

Status FastAppend::CleanUncommitted(
    const std::unordered_set<std::string>& /*committed*/) {
  new_manifests_.clear();
  rewritten_append_manifests_.clear();
  copied_manifests_summary_.Clear();
  return {};
}

Result<std::shared_ptr<PartitionSpec>> FastAppend::Spec(int32_t spec_id) {
  return base().PartitionSpecById(spec_id);
}

Result<ManifestFile> FastAppend::CopyManifest(const ManifestFile& manifest) {
  const TableMetadata& current = base();
  ICEBERG_ASSIGN_OR_RAISE(auto schema, current.Schema());
  ICEBERG_ASSIGN_OR_RAISE(auto spec,
                          current.PartitionSpecById(manifest.partition_spec_id));

  // Generate a unique manifest path using the transaction's metadata location
  std::string new_manifest_path = ManifestPath();
  int64_t snapshot_id = SnapshotId();

  // A failed copy must not leave partial counts in the cached summary.
  SnapshotSummaryBuilder summary;
  ICEBERG_ASSIGN_OR_RAISE(
      auto copied,
      CopyAppendManifest(manifest, ctx_->table->io(), schema, spec, snapshot_id,
                         new_manifest_path, current.format_version, &summary));
  copied_manifests_summary_.Merge(summary);
  return copied;
}

Result<std::vector<ManifestFile>> FastAppend::WriteNewManifests() {
  // New additions invalidate the cache. Old paths stay staged until cleanup.
  if (has_new_files_ && !new_manifests_.empty()) {
    new_manifests_.clear();
  }

  // Write new manifests if there are new data files
  if (new_manifests_.empty() && !new_data_files_by_spec_.empty()) {
    for (const auto& [spec_id, data_files] : new_data_files_by_spec_) {
      ICEBERG_ASSIGN_OR_RAISE(auto spec, Spec(spec_id));
      ICEBERG_ASSIGN_OR_RAISE(auto written_manifests,
                              WriteDataManifests(data_files.as_span(), spec));
      new_manifests_.insert(new_manifests_.end(),
                            std::make_move_iterator(written_manifests.begin()),
                            std::make_move_iterator(written_manifests.end()));
    }
    has_new_files_ = false;
  }

  return new_manifests_;
}

}  // namespace iceberg
