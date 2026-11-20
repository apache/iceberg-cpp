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

#include "iceberg/inspect/metadata_manifest_util_internal.h"

#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "iceberg/constants.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/partition_spec.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

namespace {

class LiveFilesIterator : public Stream<LiveFile> {
 public:
  LiveFilesIterator(std::shared_ptr<FileIO> io, std::shared_ptr<Schema> schema,
                    std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs,
                    std::vector<ManifestFile> manifests, std::vector<std::string> columns)
      : io_(std::move(io)),
        schema_(std::move(schema)),
        specs_(std::move(specs)),
        manifests_(std::move(manifests)),
        columns_(std::move(columns)) {}

 protected:
  Result<std::optional<LiveFile>> NextImpl() override {
    while (entry_index_ == entries_.size()) {
      entries_.clear();
      entry_index_ = 0;
      if (manifest_index_ == manifests_.size()) {
        return std::nullopt;
      }
      const auto& manifest = manifests_[manifest_index_++];
      auto spec = specs_.find(manifest.partition_spec_id);
      ICEBERG_CHECK(spec != specs_.end(),
                    "Cannot find partition spec {} for manifest '{}'",
                    manifest.partition_spec_id, manifest.manifest_path);
      ICEBERG_PRECHECK(spec->second != nullptr, "Partition spec {} is null",
                       manifest.partition_spec_id);
      current_spec_ = spec->second;
      ICEBERG_ASSIGN_OR_RAISE(auto reader,
                              ManifestReader::Make(manifest, io_, schema_, specs_));
      if (!columns_.empty()) {
        reader->Select(columns_);
      }
      ICEBERG_ASSIGN_OR_RAISE(entries_, reader->LiveEntries());
    }
    auto& entry = entries_[entry_index_++];
    ICEBERG_PRECHECK(entry.data_file != nullptr,
                     "Manifest contains an entry with no data file");
    return LiveFile{.file = std::move(entry.data_file),
                    .spec = current_spec_,
                    .snapshot_id = entry.snapshot_id};
  }

 private:
  std::shared_ptr<FileIO> io_;
  std::shared_ptr<Schema> schema_;
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs_;
  std::vector<ManifestFile> manifests_;
  std::vector<std::string> columns_;
  std::vector<ManifestEntry> entries_;
  std::shared_ptr<PartitionSpec> current_spec_;
  size_t manifest_index_ = 0;
  size_t entry_index_ = 0;
};

}  // namespace

Result<std::unique_ptr<Stream<LiveFile>>> LiveFiles(
    const TableMetadata& metadata, const std::shared_ptr<FileIO>& io,
    const std::shared_ptr<Snapshot>& snapshot, std::vector<std::string> columns) {
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata.Schema());
  std::unordered_map<int32_t, std::shared_ptr<PartitionSpec>> specs;
  for (const auto& spec : metadata.partition_specs) {
    ICEBERG_PRECHECK(spec != nullptr, "Partition spec cannot be null");
    specs.emplace(spec->spec_id(), spec);
  }
  std::vector<ManifestFile> manifests;
  if (snapshot != nullptr) {
    SnapshotReader snapshot_reader(snapshot.get());
    ICEBERG_ASSIGN_OR_RAISE(auto snapshot_manifests, snapshot_reader.Manifests(io));
    manifests.assign(snapshot_manifests.begin(), snapshot_manifests.end());
  }
  return std::make_unique<LiveFilesIterator>(io, std::move(schema), std::move(specs),
                                             std::move(manifests), std::move(columns));
}

}  // namespace iceberg::internal
