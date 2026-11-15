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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/inspect/metadata_manifest_util_internal.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/scan_test_base.h"
#include "iceberg/util/timepoint.h"

namespace iceberg::internal {
namespace {
class MetadataManifestTest : public ScanTestBase {
 protected:
  std::shared_ptr<Snapshot> SnapshotWith(std::vector<ManifestFile> manifests) {
    return std::make_shared<Snapshot>(
        Snapshot{.snapshot_id = 10,
                 .sequence_number = 1,
                 .timestamp_ms = TimePointMsFromUnixMs(1000),
                 .manifest_list = WriteManifestList(GetParam(), 10, 0, 1, manifests)});
  }
};

TEST_P(MetadataManifestTest, ReadsLiveFilesAcrossManifestsAndRetainsInputs) {
  auto first = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("first.parquet")),
       MakeEntry(ManifestStatus::kDeleted, 10, 1, MakeDataFile("deleted.parquet"))});
  auto second = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("second.parquet"))});
  auto snapshot = SnapshotWith({first, second});
  auto metadata = MakeTableMetadata({snapshot}, 10);
  ICEBERG_UNWRAP_OR_FAIL(auto files, LiveFiles(*metadata, file_io_, snapshot));
  metadata.reset();
  snapshot.reset();
  file_io_.reset();
  for (const auto& path : {"first.parquet", "second.parquet"}) {
    ICEBERG_UNWRAP_OR_FAIL(auto row, files->Next());
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->file->file_path, path);
    ASSERT_NE(row->spec, nullptr);
  }
  for (int attempt = 0; attempt < 2; ++attempt) {
    ICEBERG_UNWRAP_OR_FAIL(auto row, files->Next());
    EXPECT_FALSE(row.has_value());
  }
}

TEST_P(MetadataManifestTest, DefersManifestReadsAndKeepsFailuresTerminal) {
  auto first = WriteDataManifest(
      GetParam(), 10,
      {MakeEntry(ManifestStatus::kAdded, 10, 1, MakeDataFile("first.parquet"))});
  auto missing = first;
  missing.manifest_path = "missing-manifest.avro";
  auto snapshot = SnapshotWith({first, missing});
  auto metadata = MakeTableMetadata({snapshot}, 10);
  ICEBERG_UNWRAP_OR_FAIL(auto files, LiveFiles(*metadata, file_io_, snapshot));
  ICEBERG_UNWRAP_OR_FAIL(auto row, files->Next());
  ASSERT_TRUE(row.has_value());
  EXPECT_EQ(row->file->file_path, "first.parquet");
  for (int attempt = 0; attempt < 2; ++attempt) {
    EXPECT_FALSE(files->Next().has_value());
  }
}

TEST_P(MetadataManifestTest, EmptyTablesHaveNoLiveFiles) {
  auto metadata = MakeTableMetadata({}, kInvalidSnapshotId);
  ICEBERG_UNWRAP_OR_FAIL(auto files, LiveFiles(*metadata, file_io_, nullptr));
  ICEBERG_UNWRAP_OR_FAIL(auto row, files->Next());
  EXPECT_FALSE(row.has_value());
}

INSTANTIATE_TEST_SUITE_P(FormatVersions, MetadataManifestTest,
                         ::testing::Values(1, 2, 3));
}  // namespace
}  // namespace iceberg::internal
