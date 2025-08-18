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

/// \file iceberg/v2_metadata.h

#include "iceberg/manifest_entry.h"
#include "iceberg/manifest_list.h"

namespace iceberg {

/// \brief v2 metadata wrapper.
///
/// Wrapper for v2 manifest list and manifest entry.
class V2MetaData {
 public:
  /// \brief v2 manifest file wrapper.
  struct ManifestFileWrapper : public ManifestFile {
    explicit ManifestFileWrapper(int64_t commit_snapshotId, int64_t sequence_number) {}

    ManifestFile Wrap(ManifestFile file) { return *this; }
  };

  /// \brief v2 manifest entry wrapper.
  struct ManifestEntryWrapper : public ManifestEntry {
    explicit ManifestEntryWrapper(int64_t commit_snapshot_id) {}

    ManifestEntry Wrap(ManifestEntry entry) { return *this; }
  };

  static ManifestFileWrapper manifestFileWrapper(int64_t commit_snapshotId,
                                                 int64_t sequence_number) {
    return ManifestFileWrapper(commit_snapshotId, sequence_number);
  }

  static ManifestEntryWrapper manifestEntryWrapper(int64_t commit_snapshot_id) {
    return ManifestEntryWrapper(commit_snapshot_id);
  }
};

}  // namespace iceberg
