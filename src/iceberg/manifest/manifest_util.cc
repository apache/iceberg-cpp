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

#include <memory>
#include <optional>

#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/util/macros.h"

namespace iceberg {

Result<ManifestFile> CopyAppendManifest(
    const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io,
    const std::shared_ptr<Schema>& schema, const std::shared_ptr<PartitionSpec>& spec,
    int64_t snapshot_id, const std::string& output_path, int8_t format_version,
    SnapshotSummaryBuilder* summary_builder) {
  // use metadata that will add the current snapshot's ID for the rewrite
  // read first_row_id as null because this copies the incoming manifest before commit
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata,
                          InheritableMetadataFactory::ForCopy(snapshot_id));
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader,
      ManifestReader::Make(manifest.manifest_path, manifest.manifest_length, file_io,
                           schema, spec, std::move(inheritable_metadata),
                           /*first_row_id=*/std::nullopt));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

  // do not produce row IDs for the copy
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, ManifestWriter::MakeWriter(
                       format_version, snapshot_id, output_path, file_io, spec, schema,
                       ManifestContent::kData, /*first_row_id=*/std::nullopt));

  for (auto& entry : entries) {
    ICEBERG_CHECK(entry.status == ManifestStatus::kAdded,
                  "Manifest to copy must only contain added entries");
    if (summary_builder != nullptr && entry.data_file != nullptr) {
      ICEBERG_RETURN_UNEXPECTED(summary_builder->AddedFile(*spec, *entry.data_file));
    }

    ICEBERG_RETURN_UNEXPECTED(writer->WriteAddedEntry(entry));
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

}  // namespace iceberg
