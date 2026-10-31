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
#include <vector>

#include "iceberg/file_reader.h"
#include "iceberg/inheritable_metadata.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/manifest/manifest_reader.h"
#include "iceberg/manifest/manifest_util_internal.h"
#include "iceberg/manifest/manifest_writer.h"
#include "iceberg/result.h"
#include "iceberg/schema.h"
#include "iceberg/snapshot.h"
#include "iceberg/table_metadata.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/string_util.h"

namespace iceberg {

Result<int8_t> ReadManifestFormatVersion(const ManifestFile& manifest,
                                         const std::shared_ptr<FileIO>& file_io) {
  ReaderOptions options{.path = manifest.manifest_path,
                        .length = manifest.manifest_length,
                        .io = file_io,
                        .projection = std::make_shared<Schema>(
                            std::vector<SchemaField>{ManifestEntry::kStatus})};
  ICEBERG_ASSIGN_OR_RAISE(auto reader,
                          ReaderFactoryRegistry::Open(FileFormatType::kAvro, options));
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, reader->Metadata());
  ICEBERG_RETURN_UNEXPECTED(reader->Close());
  auto version = metadata.find("format-version");
  if (version == metadata.end()) {
    return int8_t{1};
  }
  ICEBERG_ASSIGN_OR_RAISE(auto format_version,
                          StringUtils::ParseNumber<int8_t>(version->second));
  ICEBERG_CHECK(format_version >= 1 &&
                    format_version <= TableMetadata::kSupportedTableFormatVersion,
                "Unsupported manifest format version {} in {}", format_version,
                manifest.manifest_path);
  return format_version;
}

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

Result<ManifestFile> CopyRewriteManifest(
    const ManifestFile& manifest, const std::shared_ptr<FileIO>& file_io,
    const std::shared_ptr<Schema>& schema, const std::shared_ptr<PartitionSpec>& spec,
    int64_t snapshot_id, const std::string& output_path, int8_t format_version) {
  ICEBERG_ASSIGN_OR_RAISE(auto source_format_version,
                          ReadManifestFormatVersion(manifest, file_io));
  // Require explicit snapshot IDs and preserve them in the copied entries.
  ICEBERG_ASSIGN_OR_RAISE(auto inheritable_metadata, InheritableMetadataFactory::Empty());
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, ManifestReader::Make(
                       manifest.manifest_path, manifest.manifest_length, file_io, schema,
                       spec, std::move(inheritable_metadata), manifest.first_row_id,
                       /*is_committed=*/false));
  ICEBERG_ASSIGN_OR_RAISE(auto entries, reader->Entries());

  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      ManifestWriter::MakeWriter(format_version, snapshot_id, output_path, file_io, spec,
                                 schema, manifest.content, manifest.first_row_id));
  for (auto& entry : entries) {
    // A rewritten added manifest may only contain existing entries.
    if (entry.status == ManifestStatus::kAdded) {
      return ValidationFailed(
          "Invalid manifest entry status: ADDED (allowed status: EXISTING)");
    }
    if (entry.status == ManifestStatus::kDeleted) {
      return ValidationFailed(
          "Invalid manifest entry status: DELETED (allowed status: EXISTING)");
    }
    if (source_format_version == 1) {
      // V1 does not persist sequence numbers. Materialize its legacy defaults
      // before writing EXISTING entries into a v2 or v3 replacement manifest.
      entry.sequence_number = entry.sequence_number.value_or(0);
      entry.file_sequence_number = entry.file_sequence_number.value_or(0);
    }
    ICEBERG_RETURN_UNEXPECTED(writer->WriteExistingEntry(entry));
  }
  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  return writer->ToManifestFile();
}

}  // namespace iceberg
