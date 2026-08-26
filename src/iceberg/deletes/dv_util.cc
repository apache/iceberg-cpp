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

#include <cstddef>
#include <cstdint>
#include <format>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/deletes/dv_util_internal.h"
#include "iceberg/deletes/dv_writer.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/partition_spec.h"
#include "iceberg/puffin/file_metadata.h"
#include "iceberg/result.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"
#include "iceberg/version.h"

namespace iceberg {

Result<std::vector<std::shared_ptr<DataFile>>> DVUtil::MergeAndWriteDVs(
    std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
    const std::shared_ptr<FileIO>& io) {
  if (groups.empty()) {
    return std::vector<std::shared_ptr<DataFile>>{};
  }

  ICEBERG_ASSIGN_OR_RAISE(
      auto writer,
      DVWriter::Make(DVWriterOptions{
          .path = std::string(output_path),
          .io = io,
          .load_previous_deletes = [](std::string_view)
              -> Result<std::optional<PositionDeleteIndex>> { return std::nullopt; }}));

  for (const auto& group : groups) {
    PositionDeleteIndex merged;
    for (const auto& delete_file : group.delete_files) {
      ICEBERG_ASSIGN_OR_RAISE(auto index, DVUtil::ReadDV(delete_file, io));
      merged.Merge(index);
    }
    ICEBERG_RETURN_UNEXPECTED(
        writer->Delete(group.referenced_data_file, merged, group.spec, group.partition));
  }

  ICEBERG_RETURN_UNEXPECTED(writer->Close());
  ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
  return std::move(metadata.data_files);
}

Result<PositionDeleteIndex> DVUtil::ReadDV(const std::shared_ptr<DataFile>& delete_file,
                                           const std::shared_ptr<FileIO>& io) {
  ICEBERG_PRECHECK(delete_file != nullptr, "Delete file must not be null");
  ICEBERG_PRECHECK(io != nullptr, "DV read requires a FileIO");
  ICEBERG_PRECHECK(ContentFileUtil::IsDV(*delete_file),
                   "Cannot read, not a deletion vector: {}", delete_file->file_path);
  ICEBERG_PRECHECK(
      delete_file->content_offset.has_value() &&
          delete_file->content_size_in_bytes.has_value(),
      "Deletion vector requires content_offset and content_size_in_bytes: {}",
      delete_file->file_path);

  const int64_t offset = delete_file->content_offset.value();
  const int64_t length = delete_file->content_size_in_bytes.value();
  ICEBERG_PRECHECK(offset >= 0 && length >= 0,
                   "Invalid deletion vector offset/length: offset={}, length={}", offset,
                   length);
  ICEBERG_PRECHECK(length <= std::numeric_limits<int32_t>::max(),
                   "Cannot read deletion vector larger than 2GB: {}", length);

  ICEBERG_ASSIGN_OR_RAISE(auto input_file, io->NewInputFile(delete_file->file_path));
  ICEBERG_ASSIGN_OR_RAISE(auto stream, input_file->Open());

  std::vector<std::byte> bytes(static_cast<size_t>(length));
  ICEBERG_RETURN_UNEXPECTED(stream->ReadFully(offset, bytes));
  ICEBERG_RETURN_UNEXPECTED(stream->Close());

  std::span<const uint8_t> blob(reinterpret_cast<const uint8_t*>(bytes.data()),
                                bytes.size());
  return PositionDeleteIndex::Deserialize(blob, delete_file);
}

Result<puffin::BlobMetadata> DVUtil::WriteDVBlob(puffin::PuffinWriter& writer,
                                                 std::string_view referenced_data_file,
                                                 PositionDeleteIndex& positions) {
  ICEBERG_ASSIGN_OR_RAISE(auto data, positions.Serialize());

  puffin::Blob blob{
      .type = std::string(puffin::StandardBlobTypes::kDeletionVectorV1),
      .input_fields = {MetadataColumns::kFilePositionColumnId},
      .snapshot_id = -1,
      .sequence_number = -1,
      .data = std::move(data),
      .requested_compression = puffin::PuffinCompressionCodec::kNone,
  };
  blob.properties.emplace("referenced-data-file", std::string(referenced_data_file));
  blob.properties.emplace("cardinality", std::format("{}", positions.Cardinality()));
  return writer.Write(blob);
}

Result<std::shared_ptr<DataFile>> DVUtil::MakeDVDataFile(
    std::string_view path, int64_t file_size, std::string_view referenced_data_file,
    const PartitionValues& partition, int64_t cardinality,
    const std::shared_ptr<PartitionSpec>& spec,
    const puffin::BlobMetadata& blob_metadata) {
  ICEBERG_PRECHECK(spec != nullptr, "Deletion vector requires a partition spec");
  return std::make_shared<DataFile>(DataFile{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = std::string(path),
      .file_format = FileFormatType::kPuffin,
      .partition = partition,
      .record_count = cardinality,
      .file_size_in_bytes = file_size,
      .referenced_data_file = std::string(referenced_data_file),
      .content_offset = blob_metadata.offset,
      .content_size_in_bytes = blob_metadata.length,
      .partition_spec_id = spec->spec_id(),
  });
}

}  // namespace iceberg
