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
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "iceberg/data/dv_util_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_io.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/result.h"
#include "iceberg/util/content_file_util.h"
#include "iceberg/util/macros.h"

namespace iceberg {

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

}  // namespace iceberg
