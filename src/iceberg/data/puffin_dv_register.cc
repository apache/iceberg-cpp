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

#include "iceberg/data/puffin_dv_register.h"

#include <optional>
#include <string_view>
#include <utility>

#include "iceberg/data/deletion_vector_writer.h"
#include "iceberg/data/dv_util_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

class DataPuffinDVIO final : public PuffinDVIO {
 public:
  Result<std::vector<std::shared_ptr<DataFile>>> MergeAndWriteDVs(
      std::span<const DeletionVectorMergeGroup> groups, std::string_view output_path,
      const std::shared_ptr<FileIO>& io) override {
    if (groups.empty()) {
      return std::vector<std::shared_ptr<DataFile>>{};
    }

    ICEBERG_ASSIGN_OR_RAISE(
        auto writer,
        DeletionVectorWriter::Make(DeletionVectorWriterOptions{
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
      ICEBERG_RETURN_UNEXPECTED(writer->Delete(group.referenced_data_file, merged,
                                               group.spec, group.partition));
    }

    ICEBERG_RETURN_UNEXPECTED(writer->Close());
    ICEBERG_ASSIGN_OR_RAISE(auto metadata, writer->Metadata());
    return std::move(metadata.data_files);
  }
};

}  // namespace

std::shared_ptr<PuffinDVIO> MakePuffinDVIO() {
  return std::make_shared<DataPuffinDVIO>();
}

void RegisterPuffinDVIO() {
  static PuffinDVIORegistry registry(
      []() -> Result<std::shared_ptr<PuffinDVIO>> { return MakePuffinDVIO(); });
}

}  // namespace iceberg
