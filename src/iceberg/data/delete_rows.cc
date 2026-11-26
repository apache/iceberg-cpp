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

#include "iceberg/data/delete_rows.h"

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>
#include <nlohmann/json.hpp>

#include "iceberg/data/file_scan_task_reader.h"
#include "iceberg/data/output_file_cleanup_internal.h"
#include "iceberg/data/position_deletes_internal.h"
#include "iceberg/data_table_scan.h"
#include "iceberg/expression/binder.h"
#include "iceberg/json_serde_internal.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/name_mapping.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_properties.h"
#include "iceberg/update/row_delta.h"

namespace iceberg {
namespace {

// Record the original references before binding can fold a predicate to a constant.
// The task reader needs these columns when it binds residuals to the read schema.
class FilterColumns : public Binder {
 public:
  explicit FilterColumns(const Schema& schema)
      : Binder(schema, /*case_sensitive=*/true) {}
  using Binder::Predicate;
  Result<std::shared_ptr<Expression>> Predicate(
      const std::shared_ptr<UnboundPredicate>& pred) override {
    names.emplace_back(pred->reference()->name());
    return Binder::Predicate(pred);
  }
  std::vector<std::string> names;
};

Result<std::vector<int64_t>> MatchingPositions(FileScanTaskReader& reader,
                                               const FileScanTask& task) {
  ICEBERG_ASSIGN_OR_RAISE(auto opened, reader.Open(task));
  nanoarrow::UniqueArrayStream stream(&opened);
  std::vector<int64_t> positions;
  while (true) {
    nanoarrow::UniqueArray batch;
    ArrowError error{};
    if (ArrowArrayStreamGetNext(stream.get(), batch.get(), &error) != NANOARROW_OK) {
      return IOError("Reading rows for deletion: {}", error.message);
    }
    if (!batch->release) break;
    if (batch->length == 0) continue;
    // _pos is the first projected field, a required int64 absolute file position.
    const auto& column = *batch->children[0];
    const auto* begin =
        static_cast<const int64_t*>(column.buffers[1]) + column.offset + batch->offset;
    positions.insert(positions.end(), begin, begin + batch->length);
  }
  return positions;
}

Result<std::vector<internal::FilePositionDeletes>> CollectDeletes(
    FileScanTaskReader& reader, FileScanTaskStream& tasks) {
  std::vector<internal::FilePositionDeletes> groups;
  std::unordered_map<std::string, size_t> by_file;
  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto task, tasks.Next());
    if (!task) break;
    ICEBERG_ASSIGN_OR_RAISE(auto positions, MatchingPositions(reader, **task));
    if (positions.empty()) continue;
    auto [it, inserted] =
        by_file.try_emplace((*task)->data_file()->file_path, groups.size());
    if (inserted) {
      groups.push_back({std::move(*task), std::move(positions)});
    } else {
      auto& group = groups[it->second].positions;
      group.insert(group.end(), positions.begin(), positions.end());
    }
  }
  return groups;
}

}  // namespace

Status MergeOnReadDelete(Table& table, std::shared_ptr<Expression> filter) {
  ICEBERG_PRECHECK(filter != nullptr, "Delete filter cannot be null");
  auto metadata = table.metadata();
  ICEBERG_PRECHECK(metadata->format_version >= 2,
                   "Merge-on-read deletes require v2 or later");
  auto isolation = table.properties().Get(TableProperties::kDeleteIsolationLevel);
  ICEBERG_PRECHECK(isolation == "serializable" || isolation == "snapshot",
                   "Unsupported delete isolation level: {}", isolation);
  ICEBERG_ASSIGN_OR_RAISE(auto schema, metadata->Schema());
  FilterColumns columns(*schema);
  ICEBERG_RETURN_UNEXPECTED(
      (Visit<std::shared_ptr<Expression>, Binder>(filter, columns)));
  const auto snapshot_id = metadata->current_snapshot_id;
  if (snapshot_id == kInvalidSnapshotId) return {};

  ICEBERG_ASSIGN_OR_RAISE(auto selected, schema->Select(columns.names));
  std::vector<SchemaField> fields{MetadataColumns::kRowPosition};
  fields.insert(fields.end(), selected->fields().begin(), selected->fields().end());
  ICEBERG_ASSIGN_OR_RAISE(auto builder, table.NewScan());
  builder->UseSnapshot(snapshot_id, /*use_snapshot_schema=*/false)
      .Filter(filter)
      .Project(std::move(selected));
  ICEBERG_ASSIGN_OR_RAISE(auto scan, builder->Build());
  ICEBERG_ASSIGN_OR_RAISE(auto tasks, scan->PlanFilesStream());

  const auto& properties = table.properties().configs();
  std::shared_ptr<NameMapping> mapping;
  auto mapping_it = properties.find(std::string(TableProperties::kDefaultNameMapping));
  if (mapping_it != properties.end()) {
    ICEBERG_ASSIGN_OR_RAISE(auto json, FromJsonString(mapping_it->second));
    ICEBERG_ASSIGN_OR_RAISE(mapping, NameMappingFromJson(json));
  }
  ICEBERG_ASSIGN_OR_RAISE(
      auto reader, FileScanTaskReader::Make({
                       .io = scan->io(),
                       .table_schema = schema,
                       .schemas = metadata->schemas,
                       .projected_schema = std::make_shared<Schema>(std::move(fields)),
                       .name_mapping = std::move(mapping),
                       .properties = properties,
                       .apply_residuals = true,
                   }));
  ICEBERG_ASSIGN_OR_RAISE(auto groups, CollectDeletes(*reader, *tasks));
  if (groups.empty()) return {};

  ICEBERG_ASSIGN_OR_RAISE(auto delta, table.NewRowDelta());
  delta->ValidateFromSnapshot(snapshot_id)
      .ConflictDetectionFilter(std::move(filter))
      .ValidateDeletedFiles();
  if (isolation == "serializable") delta->ValidateNoConflictingDataFiles();

  std::set<std::string> outputs;
  auto written =
      internal::WritePositionDeletes(table, scan->io(), std::move(groups), outputs);
  if (!written) {
    return internal::FailWithOutputCleanup(std::move(written.error()), *table.io(),
                                           outputs);
  }
  delta->ValidateDataFilesExist(written->referenced_data_files);
  for (const auto& file : written->data_files) delta->AddDeletes(file);
  for (const auto& file : written->rewritten_delete_files) delta->RemoveDeletes(file);
  auto status = delta->Commit();
  if (!status && status.error().kind != ErrorKind::kCommitStateUnknown) {
    return internal::FailWithOutputCleanup(std::move(status.error()), *table.io(),
                                           outputs);
  }
  return status;
}

}  // namespace iceberg
