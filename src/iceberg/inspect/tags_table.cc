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

#include "iceberg/inspect/tags_table.h"

#include <algorithm>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "iceberg/arrow_row_builder_internal.h"
#include "iceberg/inspect/metadata_table_stream_internal.h"
#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/snapshot.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/type.h"
#include "iceberg/util/macros.h"

namespace iceberg {
namespace {

struct TagRow {
  std::string name;
  int64_t snapshot_id;
  std::optional<int64_t> max_ref_age_ms;
};

Status AppendTag(ArrowRowBuilder& builder, const TagRow& tag) {
  ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(0), tag.name));
  ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(1), tag.snapshot_id));
  if (tag.max_ref_age_ms.has_value()) {
    ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(2), *tag.max_ref_age_ms));
  } else {
    ICEBERG_RETURN_UNEXPECTED(AppendNull(builder.column(2)));
  }
  return builder.FinishRow();
}

}  // namespace

TagsTable::TagsTable(std::shared_ptr<Table> table) : MetadataTable(std::move(table)) {}

TagsTable::~TagsTable() = default;

const std::shared_ptr<Schema>& TagsTable::schema() const {
  static const auto schema = std::make_shared<Schema>(std::vector<SchemaField>{
      SchemaField::MakeRequired(1, "name", string()),
      SchemaField::MakeRequired(2, "snapshot_id", int64()),
      SchemaField::MakeOptional(3, "max_reference_age_in_ms", int64()),
  });
  return schema;
}

Result<std::unique_ptr<TagsTable>> TagsTable::Make(std::shared_ptr<Table> table) {
  ICEBERG_PRECHECK(table != nullptr, "Table cannot be null");
  return std::unique_ptr<TagsTable>(new TagsTable(std::move(table)));
}

Result<ArrowArrayStream> TagsTable::Scan() {
  std::vector<TagRow> rows;
  for (const auto& [name, ref] : source_table()->metadata()->refs) {
    if (ref == nullptr || ref->type() != SnapshotRefType::kTag) {
      continue;
    }
    const auto& retention = std::get<SnapshotRef::Tag>(ref->retention);
    rows.push_back(TagRow{.name = name,
                          .snapshot_id = ref->snapshot_id,
                          .max_ref_age_ms = retention.max_ref_age_ms});
  }
  std::ranges::sort(rows, {}, &TagRow::name);
  return internal::MakeMetadataTableStream(*schema(), std::move(rows), AppendTag);
}

}  // namespace iceberg
