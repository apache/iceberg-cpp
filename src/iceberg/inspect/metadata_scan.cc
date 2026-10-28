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

#include "iceberg/inspect/metadata_scan.h"

#include <memory>
#include <utility>

#include "iceberg/inspect/metadata_table.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"

namespace iceberg {

MetadataScanBuilder::MetadataScanBuilder(const MetadataTable& table)
    : table_(&table), projected_schema_(table.schema()) {}

MetadataScanBuilder::~MetadataScanBuilder() = default;

MetadataScanBuilder& MetadataScanBuilder::Select(
    const std::vector<std::string>& columns) {
  auto projected = table_->schema()->Select(columns, case_sensitive_);
  if (projected) {
    projected_schema_ = std::move(projected).value();
  } else {
    AddError(projected.error());
  }
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::Project(std::shared_ptr<Schema> schema) {
  projected_schema_ = std::move(schema);
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::CaseSensitive(bool value) {
  case_sensitive_ = value;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::UseSnapshot(int64_t snapshot_id) {
  ICEBERG_BUILDER_CHECK(
      std::holds_alternative<std::monostate>(snapshot_selection_.snapshot),
      "Cannot override snapshot, a snapshot or timestamp is already set");
  snapshot_selection_.snapshot = snapshot_id;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::UseRef(const std::string& ref) {
  ICEBERG_BUILDER_CHECK(snapshot_selection_.ref_name.empty(),
                        "Cannot override ref, already set ref={}",
                        snapshot_selection_.ref_name);
  ICEBERG_BUILDER_CHECK(!ref.empty(), "Ref name cannot be empty");
  snapshot_selection_.ref_name = ref;
  return *this;
}

MetadataScanBuilder& MetadataScanBuilder::AsOfTime(int64_t timestamp_millis) {
  ICEBERG_BUILDER_CHECK(
      std::holds_alternative<std::monostate>(snapshot_selection_.snapshot),
      "Cannot override snapshot, a snapshot or timestamp is already set");
  snapshot_selection_.snapshot = TimePointMsFromUnixMs(timestamp_millis);
  return *this;
}

Result<ArrowArrayStream> MetadataScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_PRECHECK(projected_schema_ != nullptr, "Projected schema cannot be null");
  return table_->ScanSnapshot(snapshot_selection_, *projected_schema_);
}

}  // namespace iceberg
