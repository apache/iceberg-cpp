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

Result<ArrowArrayStream> MetadataScanBuilder::Build() {
  ICEBERG_RETURN_UNEXPECTED(CheckErrors());
  ICEBERG_PRECHECK(projected_schema_ != nullptr, "Projected schema cannot be null");
  return table_->ScanProjection(*projected_schema_);
}

}  // namespace iceberg
