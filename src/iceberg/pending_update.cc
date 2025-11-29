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

#include "iceberg/pending_update.h"

#include "iceberg/catalog.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_update.h"
#include "iceberg/util/macros.h"

namespace iceberg {

// ============================================================================
// UpdateProperties implementation
// ============================================================================

PropertiesUpdate& PropertiesUpdate::Set(std::string const& key,
                                        std::string const& value) {
  updates_[key] = value;
  return *this;
}

PropertiesUpdate& PropertiesUpdate::Remove(std::string const& key) {
  removals_.push_back(key);
  return *this;
}

Result<PropertiesUpdateChanges> PropertiesUpdate::Apply() {
  return PropertiesUpdateChanges{updates_, removals_};
}

Status PropertiesUpdate::ApplyResult(TableMetadataBuilder& builder,
                                     PropertiesUpdateChanges result) {
  if (!result.updates.empty()) {
    builder.SetProperties(result.updates);
  }
  if (!result.removals.empty()) {
    builder.RemoveProperties(result.removals);
  }
  return {};
}

Status PropertiesUpdate::Commit() {
  return NotImplemented("UpdateProperties::Commit() not implemented");
}

}  // namespace iceberg
