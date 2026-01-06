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

#include "iceberg/catalog/rest/types.h"

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_metadata.h"
#include "iceberg/table_requirement.h"
#include "iceberg/table_update.h"

namespace iceberg::rest {

bool CreateTableRequest::operator==(const CreateTableRequest& other) const {
  if (name != other.name || location != other.location ||
      stage_create != other.stage_create || properties != other.properties) {
    return false;
  }

  if (!schema != !other.schema) {
    return false;
  }
  if (schema && *schema != *other.schema) {
    return false;
  }

  if (!partition_spec != !other.partition_spec) {
    return false;
  }
  if (partition_spec && *partition_spec != *other.partition_spec) {
    return false;
  }

  if (!write_order != !other.write_order) {
    return false;
  }
  if (write_order && *write_order != *other.write_order) {
    return false;
  }
  return true;
}

bool LoadTableResult::operator==(const LoadTableResult& other) const {
  if (metadata_location != other.metadata_location || config != other.config) {
    return false;
  }

  if (!metadata != !other.metadata) {
    return false;
  }
  if (metadata && *metadata != *other.metadata) {
    return false;
  }
  return true;
}

bool CommitTableRequest::operator==(const CommitTableRequest& other) const {
  if (identifier != other.identifier) {
    return false;
  }
  if (requirements.size() != other.requirements.size()) {
    return false;
  }
  if (updates.size() != other.updates.size()) {
    return false;
  }

  for (size_t i = 0; i < requirements.size(); ++i) {
    if (!requirements[i] != !other.requirements[i]) {
      return false;
    }
    if (requirements[i] && !requirements[i]->Equals(*other.requirements[i])) {
      return false;
    }
  }

  for (size_t i = 0; i < updates.size(); ++i) {
    if (!updates[i] != !other.updates[i]) {
      return false;
    }
    if (updates[i] && !updates[i]->Equals(*other.updates[i])) {
      return false;
    }
  }

  return true;
}

bool CommitTableResponse::operator==(const CommitTableResponse& other) const {
  if (metadata_location != other.metadata_location) {
    return false;
  }
  if (!metadata != !other.metadata) {
    return false;
  }
  if (metadata && *metadata != *other.metadata) {
    return false;
  }
  return true;
}

}  // namespace iceberg::rest
