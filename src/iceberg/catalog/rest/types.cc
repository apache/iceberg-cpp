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

}  // namespace iceberg::rest
