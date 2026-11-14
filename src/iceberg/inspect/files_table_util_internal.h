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

#pragma once

#include "iceberg/inspect/metadata_table_util_internal.h"

namespace iceberg::internal {

/// \brief Build the files metadata table schema for a table.
Result<std::shared_ptr<Schema>> FilesTableSchema(
    const Schema& table_schema, const std::shared_ptr<StructType>& partition_type);

/// \brief Append a data-file row using the files metadata table schema.
Status AppendDataFile(ArrowRowBuilder& builder, const Schema& schema,
                      const Schema& table_schema, const StructType& partition_type,
                      int32_t readable_metrics_id, const LiveFile& live_file);

}  // namespace iceberg::internal
