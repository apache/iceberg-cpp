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

#include <memory>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/type_fwd.h"

namespace iceberg::internal {

/// \brief Build the Java-compatible union of active partition fields across all specs.
ICEBERG_EXPORT Result<std::shared_ptr<StructType>> UnifiedPartitionType(
    const TableMetadata& metadata);

/// \brief Project values written with one spec into the table-wide partition type.
ICEBERG_EXPORT Result<PartitionValues> ProjectPartitionValues(
    const StructType& partition_type, const PartitionSpec& spec,
    const PartitionValues& values);

/// \brief Append a primitive partition value or decoded metric bound.
ICEBERG_EXPORT Status AppendLiteral(ArrowArray* array, const Literal& literal);

/// \brief Append partition values to an Arrow struct builder.
ICEBERG_EXPORT Status AppendPartitionValues(ArrowArray* array,
                                            const StructType& partition_type,
                                            const PartitionValues& values);

}  // namespace iceberg::internal
