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

#include <string>
#include <vector>

#include "iceberg/catalog/hive/iceberg_hive_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

/// \file iceberg/catalog/hive/hive_schema.h
/// \brief Render Iceberg schemas as Hive type strings.

namespace iceberg::hive {

/// \brief Column metadata passed to HMS.
struct ICEBERG_HIVE_EXPORT HiveColumn {
  std::string name;
  std::string type_string;
  std::string comment;
};

/// \brief Render a Hive type string, including nested types.
/// Follow Java: variant -> unknown, time/uuid -> string, fixed -> binary.
/// Unsupported types return kNotImplemented. Zoned timestamps require Hive 3+.
ICEBERG_HIVE_EXPORT Result<std::string> TypeToHiveString(const Type& type);

/// \brief Convert top-level fields to HMS columns. Docs become comments.
/// Field IDs and requiredness are not represented in HMS.
ICEBERG_HIVE_EXPORT Result<std::vector<HiveColumn>> SchemaToHiveColumns(
    const Schema& schema);

}  // namespace iceberg::hive
