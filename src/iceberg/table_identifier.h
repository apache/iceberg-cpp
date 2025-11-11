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

/// \file iceberg/table_identifier.h
/// A TableIdentifier is a unique identifier for a table

#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"

namespace iceberg {

/// \brief A namespace in a catalog.
struct ICEBERG_EXPORT Namespace {
  std::vector<std::string> levels;
};

/// \brief Identifies a table in iceberg catalog.
struct ICEBERG_EXPORT TableIdentifier {
  Namespace ns;
  std::string name;

  /// \brief Validates the TableIdentifier.
  Status Validate() const {
    if (name.empty()) {
      return Invalid("Invalid table identifier: missing table name");
    }
    return {};
  }
};

}  // namespace iceberg
