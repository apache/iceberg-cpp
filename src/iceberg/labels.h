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

/// \file iceberg/labels.h
/// \brief Catalog-provided labels for tables and their fields.

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "iceberg/iceberg_export.h"

namespace iceberg {

/// \brief Catalog-provided labels for a single field, identified by its field ID.
struct ICEBERG_EXPORT FieldLabel {
  int32_t field_id = 0;
  std::unordered_map<std::string, std::string> labels;

  bool operator==(const FieldLabel&) const = default;
};

/// \brief Optional catalog-provided labels returned when a table is loaded.
///
/// Labels are advisory enrichment, not table state: they are not persisted with the
/// table and may be absent.
struct ICEBERG_EXPORT Labels {
  std::unordered_map<std::string, std::string> object_labels;
  std::vector<FieldLabel> fields;

  /// \brief Returns true when there are neither object-level nor field-level labels.
  bool empty() const { return object_labels.empty() && fields.empty(); }

  bool operator==(const Labels&) const = default;
};

}  // namespace iceberg
