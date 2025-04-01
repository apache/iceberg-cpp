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

/// \file iceberg/schema.h
/// Schemas for Iceberg tables.  This header contains the definition of Schema
/// and any utility functions.  See iceberg/type.h and iceberg/field.h as well.

#include <cstdint>
#include <string>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/schema_field.h"
#include "iceberg/type.h"

namespace iceberg {

/// \brief A schema for a Table.
///
/// A schema is a list of typed columns, along with a unique integer ID.  A
/// Table may have different schemas over its lifetime due to schema
/// evolution.
class ICEBERG_EXPORT Schema : public StructType {
 public:
  Schema(int32_t schema_id, std::vector<SchemaField> fields);

  /// \brief Get the schema ID.
  ///
  /// A schema is identified by a unique ID for the purposes of schema
  /// evolution.
  [[nodiscard]] int32_t schema_id() const;

  [[nodiscard]] std::string ToString() const override;

  friend bool operator==(const Schema& lhs, const Schema& rhs) { return lhs.Equals(rhs); }

  friend bool operator!=(const Schema& lhs, const Schema& rhs) { return !(lhs == rhs); }

 private:
  /// \brief Compare two schemas for equality.
  [[nodiscard]] bool Equals(const Schema& other) const;

  const int32_t schema_id_;
};

}  // namespace iceberg
