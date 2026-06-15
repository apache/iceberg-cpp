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

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Base class for Iceberg metadata tables.
class ICEBERG_EXPORT MetadataTable {
 public:
  enum class Kind {
    kSnapshots,
    kHistory,
  };

  static Result<std::unique_ptr<MetadataTable>> Make(std::shared_ptr<Table> table,
                                                     Kind kind);

  virtual ~MetadataTable();

  virtual Kind kind() const noexcept = 0;

  const TableIdentifier& name() const { return identifier_; }

  const std::shared_ptr<Schema>& schema() const { return schema_; }

  const std::shared_ptr<Table>& source_table() const { return source_table_; }

 protected:
  explicit MetadataTable(std::shared_ptr<Table> source_table, TableIdentifier identifier,
                         std::shared_ptr<Schema> schema);

 private:
  TableIdentifier identifier_;
  std::shared_ptr<Schema> schema_;
  std::shared_ptr<Table> source_table_;
};

}  // namespace iceberg
