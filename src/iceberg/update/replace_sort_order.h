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
#include <vector>

#include "iceberg/expression/term.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/pending_update.h"
#include "iceberg/sort_field.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Replacing table sort order with a newly created order.
class ICEBERG_EXPORT ReplaceSortOrder : public PendingUpdate {
 public:
  /// \brief Constructs a ReplaceSortOrder for the specified table.
  ///
  /// \param identifier The table identifier
  /// \param catalog The catalog containing the table
  /// \param metadata The current table metadata
  ReplaceSortOrder(TableIdentifier identifier, std::shared_ptr<Catalog> catalog,
                   std::shared_ptr<TableMetadata> base);

  /// \brief Add a field to the sort by field name, ascending with the given null order.
  ///
  /// \param term A term referencing the field
  /// \param null_order The null order (first or last)
  /// \return Reference to this ReplaceSortOrder for chaining
  ReplaceSortOrder& Asc(std::shared_ptr<Term> term, NullOrder null_order);

  /// \brief Add a field to the sort by field name, descending with the given null order.
  ///
  /// \param term A transform term referencing the field
  /// \param null_order The null order (first or last)
  /// \return Reference to this ReplaceSortOrder for chaining
  ReplaceSortOrder& Desc(std::shared_ptr<Term> term, NullOrder null_order);

  /// \brief Set case sensitivity of sort column name resolution.
  ///
  /// \param case_sensitive When true, column name resolution is case-sensitive
  /// \return Reference to this ReplaceSortOrder for chaining
  ReplaceSortOrder& CaseSensitive(bool case_sensitive);

  /// \brief Applies the sort order changes without committing them.
  ///
  /// Validates the pending sort order changes but does not commit them to the table.
  /// This method can be used to validate changes before actually committing them.
  ///
  /// \return Status::OK if the changes are valid, or an error if validation fails
  Status Apply() override;

  /// \brief Commits the sort order changes to the table.
  ///
  /// Validates the changes and applies them to the table through the catalog.
  ///
  /// \return Status::OK if the changes are valid and committed successfully, or an error
  Status Commit() override;

  /// \brief Get the built sort order after applying changes.
  ///
  /// \return The built SortOrder object.
  std::shared_ptr<SortOrder> GetBuiltSortOrder() const;

 private:
  /// \brief Helper to add a sort field after binding the term.
  ///
  /// \param ref The bound reference to the field
  /// \param transform The transform to apply
  /// \param direction The sort direction
  /// \param null_order The null order
  /// \return Reference to this ReplaceSortOrder for chaining
  ReplaceSortOrder& AddSortField(std::shared_ptr<BoundReference> ref,
                                 std::shared_ptr<Transform> transform,
                                 SortDirection direction, NullOrder null_order);

  TableIdentifier identifier_;
  std::shared_ptr<Catalog> catalog_;
  std::shared_ptr<TableMetadata> base_metadata_;

  std::vector<SortField> sort_fields_;
  bool case_sensitive_ = true;
  std::shared_ptr<SortOrder> built_sort_order_;
};

}  // namespace iceberg
