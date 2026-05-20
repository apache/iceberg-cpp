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

/// \file iceberg/data/delete_filter.h
/// Delete-aware filtering for Arrow C Data batches.

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/data/delete_loader.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/iceberg_data_export.h"
#include "iceberg/result.h"
#include "iceberg/schema_field.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Result of ComputeAliveRows: indices of rows not matched by any delete.
struct ICEBERG_DATA_EXPORT AliveRowSelection {
  /// Zero-based row indices within the batch that are alive (not deleted).
  std::vector<int32_t> indices;

  /// Number of alive rows (convenience accessor to avoid size_t casts).
  int64_t alive_count() const { return static_cast<int64_t>(indices.size()); }

  bool empty() const { return indices.empty(); }
};

/// \brief Counts rows removed by delete filters.
class ICEBERG_DATA_EXPORT DeleteCounter {
 public:
  void Increment(int64_t count = 1) {
    count_.fetch_add(count, std::memory_order_relaxed);
  }
  int64_t Get() const { return count_.load(std::memory_order_relaxed); }

 private:
  std::atomic<int64_t> count_{0};
};

/// \brief Concrete batch-oriented delete filter for merge-on-read data batches.
class ICEBERG_DATA_EXPORT DeleteFilter {
 public:
  /// \brief Field lookup output for current or fallback equality-delete fields.
  ///
  /// `field` is the exact field for validation. `projection_field` is the
  /// top-level field, possibly with a pruned nested struct path, that must be
  /// merged into RequiredSchema so the data reader can materialize the delete
  /// column.
  struct FieldLookupResult {
    SchemaField field;
    SchemaField projection_field;
  };

  /// \brief Lookup a field by ID, including fields from table schema fallbacks.
  using FieldLookup = std::function<Result<std::optional<FieldLookupResult>>(int32_t)>;

  /// \brief Build a lookup from the current schema and optional table schemas.
  ///
  /// The current table schema is searched first. `schemas` is the table metadata
  /// schema list and may contain `table_schema`; current schema duplicates are ignored
  /// and fallback schemas are searched from latest schema id to oldest.
  static Result<FieldLookup> MakeFieldLookup(
      std::shared_ptr<Schema> table_schema,
      std::span<const std::shared_ptr<Schema>> schemas = {});

  /// \brief Build a lookup from table metadata which uses the current schema first,
  /// then table metadata schemas as fallback.
  static Result<FieldLookup> MakeFieldLookup(
      std::shared_ptr<TableMetadata> table_metadata);

  /// \brief Create a DeleteFilter with current schema only field lookup.
  ///
  /// \param need_row_pos_col If true, `_pos` is added to `RequiredSchema` when
  ///   position deletes are present so `ComputeAliveRows` can apply them.
  ///   Pass false when the caller owns position filtering externally (e.g. a vectorised
  ///   reader that applies the position delete index directly to Arrow column buffers).
  ///   Note that when `need_row_pos_col` is false, `HasPositionDeletes()` may
  ///   return true but `ComputeAliveRows` will not apply position deletes because `_pos`
  ///   is absent from `RequiredSchema`. The caller is responsible for applying them.
  /// \param counter Optional counter incremented for each deleted row.
  static Result<std::unique_ptr<DeleteFilter>> Make(
      std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
      std::shared_ptr<Schema> table_schema, std::shared_ptr<Schema> requested_schema,
      std::shared_ptr<FileIO> io, bool need_row_pos_col = true,
      std::shared_ptr<DeleteCounter> counter = nullptr);

  /// \brief Create a DeleteFilter using table metadata for schema-aware field lookup.
  static Result<std::unique_ptr<DeleteFilter>> Make(
      std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
      std::shared_ptr<TableMetadata> table_metadata,
      std::shared_ptr<Schema> requested_schema, std::shared_ptr<FileIO> io,
      bool need_row_pos_col = true, std::shared_ptr<DeleteCounter> counter = nullptr);

  /// \brief Create a DeleteFilter with table schemas for dropped equality fields.
  static Result<std::unique_ptr<DeleteFilter>> Make(
      std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
      std::shared_ptr<Schema> table_schema, std::shared_ptr<Schema> requested_schema,
      std::shared_ptr<FileIO> io, std::span<const std::shared_ptr<Schema>> schemas,
      bool need_row_pos_col = true, std::shared_ptr<DeleteCounter> counter = nullptr);

  /// \brief Create a DeleteFilter with a custom field lookup.
  static Result<std::unique_ptr<DeleteFilter>> Make(
      std::string file_path, std::span<const std::shared_ptr<DataFile>> delete_files,
      std::shared_ptr<Schema> requested_schema, std::shared_ptr<FileIO> io,
      FieldLookup field_lookup, bool need_row_pos_col = true,
      std::shared_ptr<DeleteCounter> counter = nullptr);

  ~DeleteFilter();

  /// \brief Schema required from the underlying data file reader.
  const std::shared_ptr<Schema>& RequiredSchema() const;

  /// \brief The original schema requested by the caller, before delete columns were
  /// added.
  const std::shared_ptr<Schema>& ExpectedSchema() const;

  /// \brief Increment the delete counter by the given count.
  ///
  /// Allows callers to record deletes that occur outside `ComputeAliveRows` (e.g. when
  /// applying deletes in a vectorised path).
  void IncrementDeleteCount(int64_t count = 1);

  /// \brief Expose the loaded position delete index for external use.
  ///
  /// Triggers lazy loading of position delete files on first call. Returns nullptr
  /// when there are no position deletes. Returns an error if loading fails.
  ///
  /// The returned pointer is valid only for the lifetime of this DeleteFilter.
  Result<const PositionDeleteIndex*> DeletedRowPositions() const;

  /// \brief Returns a predicate that is true for rows NOT matched by any equality delete.
  ///
  /// The returned function is valid for the lifetime of this DeleteFilter and is cached
  /// after the first call. When there are no equality deletes, returns a predicate that
  /// always returns true (every row is alive).
  ///
  /// \note The returned predicate is NOT thread-safe: it mutates internal projection
  /// state on each call. Do not invoke it concurrently from multiple threads.
  Result<std::function<Result<bool>(const StructLike&)>> EqDeletedRowFilter() const;

  /// \brief Returns a predicate that is true for rows matched by any equality delete.
  ///
  /// Inverse of `EqDeletedRowFilter()`. When there are no equality deletes, returns a
  /// predicate that always returns false (no row is deleted).
  Result<std::function<Result<bool>(const StructLike&)>> FindEqualityDeleteRows() const;

  /// \brief Compute alive rows relative to the supplied Arrow C Data batch.
  ///
  /// Returns the indices (zero-based, relative to the batch) of rows not matched by
  /// any delete. Deleted-row counts are forwarded to the DeleteCounter supplied at
  /// construction.
  Result<AliveRowSelection> ComputeAliveRows(const ArrowSchema& batch_schema,
                                             const ArrowArray& batch) const;

  bool HasPositionDeletes() const;
  bool HasEqualityDeletes() const;

  DeleteFilter(const DeleteFilter&) = delete;
  DeleteFilter& operator=(const DeleteFilter&) = delete;

 private:
  struct EqDeleteGroup;

  DeleteFilter(std::string file_path, std::shared_ptr<Schema> requested_schema,
               std::shared_ptr<FileIO> io, FieldLookup field_lookup,
               bool need_row_pos_col, std::shared_ptr<DeleteCounter> counter);

  Status Init(std::span<const std::shared_ptr<DataFile>> delete_files);
  Result<std::shared_ptr<Schema>> ComputeRequiredSchema() const;
  Status EnsurePositionDeletesLoaded() const;
  Status EnsureEqualityDeletesLoaded() const;

  const std::string file_path_;
  std::vector<std::shared_ptr<DataFile>> pos_deletes_;
  std::vector<std::shared_ptr<DataFile>> eq_deletes_;

  std::shared_ptr<Schema> requested_schema_;
  std::shared_ptr<Schema> required_schema_;
  FieldLookup field_lookup_;

  const bool need_row_pos_col_;
  // Position of `_pos` in required_schema_ when existent
  std::optional<size_t> pos_field_position_;
  std::shared_ptr<DeleteCounter> counter_;

  // TODO(gangwu): expose a factory hook (e.g. a std::function<DeleteLoader()> or a
  // virtual newDeleteLoader()) so callers can inject a caching DeleteLoader (analogous to
  // SparkDeleteFilter.CachingDeleteLoader in Java).
  DeleteLoader delete_loader_;

  mutable std::mutex pos_mutex_;
  mutable bool pos_loaded_ = false;
  mutable PositionDeleteIndex pos_index_;

  mutable std::mutex eq_mutex_;
  mutable bool eq_loaded_ = false;
  mutable std::vector<std::unique_ptr<EqDeleteGroup>> eq_groups_;
  mutable std::function<Result<bool>(const StructLike&)> eq_deleted_row_filter_cache_;
};

}  // namespace iceberg
