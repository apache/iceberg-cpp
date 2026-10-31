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

/// \file iceberg/inspect/metadata_scan.h
/// \brief Configure metadata scans, plan reusable tasks, and read Arrow batches.

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"
#include "iceberg/util/error_collector.h"
#include "iceberg/util/stream.h"

namespace iceberg {

using MetadataScanTaskStream = Stream<std::shared_ptr<MetadataScanTask>>;
using MetadataScanTaskStreamPtr = std::unique_ptr<MetadataScanTaskStream>;

namespace internal {

/// \brief Captured metadata and logical settings shared by a builder and its scans.
struct MetadataScanContext {
  MetadataTableKind kind;
  std::shared_ptr<const TableMetadata> table_metadata;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<Schema> projected_schema;
  bool case_sensitive = true;
  std::optional<int64_t> snapshot_id;
  std::string ref_name;
  std::optional<int64_t> as_of_time_in_millis;
};

}  // namespace internal

/// \brief Configure a scan over captured metadata; the metadata table must outlive it.
class ICEBERG_EXPORT MetadataScanBuilder : public ErrorCollector {
 public:
  ~MetadataScanBuilder() override;

  /// \brief Capture the source metadata and schema of a metadata table.
  static Result<std::unique_ptr<MetadataScanBuilder>> Make(const MetadataTable& table);

  /// \brief Select names using the current CaseSensitive() setting.
  ///
  /// Columns are returned in schema order; unknown names are ignored. An empty
  /// list selects no columns and `*` selects all columns. Nested names use dots;
  /// selecting a map value selects the whole map, while selecting only its key fails.
  MetadataScanBuilder& Select(const std::vector<std::string>& columns);

  /// \brief Project whole columns by field ID, optionally reordering them.
  ///
  /// Types must match and nullable columns cannot become required.
  MetadataScanBuilder& Project(std::shared_ptr<Schema> schema);

  /// \brief Match column names case-sensitively; enabled by default.
  MetadataScanBuilder& CaseSensitive(bool value);

  /// \brief Select a snapshot ID; Build rejects tables without time travel.
  MetadataScanBuilder& UseSnapshot(int64_t snapshot_id);

  /// \brief Select a branch or tag; Build rejects tables without time travel.
  MetadataScanBuilder& UseRef(const std::string& ref);

  /// \brief Select the latest snapshot at or before this timestamp on the selected ref.
  ///
  /// The timestamp is milliseconds since the Unix epoch. Build rejects tables
  /// without time travel.
  MetadataScanBuilder& AsOfTime(int64_t as_of_time_in_millis);

  /// \brief Validate the settings and build a scan with its own copy of them.
  ///
  /// Later builder changes and normal source table refreshes do not affect it.
  Result<std::unique_ptr<MetadataScan>> Build();

 private:
  explicit MetadataScanBuilder(const MetadataTable& table);

  const MetadataTable* table_;
  internal::MetadataScanContext context_;
};

/// \brief A metadata scan that plans tasks or reads all of them as Arrow batches.
class ICEBERG_EXPORT MetadataScan {
 public:
  virtual ~MetadataScan();

  /// \brief Return a fresh task stream; consuming it may report planning errors.
  virtual Result<MetadataScanTaskStreamPtr> PlanTasksStream() const = 0;

  /// \brief Collect every task from PlanTasksStream(), preserving planning order.
  virtual Result<std::vector<std::shared_ptr<MetadataScanTask>>> PlanTasks() const;

  /// \brief Read every task in planning order as one Arrow stream.
  ///
  /// The stream owns its read state and can outlive this scan. Release the stream
  /// and each returned schema or array independently. To set batch size, open the
  /// planned tasks with MetadataScanTaskReader; each task retains its projection.
  virtual Result<ArrowArrayStream> ToArrowArrayStream() const;

 protected:
  explicit MetadataScan(internal::MetadataScanContext context);

  const internal::MetadataScanContext& context() const { return context_; }

 private:
  const internal::MetadataScanContext context_;
};

}  // namespace iceberg
