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

/// \file iceberg/inspect/row_builder_internal.h
/// Internal Arrow row-building utilities shared by metadata tables.
///
/// Metadata tables (snapshots, history, manifests, ...) materialize in-memory
/// structures into Arrow batches that conform to the table's Iceberg schema.
/// `ArrowRowBuilder` wraps a nanoarrow `ArrowArray` initialized from such a
/// schema and exposes per-column builders plus typed append helpers so each
/// metadata table can emit rows without re-implementing the nanoarrow
/// boilerplate.

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_map>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Builds an Arrow struct array (a batch) for an arbitrary Iceberg schema.
///
/// Typical usage:
/// \code
///   ICEBERG_ASSIGN_OR_RAISE(auto builder, ArrowRowBuilder::Make(schema));
///   for (const auto& row : rows) {
///     ICEBERG_RETURN_UNEXPECTED(AppendInt(builder.column(0), row.id));
///     ICEBERG_RETURN_UNEXPECTED(AppendString(builder.column(1), row.name));
///     ICEBERG_RETURN_UNEXPECTED(builder.FinishRow());
///   }
///   ICEBERG_ASSIGN_OR_RAISE(auto array, std::move(builder).Finish());
/// \endcode
class ICEBERG_EXPORT ArrowRowBuilder {
 public:
  /// \brief Create a row builder for the given Iceberg schema.
  static Result<ArrowRowBuilder> Make(const Schema& schema);

  ArrowRowBuilder(ArrowRowBuilder&& other) noexcept;
  ArrowRowBuilder& operator=(ArrowRowBuilder&& other) noexcept;

  ArrowRowBuilder(const ArrowRowBuilder&) = delete;
  ArrowRowBuilder& operator=(const ArrowRowBuilder&) = delete;

  ~ArrowRowBuilder();

  /// \brief The number of top-level columns in the batch.
  int64_t num_columns() const;

  /// \brief Access the nanoarrow child builder for a top-level column.
  ///
  /// \param index Zero-based column index. Returns nullptr if out of range.
  ArrowArray* column(int64_t index);

  /// \brief Finish the current row, advancing the struct length by one.
  ///
  /// Call after appending exactly one value (or null) to every column.
  Status FinishRow();

  /// \brief Finish building and transfer ownership of the resulting array.
  ///
  /// The builder must not be used after this call.
  Result<ArrowArray> Finish() &&;

 private:
  explicit ArrowRowBuilder(std::unique_ptr<ArrowArray>&& array) noexcept;

  std::unique_ptr<ArrowArray> array_;
};

/// \brief Append a null to a nanoarrow array builder.
ICEBERG_EXPORT Status AppendNull(ArrowArray* array);

/// \brief Append a boolean value to a nanoarrow array builder.
ICEBERG_EXPORT Status AppendBoolean(ArrowArray* array, bool value);

/// \brief Append an integer value to a nanoarrow array builder.
///
/// Works for int32/int64/timestamp columns, which nanoarrow stores as int64.
ICEBERG_EXPORT Status AppendInt(ArrowArray* array, int64_t value);

/// \brief Append a string value to a nanoarrow array builder.
ICEBERG_EXPORT Status AppendString(ArrowArray* array, std::string_view value);

/// \brief Append a map<string, string> value to a nanoarrow map array builder.
///
/// Appends one (possibly empty) map element. The iteration order of the
/// resulting entries is unspecified.
ICEBERG_EXPORT Status AppendStringMap(
    ArrowArray* array, const std::unordered_map<std::string, std::string>& entries);

}  // namespace iceberg
