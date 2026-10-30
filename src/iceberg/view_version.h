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

/// \file iceberg/view_version.h
/// A version of an Iceberg view.

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include "iceberg/iceberg_export.h"
#include "iceberg/table_identifier.h"
#include "iceberg/util/timepoint.h"

namespace iceberg {

/// \brief The SQL representation of a view version's definition.
///
/// Each SQL representation pairs a query with its engine dialect, such as
/// "spark". Dialect strings are preserved verbatim and compared with ASCII
/// case folding.
struct ICEBERG_EXPORT SqlViewRepresentation {
  /// The SQL SELECT statement that defines the view.
  std::string sql;
  /// The dialect of the SELECT statement, such as "spark" or "trino".
  std::string dialect;

  /// \brief Compare two SQL representations for equality.
  friend bool operator==(const SqlViewRepresentation& lhs,
                         const SqlViewRepresentation& rhs) = default;
};

/// \brief An opaque representation type this library does not interpret.
///
/// Unknown representations are preserved so that metadata round trips and
/// property edits do not lose them. Definition changes that involve them are
/// rejected instead of being partially rewritten.
struct ICEBERG_EXPORT UnknownViewRepresentation {
  /// The representation type identifier from the metadata, such as "cypher".
  std::string type;

  /// \brief Compare two unknown representations for equality.
  friend bool operator==(const UnknownViewRepresentation& lhs,
                         const UnknownViewRepresentation& rhs) = default;
};

/// \brief A representation of a view version's definition: SQL or opaque.
using ViewRepresentation = std::variant<SqlViewRepresentation, UnknownViewRepresentation>;

/// \brief The type identifier of the SQL representation.
inline constexpr std::string_view kSqlRepresentationType = "sql";

/// \brief Get the SQL representation.
///
/// \return the SQL representation, or nullptr when the representation is not
/// a SQL representation. The pointer remains valid until the representation
/// is modified or destroyed.
ICEBERG_EXPORT const SqlViewRepresentation* GetSql(const ViewRepresentation& repr);

/// \brief Get the representation's type identifier.
///
/// Returns kSqlRepresentationType for SQL representations and the opaque
/// type identifier otherwise.
ICEBERG_EXPORT std::string_view RepresentationType(const ViewRepresentation& repr);

/// \brief A view version, the state of a view's definition at a point in time.
///
/// View versions are immutable: a definition change creates a new version.
/// A version can carry more than one representation, but at most one SQL
/// representation per dialect.
struct ICEBERG_EXPORT ViewVersion {
  /// ID of the view version.
  int32_t version_id;
  /// ID of the schema for this view version.
  int32_t schema_id;
  /// Timestamp in milliseconds from the unix epoch when the version was created.
  TimePointMs timestamp_ms;
  /// Summary metadata about the version, such as "engine-name" and
  /// "engine-version".
  std::unordered_map<std::string, std::string> summary;
  /// A list of representations for the view definition.
  std::vector<ViewRepresentation> representations;
  /// Catalog name to use when a reference in the SELECT does not contain a
  /// catalog. When unset, the catalog in which the view is stored is used.
  std::optional<std::string> default_catalog;
  /// Namespace to use when a reference in the SELECT is a single identifier.
  Namespace default_namespace;

  /// \brief Compare two view versions for equality.
  friend bool operator==(const ViewVersion& lhs, const ViewVersion& rhs) {
    return lhs.Equals(rhs);
  }

 private:
  /// \brief Compare two view versions for equality.
  bool Equals(const ViewVersion& other) const;
};

}  // namespace iceberg
