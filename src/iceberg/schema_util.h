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
#include <string>
#include <variant>
#include <vector>

#include "iceberg/expression/literal.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A field schema partner to carry projection information.
struct ICEBERG_EXPORT FieldProjection {
  /// \brief How the field is projected.
  enum class Kind {
    /// \brief The field is projected from the source with possible conversion for
    /// supported schema evolution.
    kProjected,
    /// \brief Metadata column whose value is generated on demand.
    kMetadata,
    /// \brief The field is a constant value (e.g. partition field value)
    kConstant,
    /// \brief The field is missing in the source and should be filled with default value.
    kDefault,
    /// \brief An optional field that is not present in the source.
    kNull,
  };

  /// \brief A variant to indicate how to set the value of the field.
  /// \note `std::monostate` is used to indicate that the field is not projected.
  /// \note `size_t` is used to indicate the field index in the source schema on the same
  /// nesting level when `kind` is `kProjected`.
  /// \note `Literal` is used to indicate the value of the field when `kind` is
  /// `kConstant` or `kDefault`.
  using From = std::variant<std::monostate, size_t, Literal>;

  /// \brief Format-specific attributes for the field.
  /// For example, for Parquet it might store column id and level info of the projected
  /// leaf field.
  struct ExtraAttributes {
    virtual ~ExtraAttributes() = default;
  };

  /// \brief The kind of projection of the field it partners with.
  Kind kind;
  /// \brief The source to set the value of the field.
  From from;
  /// \brief The children of the field if it is a nested field.
  std::vector<FieldProjection> children;
  /// \brief Format-specific attributes for the field.
  std::shared_ptr<ExtraAttributes> extra;
};

/// \brief A schema partner to carry projection information.
struct ICEBERG_EXPORT SchemaProjection {
  std::vector<FieldProjection> fields;
};

/// \brief Project the expected schema on top of the source schema.
///
/// \param expected_schema The expected schema.
/// \param source_schema The source schema.
/// \param prune_source Whether the source schema can be pruned to project the expected
/// schema on it. For example, literally a Parquet reader implementation is capable of
/// column pruning, so `prune_source` is set to true in this case such that the `from`
/// field in `FieldProjection` exactly reflects the position (relative to its nesting
/// level) to get the column value from the reader.
/// \return The projection result.
ICEBERG_EXPORT Result<SchemaProjection> Project(const Schema& expected_schema,
                                                const Schema& source_schema,
                                                bool prune_source);

ICEBERG_EXPORT std::string_view ToString(FieldProjection::Kind kind);
ICEBERG_EXPORT std::string ToString(const FieldProjection& projection);
ICEBERG_EXPORT std::string ToString(const SchemaProjection& projection);

}  // namespace iceberg
