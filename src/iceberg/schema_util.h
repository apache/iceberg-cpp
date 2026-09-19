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

/// \file iceberg/schema_util.h
/// \brief Provide schema projection utilities.

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
  std::shared_ptr<ExtraAttributes> attributes;
};

/// \brief A schema partner to carry projection information.
struct ICEBERG_EXPORT SchemaProjection {
  std::vector<FieldProjection> fields;
};

/// \brief Options to control schema projection behavior.
struct ICEBERG_EXPORT ProjectionOptions {
  /// \brief Which default value fills a field that is missing in the source schema.
  enum class DefaultPolicy {
    /// \brief Use the v3 `initial-default`; for reading data written before the field
    /// existed.
    kInitial,
    /// \brief Use the v3 `write-default`; for aligning data to the write schema.
    kWrite,
  };

  /// \brief The default value policy for missing fields.
  DefaultPolicy default_policy = DefaultPolicy::kInitial;
  /// \brief Whether type promotion (e.g. int to long) is allowed for projected fields.
  /// Must be disabled when projecting for the write path, where the source values are
  /// stored as-is and therefore must exactly match the expected type.
  bool allow_type_promotion = true;
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
/// \param options Options controlling default values and type promotion.
/// \return The projection result.
ICEBERG_EXPORT Result<SchemaProjection> Project(const Schema& expected_schema,
                                                const Schema& source_schema,
                                                bool prune_source,
                                                const ProjectionOptions& options = {});

ICEBERG_EXPORT std::string_view ToString(FieldProjection::Kind kind);
ICEBERG_EXPORT std::string ToString(const FieldProjection& projection);
ICEBERG_EXPORT std::string ToString(const SchemaProjection& projection);

}  // namespace iceberg
