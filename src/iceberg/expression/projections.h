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

/// \file iceberg/expression/projections.h
/// Utils to project expressions on rows to expressions on partitions.

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief A class that projects expressions for a table's data rows into expressions on
/// the table's partition values, for a table's partition spec.
class ICEBERG_EXPORT ProjectionEvaluator {
 public:
  ~ProjectionEvaluator();

  /// \brief Project the given row expression to a partition expression.
  ///
  /// \param expr an expression on data rows
  /// \return an expression on partition data (depends on the projection)
  Result<std::shared_ptr<Expression>> Project(const std::shared_ptr<Expression>& expr);

 private:
  friend class Projections;

  /// \brief Create a ProjectionEvaluator.
  ///
  /// \param visitor The projection visitor to use
  explicit ProjectionEvaluator(std::unique_ptr<class ProjectionVisitor> visitor);

  std::unique_ptr<ProjectionVisitor> visitor_;
};

/// \brief Utils to project expressions on rows to expressions on partitions.
///
/// There are two types of projections: inclusive and strict.
///
/// An inclusive projection guarantees that if an expression matches a row, the projected
/// expression will match the row's partition.
///
/// A strict projection guarantees that if a partition matches a projected expression,
/// then all rows in that partition will match the original expression.
struct ICEBERG_EXPORT Projections {
  /// \brief Creates an inclusive ProjectionEvaluator for the partition spec.
  ///
  /// An evaluator is used to project expressions for a table's data rows into expressions
  /// on the table's partition values. The evaluator returned by this function is
  /// inclusive and will build expressions with the following guarantee: if the original
  /// expression matches a row, then the projected expression will match that row's
  /// partition.
  ///
  /// Each predicate in the expression is projected using Transform::Project.
  ///
  /// \param spec a partition spec
  /// \param schema a schema
  /// \param case_sensitive whether the Projection should consider case sensitivity on
  /// column names or not. Defaults to true (case sensitive).
  /// \return an inclusive projection evaluator for the partition spec
  static std::unique_ptr<ProjectionEvaluator> Inclusive(const PartitionSpec& spec,
                                                        const Schema& schema,
                                                        bool case_sensitive = true);

  /// \brief Creates a strict ProjectionEvaluator for the partition spec.
  ///
  /// An evaluator is used to project expressions for a table's data rows into expressions
  /// on the table's partition values. The evaluator returned by this function is strict
  /// and will build expressions with the following guarantee: if the projected expression
  /// matches a partition, then the original expression will match all rows in that
  /// partition.
  ///
  /// Each predicate in the expression is projected using Transform::ProjectStrict.
  ///
  /// \param spec a partition spec
  /// \param schema a schema
  /// \param case_sensitive whether the Projection should consider case sensitivity on
  /// column names or not. Defaults to true (case sensitive).
  /// \return a strict projection evaluator for the partition spec
  static std::unique_ptr<ProjectionEvaluator> Strict(const PartitionSpec& spec,
                                                     const Schema& schema,
                                                     bool case_sensitive = true);
};

}  // namespace iceberg
