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

/// \file iceberg/expression/residual_evaluator.h
///
/// Finds the residuals for an Expression the partitions in the given PartitionSpec.
///
/// A residual expression is made by partially evaluating an expression using partition
/// values. For example, if a table is partitioned by day(utc_timestamp) and is read with
/// a filter expression utc_timestamp &gt;= a and utc_timestamp &lt;= b, then there are 4
/// possible residuals expressions for the partition data, d:
///
/// <ul>
///   <li>If d &gt; day(a) and d &lt; day(b), the residual is always true
///   <li>If d == day(a) and d != day(b), the residual is utc_timestamp &gt;= a
///   <li>if d == day(b) and d != day(a), the residual is utc_timestamp &lt;= b
///   <li>If d == day(a) == day(b), the residual is utc_timestamp &gt;= a and
///   utc_timestamp &lt;= b
/// </ul>
///
/// <p>Partition data is passed using {@link StructLike}. Residuals are returned by
/// #residualFor(StructLike).
///
/// <p>This class is thread-safe.
///

#include <memory>

#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT ResidualEvaluator {
 public:
  /// \brief Make a residual evaluator
  ///
  /// \param expr The expression to evaluate
  /// \param spec The partition spec
  /// \param schema The schema of the table
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<ResidualEvaluator>> Make(
      std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec> spec,
      std::shared_ptr<Schema> schema, bool case_sensitive = true);

  /// \brief Make a residual evaluator for unpartitioned tables
  ///
  /// \param expr The expression to evaluate
  static Result<std::unique_ptr<ResidualEvaluator>> MakeUnpartitioned(
      std::shared_ptr<Expression> expr);

  ~ResidualEvaluator();

  /// \brief Returns a residual expression for the given partition values.
  ///
  /// \param partition_data The partition data values
  /// \return the residual of this evaluator's expression from the partition values
  virtual Result<std::shared_ptr<Expression>> ResidualFor(
      const StructLike& partition_data) const;

 protected:
  explicit ResidualEvaluator(const std::shared_ptr<Expression>& expr,
                             std::shared_ptr<Schema> schema, bool case_sensitive);

 protected:
  bool case_sensitive_;
  std::shared_ptr<Expression> expr_;
  std::shared_ptr<Schema> schema_;
};

}  // namespace iceberg
