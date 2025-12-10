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
/// Residual evaluator for finding residual expressions after partition evaluation.

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Finds the residuals for an Expression using the partitions in the given
/// PartitionSpec.
///
/// A residual expression is made by partially evaluating an expression using partition
/// values. For example, if a table is partitioned by day(utc_timestamp) and is read
/// with a filter expression utc_timestamp >= a and utc_timestamp <= b, then there are
/// 4 possible residual expressions for the partition data, d:
///
/// - If d > day(a) and d < day(b), the residual is always true
/// - If d == day(a) and d != day(b), the residual is utc_timestamp >= a
/// - If d == day(b) and d != day(a), the residual is utc_timestamp <= b
/// - If d == day(a) == day(b), the residual is utc_timestamp >= a and utc_timestamp <= b
///
/// Partition data is passed using StructLike. Residuals are returned by ResidualFor().
class ICEBERG_EXPORT ResidualEvaluator {
 public:
  /// \brief Return a residual evaluator for an unpartitioned PartitionSpec.
  ///
  /// \param expr An expression
  /// \return A residual evaluator that always returns the expression
  static Result<std::unique_ptr<ResidualEvaluator>> Unpartitioned(
      std::shared_ptr<Expression> expr);

  /// \brief Return a residual evaluator for a PartitionSpec and Expression.
  ///
  /// \param expr An expression
  /// \param spec A partition spec
  /// \param schema The schema to bind expressions against
  /// \param case_sensitive Whether field name matching is case-sensitive
  /// \return A residual evaluator for the expression
  static Result<std::unique_ptr<ResidualEvaluator>> Make(std::shared_ptr<Expression> expr,
                                                         const PartitionSpec& spec,
                                                         const Schema& schema,
                                                         bool case_sensitive = true);

  ~ResidualEvaluator();

  /// \brief Returns a residual expression for the given partition values.
  ///
  /// \param partition_data Partition data values
  /// \return The residual of this evaluator's expression from the partition values
  virtual Result<std::shared_ptr<Expression>> ResidualFor(
      const StructLike& partition_data) const;

 protected:
  ResidualEvaluator(std::shared_ptr<Expression> expr, const PartitionSpec& spec,
                    const Schema& schema, bool case_sensitive);

  std::shared_ptr<Expression> expr_;

 private:
  const PartitionSpec& spec_;
  const Schema& schema_;
  bool case_sensitive_;
};

}  // namespace iceberg
