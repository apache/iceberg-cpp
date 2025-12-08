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

/// \file iceberg/expression/evaluator.h
/// Evaluator for checking if a data row matches a bound expression.

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Evaluates an Expression against data rows.
///
/// This class evaluates bound expressions against StructLike data rows to determine
/// if the row matches the expression criteria. The evaluator binds unbound expressions
/// to a schema on construction and then can be used to evaluate multiple data rows.
///
/// \note: The evaluator is thread-safe.
class ICEBERG_EXPORT Evaluator {
 public:
  /// \brief Make an evaluator for an unbound expression.
  ///
  /// \param schema The schema to bind against
  /// \param unbound The unbound expression to evaluate
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<Evaluator>> Make(const Schema& schema,
                                                 std::shared_ptr<Expression> unbound,
                                                 bool case_sensitive = true);

  ~Evaluator();

  /// \brief Evaluate the expression against a data row.
  ///
  /// \param row The data row to evaluate
  /// \return true if the row matches the expression, false otherwise, or error
  Result<bool> Evaluate(const StructLike& row) const;

 private:
  explicit Evaluator(std::shared_ptr<Expression> bound_expr);

  std::shared_ptr<Expression> bound_expr_;
};

}  // namespace iceberg
