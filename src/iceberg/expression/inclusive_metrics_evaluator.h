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

/// \file iceberg/expression/inclusive_metrics_evaluator.h
///
/// Evaluates an Expression on a DataFile to test whether rows in the file may match.
///
/// This evaluation is inclusive: it returns true if a file may match and false if it
/// cannot match.
///
/// Files are passed to #eval(ContentFile), which returns true if the file may contain
/// matching rows and false if the file cannot contain matching rows. Files may be skipped
/// if and only if the return value of eval is false.
///
/// Due to the comparison implementation of ORC stats, for float/double columns in ORC
/// files, if the first value in a file is NaN, metrics of this file will report NaN for
/// both upper and lower bound despite that the column could contain non-NaN data. Thus in
/// some scenarios explicitly checks for NaN is necessary in order to not skip files that
/// may contain matching data.
///

#include <memory>

#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

class ICEBERG_EXPORT InclusiveMetricsEvaluator {
 public:
  /// \brief Make a inclusive metrics evaluator
  ///
  /// \param expr The expression to evaluate
  /// \param schema The schema of the table
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<InclusiveMetricsEvaluator>> Make(
      std::shared_ptr<Expression> expr, const Schema& schema, bool case_sensitive = true);

  ~InclusiveMetricsEvaluator();

  /// \brief Evaluate the expression against a DataFile.
  ///
  /// \param data_file The data file to evaluate
  /// \return true if the file matches the expression, false otherwise, or error
  Result<bool> Evaluate(const DataFile& data_file) const;

 private:
  explicit InclusiveMetricsEvaluator(std::shared_ptr<Expression> expr);

  std::shared_ptr<Expression> expr_;
};

}  // namespace iceberg
