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

/// \file iceberg/expression/strict_metrics_evaluator.h
///
/// Evaluates an Expression on a DataFile to test whether all rows in the file match.
///
/// This evaluation is strict: it returns true if all rows in a file must match the
/// expression. For example, if a file's ts column has min X and max Y, this evaluator
/// will return true for ts &lt; Y+1 but not for ts &lt; Y-1.
///
/// Files are passed to #eval(ContentFile), which returns true if all rows in the file
/// must contain matching rows and false if the file may contain rows that do not match.
///
/// Due to the comparison implementation of ORC stats, for float/double columns in ORC
/// files, if the first value in a file is NaN, metrics of this file will report NaN for
/// both upper and lower bound despite that the column could contain non-NaN data. Thus in
/// some scenarios explicitly checks for NaN is necessary in order to not include files
/// that may contain rows that don't match.
///

#include <memory>

#include "iceberg/expression/expression.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Evaluates an Expression against DataFile.
/// \note: The evaluator is thread-safe.
class ICEBERG_EXPORT StrictMetricsEvaluator {
 public:
  /// \brief Make a strict metrics evaluator
  ///
  /// \param expr The expression to evaluate
  /// \param schema The schema of the table
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<StrictMetricsEvaluator>> Make(
      std::shared_ptr<Expression> expr, std::shared_ptr<Schema> schema,
      bool case_sensitive = true);

  ~StrictMetricsEvaluator();

  /// \brief Evaluate the expression against a DataFile.
  ///
  /// \param data_file The data file to evaluate
  /// \return true if the file matches the expression, false otherwise, or error
  Result<bool> Evaluate(const DataFile& data_file) const;

 private:
  explicit StrictMetricsEvaluator(std::shared_ptr<Expression> expr,
                                  std::shared_ptr<Schema> schema);

 private:
  std::shared_ptr<Expression> expr_;
  std::shared_ptr<Schema> schema_;
};

}  // namespace iceberg
