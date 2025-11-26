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

/// \file iceberg/expression/manifest_evaluator.h
///
/// Evaluates an Expression on a ManifestFile to test whether the file contains
/// matching partitions.
///
/// For row expressions, evaluation is inclusive: it returns true if a file
/// may match and false if it cannot match.
///
/// Files are passed to #eval(ManifestFile), which returns true if the manifest may
/// contain data files that match the partition expression. Manifest files may be
/// skipped if and only if the return value of eval is false.
///

#include <memory>

#include "iceberg/iceberg_export.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Evaluates an Expression against manifest.
/// \note: The evaluator is thread-safe.
class ICEBERG_EXPORT ManifestEvaluator {
 public:
  /// \brief Make a manifest evaluator for RowFilter
  ///
  /// \param expr The expression to evaluate
  /// \param spec The partition spec
  /// \param schema The schema of the table
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<ManifestEvaluator>> MakeRowFilter(
      std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec>& spec,
      const Schema& schema, bool case_sensitive = true);

  /// \brief Make a manifest evaluator for PartitionFilter
  ///
  /// \param expr The expression to evaluate
  /// \param spec The partition spec
  /// \param schema The schema of the table
  /// \param case_sensitive Whether field name matching is case-sensitive
  static Result<std::unique_ptr<ManifestEvaluator>> MakePartitionFilter(
      std::shared_ptr<Expression> expr, const std::shared_ptr<PartitionSpec>& spec,
      const Schema& schema, bool case_sensitive = true);

  ~ManifestEvaluator();

  /// \brief Evaluate the expression against a manifest.
  ///
  /// \param manifest The manifest to evaluate
  /// \return true if the row matches the expression, false otherwise, or error
  Result<bool> Evaluate(const ManifestFile& manifest) const;

 private:
  explicit ManifestEvaluator(std::shared_ptr<Expression> expr);

 private:
  std::shared_ptr<Expression> expr_;
};

}  // namespace iceberg
