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

/// \file iceberg/manifest_wrapper.h
/// Wrapper classes for manifest-related data structures that implement
/// StructLike, ArrayLike, and MapLike interfaces for unified data access.

#include <functional>

#include "iceberg/iceberg_export.h"
#include "iceberg/manifest/manifest_list.h"
#include "iceberg/row/struct_like.h"

namespace iceberg {

/// \brief StructLike wrapper for PartitionFieldSummary.
class ICEBERG_EXPORT PartitionFieldSummaryStructLike : public StructLike {
 public:
  explicit PartitionFieldSummaryStructLike(const PartitionFieldSummary& summary)
      : summary_(summary) {}
  ~PartitionFieldSummaryStructLike() override = default;

  PartitionFieldSummaryStructLike(const PartitionFieldSummaryStructLike&) = delete;
  PartitionFieldSummaryStructLike& operator=(const PartitionFieldSummaryStructLike&) =
      delete;

  Result<Scalar> GetField(size_t pos) const override;

  size_t num_fields() const override { return 4; }

  void Reset(const PartitionFieldSummary& summary) { summary_ = std::cref(summary); }

 private:
  std::reference_wrapper<const PartitionFieldSummary> summary_;
};

/// \brief ArrayLike wrapper for a vector of PartitionFieldSummary.
class ICEBERG_EXPORT PartitionFieldSummaryArrayLike : public ArrayLike {
 public:
  explicit PartitionFieldSummaryArrayLike(
      const std::vector<PartitionFieldSummary>& summaries)
      : summaries_(summaries) {}
  ~PartitionFieldSummaryArrayLike() override = default;

  PartitionFieldSummaryArrayLike(const PartitionFieldSummaryArrayLike&) = delete;
  PartitionFieldSummaryArrayLike& operator=(const PartitionFieldSummaryArrayLike&) =
      delete;

  Result<Scalar> GetElement(size_t pos) const override;

  size_t size() const override { return summaries_.get().size(); }

  void Reset(const std::vector<PartitionFieldSummary>& summaries) {
    summaries_ = std::cref(summaries);
  }

 private:
  std::reference_wrapper<const std::vector<PartitionFieldSummary>> summaries_;
  mutable std::shared_ptr<PartitionFieldSummaryStructLike> summary_;
};

/// \brief StructLike wrapper for ManifestFile.
class ICEBERG_EXPORT ManifestFileStructLike : public StructLike {
 public:
  explicit ManifestFileStructLike(const ManifestFile& file) : manifest_file_(file) {}
  ~ManifestFileStructLike() override = default;

  ManifestFileStructLike(const ManifestFileStructLike&) = delete;
  ManifestFileStructLike& operator=(const ManifestFileStructLike&) = delete;

  Result<Scalar> GetField(size_t pos) const override;

  size_t num_fields() const override;

  void Reset(const ManifestFile& file) { manifest_file_ = std::cref(file); }

 private:
  std::reference_wrapper<const ManifestFile> manifest_file_;
  mutable std::shared_ptr<PartitionFieldSummaryArrayLike> summaries_;
};

}  // namespace iceberg
