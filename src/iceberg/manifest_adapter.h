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

/// \file iceberg/metadata_adapter.h
/// Base class of adapter for v1v2v3v4 metadata.

#include "iceberg/arrow_c_data.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

// \brief Base class to append manifest metadata to Arrow array.
class ICEBERG_EXPORT ManifestAdapter {
 public:
  ManifestAdapter() = default;
  virtual ~ManifestAdapter() = default;

  virtual Status StartAppending() = 0;
  virtual Result<ArrowArray> FinishAppending() = 0;
  int64_t size() const { return size_; }

 protected:
  ArrowArray array_;
  int64_t size_ = 0;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestEntry`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestEntryAdapter : public ManifestAdapter {
 public:
  ManifestEntryAdapter() = default;
  ~ManifestEntryAdapter() override = default;

  virtual Status Append(const ManifestEntry& entry) = 0;
};

// \brief Implemented by different versions with different schemas to
// append a list of `ManifestFile`s to an `ArrowArray`.
class ICEBERG_EXPORT ManifestFileAdapter : public ManifestAdapter {
 public:
  ManifestFileAdapter() = default;
  ~ManifestFileAdapter() override = default;

  virtual Status Append(const ManifestFile& file) = 0;
};

}  // namespace iceberg
