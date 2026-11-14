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

/// \file iceberg/inspect/manifests_table.h
/// \brief Define the manifests metadata table.

#include <memory>

#include "iceberg/arrow_c_data.h"
#include "iceberg/iceberg_export.h"
#include "iceberg/inspect/metadata_table.h"
#include "iceberg/result.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/// \brief Metadata table containing manifest-list entries for a snapshot.
class ICEBERG_EXPORT ManifestsTable : public MetadataTable {
 public:
  static Result<std::unique_ptr<ManifestsTable>> Make(std::shared_ptr<const Table> table);

  ~ManifestsTable() override;

  Kind kind() const noexcept override { return Kind::kManifests; }

  std::shared_ptr<Schema> schema() const override;

 protected:
  Result<std::unique_ptr<MetadataScan>> MakeScan(
      internal::MetadataScanContext context) const override;

 private:
  static Result<ArrowArrayStream> ReadRows(const internal::MetadataScanContext& context,
                                           const std::shared_ptr<FileIO>& io,
                                           int64_t batch_size);

  explicit ManifestsTable(std::shared_ptr<const Table> table);
};

}  // namespace iceberg
