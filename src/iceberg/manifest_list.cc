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

#include "iceberg/manifest_list.h"

#include "iceberg/schema.h"

namespace iceberg {

const StructType& PartitionFieldSummary::Type() {
  static const StructType kInstance{{
      PartitionFieldSummary::kContainsNull,
      PartitionFieldSummary::kContainsNaN,
      PartitionFieldSummary::kLowerBound,
      PartitionFieldSummary::kUpperBound,
  }};
  return kInstance;
}

const std::shared_ptr<Schema>& ManifestFile::Type() {
  static const auto kInstance = std::make_shared<Schema>(std::vector<SchemaField>{
      kManifestPath,
      kManifestLength,
      kPartitionSpecId,
      kContent,
      kSequenceNumber,
      kMinSequenceNumber,
      kAddedSnapshotId,
      kAddedFilesCount,
      kExistingFilesCount,
      kDeletedFilesCount,
      kAddedRowsCount,
      kExistingRowsCount,
      kDeletedRowsCount,
      kPartitions,
      kKeyMetadata,
      kFirstRowId,
  });
  return kInstance;
}

}  // namespace iceberg
