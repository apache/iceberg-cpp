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

/// \file iceberg/constants.h
/// This file defines constants used commonly and shared across multiple
/// source files.  It is mostly useful to add constants that are used as
/// default values in the class definitions in the header files without
/// including other headers just for the constant definitions.

#include <cstdint>
#include <string_view>

namespace iceberg {

constexpr std::string_view kParquetFieldIdKey = "PARQUET:field_id";
constexpr int64_t kInvalidSnapshotId = -1;
constexpr int64_t kInvalidSequenceNumber = -1;
/// \brief Stand-in for the current sequence number that will be assigned when the commit
/// is successful. This is replaced when writing a manifest list by the ManifestFile
/// adapter.
constexpr int64_t kUnassignedSequenceNumber = -1;

// TODO(gangwu): move other commonly used constants here.

}  // namespace iceberg
