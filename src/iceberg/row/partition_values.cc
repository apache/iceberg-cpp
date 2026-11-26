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

#include "iceberg/row/partition_values.h"

namespace iceberg {

PartitionValues& PartitionValues::operator=(const PartitionValues& other) {
  if (this != &other) {
    values_ = other.values_;
  }
  return *this;
}

bool PartitionValues::operator==(const PartitionValues& other) const {
  return values_ == other.values_;
}

Result<Scalar> PartitionValues::GetField(size_t pos) const {
  if (pos >= values_.size()) {
    return InvalidArgument(
        "Position {} is out of bounds for PartitionValues with {} fields", pos,
        values_.size());
  }

  return LiteralToScalar(values_[pos]);
}

Result<std::reference_wrapper<const Literal>> PartitionValues::ValueAt(size_t pos) const {
  if (pos >= values_.size()) {
    return InvalidArgument("Cannot get partition value at {} from {} fields", pos,
                           values_.size());
  }
  return std::cref(values_[pos]);
}

}  // namespace iceberg
