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

#include "iceberg/deletes/position_delete_index.h"

namespace iceberg {

void PositionDeleteIndex::Delete(int64_t pos) { bitmap_.Add(pos); }

void PositionDeleteIndex::Delete(int64_t pos_start, int64_t pos_end) {
  bitmap_.AddRange(pos_start, pos_end);
}

bool PositionDeleteIndex::IsDeleted(int64_t pos) const { return bitmap_.Contains(pos); }

bool PositionDeleteIndex::IsEmpty() const { return bitmap_.IsEmpty(); }

int64_t PositionDeleteIndex::Cardinality() const {
  return static_cast<int64_t>(bitmap_.Cardinality());
}

void PositionDeleteIndex::Merge(const PositionDeleteIndex& other) {
  bitmap_.Or(other.bitmap_);
}

void PositionDeleteIndex::BulkAddForKey(int32_t key,
                                        std::span<const uint32_t> positions) {
  bitmap_.AddManyForKey(key, positions);
}

}  // namespace iceberg
