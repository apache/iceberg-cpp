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

#include "iceberg/arrow_c_data_guard_internal.h"

namespace iceberg::internal {

ArrowArrayGuard::~ArrowArrayGuard() {
  if (array_ != nullptr) {
    ArrowArrayRelease(array_);
  }
}

ArrowSchemaGuard::~ArrowSchemaGuard() {
  if (schema_ != nullptr) {
    ArrowSchemaRelease(schema_);
  }
}

ArrowArrayViewGuard::~ArrowArrayViewGuard() {
  if (view_ != nullptr) {
    ArrowArrayViewReset(view_);
  }
}

ArrowArrayBufferGuard::~ArrowArrayBufferGuard() {
  if (buffer_ != nullptr) {
    ArrowBufferReset(buffer_);
  }
}

}  // namespace iceberg::internal
