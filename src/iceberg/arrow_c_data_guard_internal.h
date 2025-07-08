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

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data.h"

namespace iceberg::internal {

class ArrowArrayGuard {
 public:
  explicit ArrowArrayGuard(ArrowArray* array) : array_(array) {}
  ~ArrowArrayGuard();

 private:
  ArrowArray* array_;
};

class ArrowSchemaGuard {
 public:
  explicit ArrowSchemaGuard(ArrowSchema* schema) : schema_(schema) {}
  ~ArrowSchemaGuard();

 private:
  ArrowSchema* schema_;
};

class ArrowArrayViewGuard {
 public:
  explicit ArrowArrayViewGuard(ArrowArrayView* view) : view_(view) {}
  ~ArrowArrayViewGuard();

 private:
  ArrowArrayView* view_;
};

class ArrowArrayBufferGuard {
 public:
  explicit ArrowArrayBufferGuard(ArrowBuffer* buffer) : buffer_(buffer) {}
  ~ArrowArrayBufferGuard();

 private:
  ArrowBuffer* buffer_;
};

}  // namespace iceberg::internal
