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

/// \file iceberg/row/arrow_array_wrapper.h
/// Wrapper classes for ArrowArray that implement StructLike, ArrayLike, and MapLike
/// interfaces for unified row-oriented data access from columnar ArrowArray data.

#include "iceberg/arrow_c_data.h"
#include "iceberg/row/struct_like.h"

namespace iceberg {

/// \brief Wrapper for one row of a struct-typed ArrowArray.
class ICEBERG_EXPORT ArrowArrayStructLike : public StructLike {
 public:
  ~ArrowArrayStructLike() override;

  Result<Scalar> GetField(size_t pos) const override;

  size_t num_fields() const override;

  Status Reset(int64_t row_index);

  Status Reset(const ArrowArray& array, int64_t row_index = 0);

  static Result<std::unique_ptr<ArrowArrayStructLike>> Make(const ArrowSchema& schema,
                                                            const ArrowArray& array,
                                                            int64_t row_index = 0);

  ArrowArrayStructLike(const ArrowArrayStructLike&) = delete;
  ArrowArrayStructLike& operator=(const ArrowArrayStructLike&) = delete;

 private:
  class Impl;
  explicit ArrowArrayStructLike(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

/// \brief Wrapper for one row of a list-typed ArrowArray.
class ICEBERG_EXPORT ArrowArrayArrayLike : public ArrayLike {
 public:
  ~ArrowArrayArrayLike() override;

  Result<Scalar> GetElement(size_t pos) const override;

  size_t size() const override;

  Status Reset(int64_t row_index);

  Status Reset(const ArrowArray& array, int64_t row_index = 0);

  static Result<std::unique_ptr<ArrowArrayArrayLike>> Make(const ArrowSchema& schema,
                                                           const ArrowArray& array,
                                                           int64_t row_index = 0);

  ArrowArrayArrayLike(const ArrowArrayArrayLike& other) = delete;
  ArrowArrayArrayLike& operator=(const ArrowArrayArrayLike& other) = delete;

 private:
  class Impl;
  explicit ArrowArrayArrayLike(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

/// \brief Wrapper for one row of a map-typed ArrowArray.
class ICEBERG_EXPORT ArrowArrayMapLike : public MapLike {
 public:
  ~ArrowArrayMapLike() override;

  Result<Scalar> GetKey(size_t pos) const override;

  Result<Scalar> GetValue(size_t pos) const override;

  size_t size() const override;

  Status Reset(int64_t row_index);

  Status Reset(const ArrowArray& array, int64_t row_index = 0);

  static Result<std::unique_ptr<ArrowArrayMapLike>> Make(const ArrowSchema& schema,
                                                         const ArrowArray& array,
                                                         int64_t row_index = 0);

  ArrowArrayMapLike(const ArrowArrayMapLike& other) = delete;
  ArrowArrayMapLike& operator=(const ArrowArrayMapLike& other) = delete;

 private:
  class Impl;
  explicit ArrowArrayMapLike(std::unique_ptr<Impl> impl);

  std::unique_ptr<Impl> impl_;
};

}  // namespace iceberg
