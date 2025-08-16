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

#include "iceberg/row/arrow_array_wrapper.h"

#include <nanoarrow/nanoarrow.h>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/result.h"
#include "iceberg/util/macros.h"

namespace iceberg {

#define NANOARROW_RETURN_IF_NOT_OK(status)                         \
  if (status != NANOARROW_OK) [[unlikely]] {                       \
    return InvalidArrowData("Nanoarrow error: {}", error.message); \
  }

namespace {

Result<ViewValue> ExtractValue(const ArrowArrayView* array_view, int64_t index) {
  if (ArrowArrayViewIsNull(array_view, index)) {
    return ViewValue{std::monostate{}};
  }

  switch (array_view->storage_type) {
    case NANOARROW_TYPE_BOOL: {
      auto value = ArrowArrayViewGetIntUnsafe(array_view, index);
      return ViewValue{static_cast<bool>(value)};
    }
    case NANOARROW_TYPE_INT32: {
      auto value = ArrowArrayViewGetIntUnsafe(array_view, index);
      return ViewValue{static_cast<int32_t>(value)};
    }
    case NANOARROW_TYPE_INT64: {
      auto value = ArrowArrayViewGetIntUnsafe(array_view, index);
      return ViewValue{value};
    }
    case NANOARROW_TYPE_FLOAT: {
      auto value = ArrowArrayViewGetDoubleUnsafe(array_view, index);
      return ViewValue{static_cast<float>(value)};
    }
    case NANOARROW_TYPE_DOUBLE: {
      auto value = ArrowArrayViewGetDoubleUnsafe(array_view, index);
      return ViewValue{value};
    }
    case NANOARROW_TYPE_STRING: {
      auto value = ArrowArrayViewGetStringUnsafe(array_view, index);
      return ViewValue{std::string(value.data, value.size_bytes)};
    }
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY: {
      auto value = ArrowArrayViewGetBytesUnsafe(array_view, index);
      std::vector<uint8_t> binary_data(value.data.as_uint8,
                                       value.data.as_uint8 + value.size_bytes);
      return ViewValue{binary_data};
    }
    case NANOARROW_TYPE_STRUCT: {
      return NotImplemented("Nested struct types are not yet supported");
    }
    case NANOARROW_TYPE_LIST: {
      return NotImplemented("List types are not yet supported");
    }
    case NANOARROW_TYPE_MAP: {
      return NotImplemented("Map types are not yet supported");
    }
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_TIME64:
    case NANOARROW_TYPE_TIMESTAMP:
    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_EXTENSION:
      // TODO(gangwu): Handle these types properly
    default:
      return NotImplemented("Unsupported Arrow type: {}",
                            static_cast<int>(array_view->storage_type));
  }
}

}  // namespace

// ArrowArrayStructLike Implementation

class ArrowArrayStructLike::Impl {
 public:
  Impl(const ArrowSchema& schema, const ArrowArray& array, int64_t row_index)
      : schema_(schema), array_(std::cref(array)), row_index_(row_index) {}

  ~Impl() = default;

  Result<ViewValue> GetField(size_t pos) const {
    if (pos >= static_cast<size_t>(schema_.n_children)) {
      return InvalidArgument("Field index {} out of range (size: {})", pos,
                             schema_.n_children);
    }

    if (row_index_ < 0 || row_index_ >= array_.get().length) {
      return InvalidArgument("Row index {} out of range (length: {})", row_index_,
                             array_.get().length);
    }

    return ExtractValue(array_view_.children[pos], row_index_);
  }

  size_t num_fields() const { return static_cast<size_t>(schema_.n_children); }

  Status Reset(const ArrowArray& array, int64_t row_index) {
    array_ = std::cref(array);
    row_index_ = row_index;

    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return {};
  }

  Status Reset(int64_t row_index) {
    row_index_ = row_index;
    return {};
  }

  Status Init() {
    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewInitFromSchema(&array_view_, &schema_, &error));
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return {};
  }

 private:
  ArrowArrayView array_view_;
  internal::ArrowArrayViewGuard array_view_guard_{&array_view_};

  const ArrowSchema& schema_;
  std::reference_wrapper<const ArrowArray> array_;
  int64_t row_index_;
};

Result<std::unique_ptr<ArrowArrayStructLike>> ArrowArrayStructLike::Make(
    const ArrowSchema& schema, const ArrowArray& array, int64_t row_index) {
  auto impl = std::make_unique<Impl>(schema, array, row_index);
  ICEBERG_RETURN_UNEXPECTED(impl->Init());
  return std::unique_ptr<ArrowArrayStructLike>(new ArrowArrayStructLike(std::move(impl)));
}

ArrowArrayStructLike::ArrowArrayStructLike(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ArrowArrayStructLike::~ArrowArrayStructLike() = default;

Result<ViewValue> ArrowArrayStructLike::GetField(size_t pos) const {
  return impl_->GetField(pos);
}

size_t ArrowArrayStructLike::num_fields() const { return impl_->num_fields(); }

Status ArrowArrayStructLike::Reset(int64_t row_index) { return impl_->Reset(row_index); }

Status ArrowArrayStructLike::Reset(const ArrowArray& array, int64_t row_index) {
  return impl_->Reset(array, row_index);
}

// ArrowArrayArrayLike Implementation

class ArrowArrayArrayLike::Impl {
 public:
  Impl(const ArrowSchema& schema, const ArrowArray& array, int64_t row_index)
      : schema_(schema), array_(std::cref(array)), row_index_(row_index) {}

  ~Impl() = default;

  Result<ViewValue> GetElement(size_t pos) const {
    if (static_cast<int64_t>(pos) >= length_) {
      return InvalidArgument("Element index {} out of range (length: {})", pos, length_);
    }

    const ArrowArrayView* child_view = array_view_.children[0];
    return ExtractValue(child_view, offset_ + static_cast<int64_t>(pos));
  }

  size_t size() const { return static_cast<size_t>(length_); }

  Status Reset(const ArrowArray& array, int64_t row_index) {
    array_ = std::cref(array);
    row_index_ = row_index;

    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return UpdateOffsets();
  }

  Status Reset(int64_t row_index) {
    row_index_ = row_index;
    return UpdateOffsets();
  }

  Status Init() {
    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewInitFromSchema(&array_view_, &schema_, &error));
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return UpdateOffsets();
  }

 private:
  Status UpdateOffsets() {
    if (row_index_ < 0 || row_index_ >= array_.get().length) {
      return InvalidArgument("Row index {} out of range (length: {})", row_index_,
                             array_.get().length);
    }

    offset_ = ArrowArrayViewListChildOffset(&array_view_, row_index_);
    length_ = ArrowArrayViewListChildOffset(&array_view_, row_index_ + 1) - offset_;
    return {};
  }

  ArrowArrayView array_view_;
  internal::ArrowArrayViewGuard array_view_guard_{&array_view_};

  const ArrowSchema& schema_;
  std::reference_wrapper<const ArrowArray> array_;
  int64_t row_index_;

  int64_t offset_ = 0;
  int64_t length_ = 0;
};

Result<std::unique_ptr<ArrowArrayArrayLike>> ArrowArrayArrayLike::Make(
    const ArrowSchema& schema, const ArrowArray& array, int64_t row_index) {
  auto impl = std::make_unique<Impl>(schema, array, row_index);
  ICEBERG_RETURN_UNEXPECTED(impl->Init());
  return std::unique_ptr<ArrowArrayArrayLike>(new ArrowArrayArrayLike(std::move(impl)));
}

ArrowArrayArrayLike::ArrowArrayArrayLike(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ArrowArrayArrayLike::~ArrowArrayArrayLike() = default;

Result<ViewValue> ArrowArrayArrayLike::GetElement(size_t pos) const {
  return impl_->GetElement(pos);
}

size_t ArrowArrayArrayLike::size() const { return impl_->size(); }

Status ArrowArrayArrayLike::Reset(int64_t row_index) { return impl_->Reset(row_index); }

Status ArrowArrayArrayLike::Reset(const ArrowArray& array, int64_t row_index) {
  return impl_->Reset(array, row_index);
}

// ArrowArrayMapLike Implementation

class ArrowArrayMapLike::Impl {
 public:
  Impl(const ArrowSchema& schema, const ArrowArray& array, int64_t row_index)
      : schema_(schema), array_(std::cref(array)), row_index_(row_index) {}

  ~Impl() = default;

  Result<ViewValue> GetKey(size_t pos) const {
    if (static_cast<int64_t>(pos) >= length_) {
      return InvalidArgument("Key index {} out of range (length: {})", pos, length_);
    }

    const ArrowArrayView* entries_view = array_view_.children[0];
    const ArrowArrayView* keys_view = entries_view->children[0];
    return ExtractValue(keys_view, offset_ + static_cast<int64_t>(pos));
  }

  Result<ViewValue> GetValue(size_t pos) const {
    if (static_cast<int64_t>(pos) >= length_) {
      return InvalidArgument("Value index {} out of range (length: {})", pos, length_);
    }

    const ArrowArrayView* entries_view = array_view_.children[0];
    const ArrowArrayView* values_view = entries_view->children[1];
    return ExtractValue(values_view, offset_ + static_cast<int64_t>(pos));
  }

  size_t size() const { return static_cast<size_t>(length_); }

  Status Reset(const ArrowArray& array, int64_t row_index) {
    array_ = std::cref(array);
    row_index_ = row_index;

    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return UpdateOffsets();
  }

  Status Reset(int64_t row_index) {
    row_index_ = row_index;
    return UpdateOffsets();
  }

  Status Init() {
    ArrowError error;
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewInitFromSchema(&array_view_, &schema_, &error));
    NANOARROW_RETURN_IF_NOT_OK(
        ArrowArrayViewSetArray(&array_view_, &array_.get(), &error));
    return UpdateOffsets();
  }

 private:
  Status UpdateOffsets() {
    if (row_index_ < 0 || row_index_ >= array_.get().length) {
      return InvalidArgument("Row index {} out of range (length: {})", row_index_,
                             array_.get().length);
    }

    offset_ = ArrowArrayViewListChildOffset(&array_view_, row_index_);
    length_ = ArrowArrayViewListChildOffset(&array_view_, row_index_ + 1) - offset_;
    return {};
  }

  ArrowArrayView array_view_;
  internal::ArrowArrayViewGuard array_view_guard_{&array_view_};

  const ArrowSchema& schema_;
  std::reference_wrapper<const ArrowArray> array_;
  int64_t row_index_;

  int64_t offset_ = 0;
  int64_t length_ = 0;
};

Result<std::unique_ptr<ArrowArrayMapLike>> ArrowArrayMapLike::Make(
    const ArrowSchema& schema, const ArrowArray& array, int64_t row_index) {
  auto impl = std::make_unique<Impl>(schema, array, row_index);
  ICEBERG_RETURN_UNEXPECTED(impl->Init());
  return std::unique_ptr<ArrowArrayMapLike>(new ArrowArrayMapLike(std::move(impl)));
}

ArrowArrayMapLike::ArrowArrayMapLike(std::unique_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

ArrowArrayMapLike::~ArrowArrayMapLike() = default;

Result<ViewValue> ArrowArrayMapLike::GetKey(size_t pos) const {
  return impl_->GetKey(pos);
}

Result<ViewValue> ArrowArrayMapLike::GetValue(size_t pos) const {
  return impl_->GetValue(pos);
}

size_t ArrowArrayMapLike::size() const { return impl_->size(); }

Status ArrowArrayMapLike::Reset(int64_t row_index) { return impl_->Reset(row_index); }

Status ArrowArrayMapLike::Reset(const ArrowArray& array, int64_t row_index) {
  return impl_->Reset(array, row_index);
}

}  // namespace iceberg
