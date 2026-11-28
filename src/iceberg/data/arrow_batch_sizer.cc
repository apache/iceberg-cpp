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

#include <cstddef>
#include <cstdint>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "iceberg/data/arrow_batch_sizer_internal.h"
#include "iceberg/util/macros.h"

namespace iceberg::internal {

namespace {

constexpr int64_t kInt64Max = std::numeric_limits<int64_t>::max();

// The number of leading buffers a binary or string view array always has.
constexpr int64_t kBinaryViewFixedBuffers = 2;

constexpr int64_t kVariadicSizeBytes = static_cast<int64_t>(sizeof(int64_t));

bool IsBinaryView(ArrowType type) {
  return type == NANOARROW_TYPE_BINARY_VIEW || type == NANOARROW_TYPE_STRING_VIEW;
}

std::optional<int64_t> CheckedAdd(int64_t lhs, int64_t rhs) {
  if (lhs < 0 || rhs < 0 || lhs > kInt64Max - rhs) {
    return std::nullopt;
  }
  return lhs + rhs;
}

std::optional<int64_t> CheckedMultiply(int64_t lhs, int64_t rhs) {
  if (lhs < 0 || rhs < 0) {
    return std::nullopt;
  }
  if (lhs != 0 && rhs > kInt64Max / lhs) {
    return std::nullopt;
  }
  return lhs * rhs;
}

// Returns whether `bytes` can also be represented as a size_t, which nanoarrow and
// the allocator use for buffer sizes.
bool FitsSizeT(int64_t bytes) {
  if constexpr (sizeof(size_t) >= sizeof(int64_t)) {
    return bytes >= 0;
  } else {
    return bytes >= 0 &&
           static_cast<uint64_t>(bytes) <= std::numeric_limits<size_t>::max();
  }
}

// The position of a nested array, e.g. "root.1.dictionary". Only formatted when an
// error is reported, so measuring a valid batch does not allocate strings.
class ArrayPath {
 public:
  static constexpr int64_t kDictionary = -1;

  // Appends a child index (or kDictionary) until the returned scope ends.
  class Scope {
   public:
    Scope(ArrayPath& path, int64_t step) : path_(path) { path_.steps_.push_back(step); }
    ~Scope() { path_.steps_.pop_back(); }
    Scope(const Scope&) = delete;
    Scope& operator=(const Scope&) = delete;

   private:
    ArrayPath& path_;
  };

  std::string ToString() const {
    std::string result = "root";
    for (int64_t step : steps_) {
      result += step == kDictionary ? ".dictionary" : "." + std::to_string(step);
    }
    return result;
  }

 private:
  std::vector<int64_t> steps_;
};

// Returns the minimum byte size nanoarrow derives for fixed buffer `index` from the
// array's offset and length alone, or std::nullopt when the product overflows.
// Buffers whose size depends on data (for example string data) report zero here.
std::optional<int64_t> LayoutBufferBytes(const ArrowLayout& layout, int index,
                                         int64_t offset_plus_length) {
  const int64_t element_size_bits = layout.element_size_bits[index];
  const int64_t element_size_bytes = element_size_bits / 8;
  switch (layout.buffer_type[index]) {
    case NANOARROW_BUFFER_TYPE_VALIDITY:
      return offset_plus_length / 8 + (offset_plus_length % 8 != 0 ? 1 : 0);
    case NANOARROW_BUFFER_TYPE_DATA_OFFSET: {
      if (offset_plus_length == 0) {
        return 0;
      }
      auto offset_count = CheckedAdd(offset_plus_length, 1);
      if (!offset_count.has_value()) {
        return std::nullopt;
      }
      return CheckedMultiply(*offset_count, element_size_bytes);
    }
    case NANOARROW_BUFFER_TYPE_SIZE:
    case NANOARROW_BUFFER_TYPE_VIEW_OFFSET:
    case NANOARROW_BUFFER_TYPE_TYPE_ID:
    case NANOARROW_BUFFER_TYPE_UNION_OFFSET:
      return CheckedMultiply(offset_plus_length, element_size_bytes);
    case NANOARROW_BUFFER_TYPE_DATA: {
      auto bits = CheckedMultiply(offset_plus_length, element_size_bits);
      if (!bits.has_value()) {
        return std::nullopt;
      }
      auto rounded = CheckedAdd(*bits, 7);
      if (!rounded.has_value()) {
        return std::nullopt;
      }
      return *rounded / 8;
    }
    case NANOARROW_BUFFER_TYPE_NONE:
    case NANOARROW_BUFFER_TYPE_VARIADIC_DATA:
    case NANOARROW_BUFFER_TYPE_VARIADIC_SIZE:
      return 0;
  }
  return std::nullopt;
}

int FixedBufferCount(const ArrowArrayView& view) {
  if (IsBinaryView(view.storage_type)) {
    return static_cast<int>(kBinaryViewFixedBuffers);
  }
  int count = 0;
  while (count < NANOARROW_MAX_FIXED_BUFFERS &&
         view.layout.buffer_type[count] != NANOARROW_BUFFER_TYPE_NONE) {
    ++count;
  }
  return count;
}

// Checks everything nanoarrow trusts before it sizes buffers: counts used to index
// array members, and every product that it computes without overflow checks.
Status Preflight(const ArrowArrayView& view, const ArrowArray& array, ArrayPath& path) {
  switch (view.storage_type) {
    case NANOARROW_TYPE_LIST_VIEW:
    case NANOARROW_TYPE_LARGE_LIST_VIEW:
    case NANOARROW_TYPE_RUN_END_ENCODED:
      return NotSupported("Cannot size Arrow {} array at {}: unsupported layout",
                          ArrowTypeString(view.storage_type), path.ToString());
    default:
      break;
  }

  if (array.length < 0 || array.offset < 0) {
    return InvalidArrowData("Arrow array at {} has negative offset {} or length {}",
                            path.ToString(), array.offset, array.length);
  }
  auto offset_plus_length = CheckedAdd(array.offset, array.length);
  if (!offset_plus_length.has_value()) {
    return InvalidArrowData("Arrow array at {} offset {} plus length {} overflows",
                            path.ToString(), array.offset, array.length);
  }

  const int fixed_buffers = FixedBufferCount(view);
  if (IsBinaryView(view.storage_type)) {
    // Validity, views, zero or more variadic data buffers, then the size buffer.
    if (array.n_buffers < kBinaryViewFixedBuffers + 1) {
      return InvalidArrowData("Arrow {} array at {} has {} buffers; expected at least {}",
                              ArrowTypeString(view.storage_type), path.ToString(),
                              array.n_buffers, kBinaryViewFixedBuffers + 1);
    }
    const int64_t variadic_buffers = array.n_buffers - kBinaryViewFixedBuffers - 1;
    // Nanoarrow's buffer accessors add the fixed buffers and size buffer using
    // int32_t arithmetic before returning the total buffer count as int64_t.
    if (variadic_buffers >
        std::numeric_limits<int32_t>::max() - kBinaryViewFixedBuffers - 1) {
      return InvalidArrowData("Arrow array at {} has too many variadic buffers: {}",
                              path.ToString(), variadic_buffers);
    }
    auto size_buffer_bytes = CheckedMultiply(variadic_buffers, kVariadicSizeBytes);
    if (!size_buffer_bytes.has_value() || !FitsSizeT(*size_buffer_bytes)) {
      return InvalidArrowData("Arrow array at {} variadic size buffer overflows",
                              path.ToString());
    }
    if (array.buffers == nullptr ||
        (variadic_buffers > 0 && array.buffers[array.n_buffers - 1] == nullptr)) {
      return InvalidArrowData("Arrow array at {} is missing its variadic size buffer",
                              path.ToString());
    }
  } else if (array.n_buffers != fixed_buffers) {
    return InvalidArrowData("Arrow {} array at {} has {} buffers; expected {}",
                            ArrowTypeString(view.storage_type), path.ToString(),
                            array.n_buffers, fixed_buffers);
  }
  if (array.n_buffers > 0 && array.buffers == nullptr) {
    return InvalidArrowData("Arrow array at {} has a null buffer list", path.ToString());
  }

  for (int index = 0; index < fixed_buffers; ++index) {
    auto bytes = LayoutBufferBytes(view.layout, index, *offset_plus_length);
    if (!bytes.has_value() || !FitsSizeT(*bytes)) {
      return InvalidArrowData(
          "Arrow {} array at {} buffer {} size overflows for offset {} and length {}",
          ArrowTypeString(view.storage_type), path.ToString(), index, array.offset,
          array.length);
    }
  }

  if (view.storage_type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
    auto child_length =
        CheckedMultiply(*offset_plus_length, view.layout.child_size_elements);
    if (!child_length.has_value()) {
      return InvalidArrowData(
          "Arrow fixed_size_list array at {} child length overflows for offset {}, "
          "length {}, and list size {}",
          path.ToString(), array.offset, array.length, view.layout.child_size_elements);
    }
  }

  if (array.n_children != view.n_children) {
    return InvalidArrowData("Arrow array at {} has {} children; expected {}",
                            path.ToString(), array.n_children, view.n_children);
  }
  if (array.n_children > 0 && array.children == nullptr) {
    return InvalidArrowData("Arrow array at {} has a null child list", path.ToString());
  }
  for (int64_t index = 0; index < view.n_children; ++index) {
    ArrayPath::Scope child(path, index);
    if (array.children[index] == nullptr) {
      return InvalidArrowData("Arrow array at {} is null", path.ToString());
    }
    ICEBERG_RETURN_UNEXPECTED(
        Preflight(*view.children[index], *array.children[index], path));
  }

  if ((view.dictionary == nullptr) != (array.dictionary == nullptr)) {
    return InvalidArrowData("Arrow array at {} dictionary does not match its schema",
                            path.ToString());
  }
  if (view.dictionary != nullptr) {
    ArrayPath::Scope dictionary(path, ArrayPath::kDictionary);
    ICEBERG_RETURN_UNEXPECTED(Preflight(*view.dictionary, *array.dictionary, path));
  }
  return {};
}

// Sums the spans of the buffers that nanoarrow sized for `view`.
Result<int64_t> SumBufferSpans(ArrowArrayView& view, ArrayPath& path) {
  int64_t total = 0;
  auto add = [&](int64_t bytes, std::string_view what) -> Status {
    if (bytes < 0) {
      return NotSupported("Cannot size Arrow {} array at {}: unknown {} size",
                          ArrowTypeString(view.storage_type), path.ToString(), what);
    }
    auto sum = CheckedAdd(total, bytes);
    if (!sum.has_value() || !FitsSizeT(*sum)) {
      return InvalidArrowData("Arrow array at {} buffer sizes overflow", path.ToString());
    }
    total = *sum;
    return {};
  };

  const int64_t buffer_count = ArrowArrayViewGetNumBuffers(&view);
  for (int64_t index = 0; index < buffer_count; ++index) {
    ICEBERG_RETURN_UNEXPECTED(
        add(ArrowArrayViewGetBufferView(&view, index).size_bytes, "buffer"));
  }

  for (int64_t index = 0; index < view.n_children; ++index) {
    ArrayPath::Scope child(path, index);
    ICEBERG_ASSIGN_OR_RAISE(auto child_bytes,
                            SumBufferSpans(*view.children[index], path));
    ICEBERG_RETURN_UNEXPECTED(add(child_bytes, "child"));
  }
  if (view.dictionary != nullptr) {
    ArrayPath::Scope dictionary(path, ArrayPath::kDictionary);
    ICEBERG_ASSIGN_OR_RAISE(auto dictionary_bytes,
                            SumBufferSpans(*view.dictionary, path));
    ICEBERG_RETURN_UNEXPECTED(add(dictionary_bytes, "dictionary"));
  }
  return total;
}

}  // namespace

class ArrowBatchSizer::Impl {
 public:
  Impl() = default;
  ~Impl() { ArrowArrayViewReset(&view_); }

  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;

  Status Init(const ArrowSchema& schema) {
    ArrowError error;
    ArrowErrorInit(&error);
    if (ArrowArrayViewInitFromSchema(&view_, &schema, &error) != NANOARROW_OK) {
      return InvalidArrowData("Cannot size batches for Arrow schema: {}", error.message);
    }
    return {};
  }

  Result<int64_t> Measure(const ArrowArray& array) {
    if (array.release == nullptr) {
      return InvalidArrowData("Cannot size a released Arrow array");
    }
    ArrayPath path;
    ICEBERG_RETURN_UNEXPECTED(Preflight(view_, array, path));
    ArrowError error;
    ArrowErrorInit(&error);
    if (ArrowArrayViewSetArray(&view_, &array, &error) != NANOARROW_OK) {
      return InvalidArrowData("Cannot size Arrow batch: {}", error.message);
    }
    return SumBufferSpans(view_, path);
  }

 private:
  ArrowArrayView view_{};
};

Result<std::unique_ptr<ArrowBatchSizer>> ArrowBatchSizer::Make(
    const ArrowSchema& schema) {
  auto impl = std::make_unique<Impl>();
  ICEBERG_RETURN_UNEXPECTED(impl->Init(schema));
  return std::unique_ptr<ArrowBatchSizer>(new ArrowBatchSizer(std::move(impl)));
}

ArrowBatchSizer::ArrowBatchSizer(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

ArrowBatchSizer::~ArrowBatchSizer() = default;

Result<int64_t> ArrowBatchSizer::Measure(const ArrowArray& array) {
  return impl_->Measure(array);
}

}  // namespace iceberg::internal
