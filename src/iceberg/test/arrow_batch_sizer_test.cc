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

#include <array>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "iceberg/data/arrow_batch_sizer_internal.h"
#include "iceberg/test/matchers.h"

namespace iceberg::internal {

namespace {

// Owns a schema and an array built with nanoarrow.
struct OwnedBatch {
  ArrowSchema schema{};
  ArrowArray array{};

  OwnedBatch() = default;
  OwnedBatch(const OwnedBatch&) = delete;
  OwnedBatch& operator=(const OwnedBatch&) = delete;
  ~OwnedBatch() {
    if (array.release != nullptr) {
      array.release(&array);
    }
    if (schema.release != nullptr) {
      schema.release(&schema);
    }
  }
};

void InitPrimitive(OwnedBatch& batch, ArrowType type) {
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, type), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
}

void Finish(OwnedBatch& batch) {
  ASSERT_EQ(ArrowArrayFinishBuildingDefault(&batch.array, nullptr), NANOARROW_OK);
}

Result<int64_t> Measure(const OwnedBatch& batch) {
  ICEBERG_ASSIGN_OR_RAISE(auto sizer, ArrowBatchSizer::Make(batch.schema));
  return sizer->Measure(batch.array);
}

// A release callback for hand-built arrays whose members are not heap allocated.
void ReleaseNothing(ArrowArray* array) { array->release = nullptr; }

}  // namespace

TEST(ArrowBatchSizerTest, FixedWidthWithAndWithoutNulls) {
  OwnedBatch no_nulls;
  InitPrimitive(no_nulls, NANOARROW_TYPE_INT32);
  for (int32_t value = 0; value < 10; ++value) {
    ASSERT_EQ(ArrowArrayAppendInt(&no_nulls.array, value), NANOARROW_OK);
  }
  Finish(no_nulls);
  // nanoarrow omits the validity buffer when there are no nulls.
  EXPECT_THAT(Measure(no_nulls), HasValue(::testing::Eq(40)));

  OwnedBatch with_nulls;
  InitPrimitive(with_nulls, NANOARROW_TYPE_INT32);
  for (int32_t value = 0; value < 10; ++value) {
    ASSERT_EQ(ArrowArrayAppendInt(&with_nulls.array, value), NANOARROW_OK);
  }
  ASSERT_EQ(ArrowArrayAppendNull(&with_nulls.array, 1), NANOARROW_OK);
  Finish(with_nulls);
  // 11 values: 2 validity bytes and 44 data bytes.
  EXPECT_THAT(Measure(with_nulls), HasValue(::testing::Eq(2 + 44)));
}

TEST(ArrowBatchSizerTest, SlicedArrayCountsBuffersFromZero) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_INT64);
  for (int64_t value = 0; value < 8; ++value) {
    ASSERT_EQ(ArrowArrayAppendInt(&batch.array, value), NANOARROW_OK);
  }
  Finish(batch);
  batch.array.offset = 3;
  batch.array.length = 5;
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(64)));
}

TEST(ArrowBatchSizerTest, VariableWidth) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_STRING);
  ASSERT_EQ(ArrowArrayAppendString(&batch.array, ArrowCharView("abc")), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendString(&batch.array, ArrowCharView("")), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendString(&batch.array, ArrowCharView("hello")), NANOARROW_OK);
  Finish(batch);
  // 4 offsets of 4 bytes, then 8 data bytes.
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(16 + 8)));
}

TEST(ArrowBatchSizerTest, NestedStructAndList) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&batch.schema, 2), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(batch.schema.children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(batch.schema.children[0], "id"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(batch.schema.children[1], NANOARROW_TYPE_LIST),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(batch.schema.children[1], "values"), NANOARROW_OK);
  ASSERT_EQ(
      ArrowSchemaSetType(batch.schema.children[1]->children[0], NANOARROW_TYPE_INT16),
      NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);

  ArrowArray* ids = batch.array.children[0];
  ArrowArray* lists = batch.array.children[1];
  for (int32_t row = 0; row < 3; ++row) {
    ASSERT_EQ(ArrowArrayAppendInt(ids, row), NANOARROW_OK);
    for (int32_t element = 0; element <= row; ++element) {
      ASSERT_EQ(ArrowArrayAppendInt(lists->children[0], element), NANOARROW_OK);
    }
    ASSERT_EQ(ArrowArrayFinishElement(lists), NANOARROW_OK);
    ASSERT_EQ(ArrowArrayFinishElement(&batch.array), NANOARROW_OK);
  }
  Finish(batch);
  // ids: 12 bytes. list offsets: 4 * 4 bytes. list values: 6 int16 = 12 bytes.
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(12 + 16 + 12)));
}

TEST(ArrowBatchSizerTest, ZeroColumnStructIsZeroBytes) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&batch.schema, 0), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
  for (int row = 0; row < 7; ++row) {
    ASSERT_EQ(ArrowArrayFinishElement(&batch.array), NANOARROW_OK);
  }
  Finish(batch);
  ASSERT_EQ(batch.array.length, 7);
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(0)));
}

TEST(ArrowBatchSizerTest, DictionaryIsCharged) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_INT8), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateDictionary(&batch.schema), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(batch.schema.dictionary, NANOARROW_TYPE_STRING),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  // Starting the indices also starts the dictionary.
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendString(batch.array.dictionary, ArrowCharView("red")),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendString(batch.array.dictionary, ArrowCharView("blue")),
            NANOARROW_OK);
  for (int index : {0, 1, 1, 0}) {
    ASSERT_EQ(ArrowArrayAppendInt(&batch.array, index), NANOARROW_OK);
  }
  Finish(batch);
  // Indices: 4 bytes. Dictionary: 3 offsets * 4 bytes + 7 data bytes.
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(4 + 12 + 7)));
}

TEST(ArrowBatchSizerTest, StringViewChargesVariadicBuffers) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_STRING_VIEW);
  // Strings of at most 12 bytes are inlined in the view.
  ASSERT_EQ(ArrowArrayAppendString(&batch.array, ArrowCharView("short")), NANOARROW_OK);
  const std::string long_value(40, 'x');
  ASSERT_EQ(ArrowArrayAppendString(&batch.array, ArrowCharView(long_value.c_str())),
            NANOARROW_OK);
  Finish(batch);
  ASSERT_GE(batch.array.n_buffers, 3);
  const int64_t variadic_buffers = batch.array.n_buffers - 3;
  ASSERT_EQ(variadic_buffers, 1);
  // Two 16-byte views, one 40-byte variadic buffer, and one 8-byte size entry.
  EXPECT_THAT(Measure(batch), HasValue(::testing::Eq(32 + 40 + 8)));
}

TEST(ArrowBatchSizerTest, ListViewIsUnsupported) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_LIST_VIEW),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(batch.schema.children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
  Finish(batch);
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kNotSupported));
}

TEST(ArrowBatchSizerTest, RejectsNegativeLengthAndOffset) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_INT32);
  Finish(batch);
  batch.array.length = -1;
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kInvalidArrowData));
  batch.array.length = 0;
  batch.array.offset = -1;
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kInvalidArrowData));
  batch.array.offset = 0;
}

TEST(ArrowBatchSizerTest, RejectsOverflowBeforeSizingBuffers) {
  ArrowSchema schema{};
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_INT64), NANOARROW_OK);
  ICEBERG_UNWRAP_OR_FAIL(auto sizer, ArrowBatchSizer::Make(schema));
  schema.release(&schema);

  // The buffers are never dereferenced: the size checks fail first.
  std::array<const void*, 2> buffers = {nullptr, nullptr};
  ArrowArray array{};
  array.n_buffers = 2;
  array.buffers = buffers.data();
  array.release = ReleaseNothing;

  // offset + length overflows.
  array.offset = std::numeric_limits<int64_t>::max();
  array.length = 1;
  EXPECT_THAT(sizer->Measure(array), IsError(ErrorKind::kInvalidArrowData));

  // (offset + length) * 64 bits overflows.
  array.offset = 0;
  array.length = std::numeric_limits<int64_t>::max() / 8;
  EXPECT_THAT(sizer->Measure(array), IsError(ErrorKind::kInvalidArrowData));
}

TEST(ArrowBatchSizerTest, RejectsOffsetBufferOverflow) {
  ArrowSchema schema{};
  ASSERT_EQ(ArrowSchemaInitFromType(&schema, NANOARROW_TYPE_LARGE_STRING), NANOARROW_OK);
  ICEBERG_UNWRAP_OR_FAIL(auto sizer, ArrowBatchSizer::Make(schema));
  schema.release(&schema);

  std::array<const void*, 3> buffers = {nullptr, nullptr, nullptr};
  ArrowArray array{};
  array.n_buffers = 3;
  array.buffers = buffers.data();
  array.release = ReleaseNothing;
  // (length + 1) * 8 offset bytes overflows.
  array.length = std::numeric_limits<int64_t>::max() / 8;
  EXPECT_THAT(sizer->Measure(array), IsError(ErrorKind::kInvalidArrowData));
}

TEST(ArrowBatchSizerTest, RejectsFixedSizeListChildLengthOverflow) {
  ArrowSchema schema{};
  ArrowSchemaInit(&schema);
  ASSERT_EQ(ArrowSchemaSetTypeFixedSize(&schema, NANOARROW_TYPE_FIXED_SIZE_LIST, 1 << 20),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema.children[0], NANOARROW_TYPE_INT8), NANOARROW_OK);
  ICEBERG_UNWRAP_OR_FAIL(auto sizer, ArrowBatchSizer::Make(schema));
  schema.release(&schema);

  std::array<const void*, 2> child_buffers = {nullptr, nullptr};
  ArrowArray child{};
  child.n_buffers = 2;
  child.buffers = child_buffers.data();
  child.release = ReleaseNothing;
  std::array<ArrowArray*, 1> children = {&child};

  std::array<const void*, 1> buffers = {nullptr};
  ArrowArray array{};
  array.n_buffers = 1;
  array.buffers = buffers.data();
  array.n_children = 1;
  array.children = children.data();
  array.release = ReleaseNothing;
  // length * 2^20 child elements overflows.
  array.length = std::numeric_limits<int64_t>::max() / 1024;
  EXPECT_THAT(sizer->Measure(array), IsError(ErrorKind::kInvalidArrowData));
}

TEST(ArrowBatchSizerTest, RejectsMismatchedBufferAndChildCounts) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_STRING);
  Finish(batch);
  const int64_t buffers = batch.array.n_buffers;
  batch.array.n_buffers = 2;
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kInvalidArrowData));
  batch.array.n_buffers = buffers;

  const int64_t children = batch.array.n_children;
  batch.array.n_children = 1;
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kInvalidArrowData));
  batch.array.n_children = children;
}

TEST(ArrowBatchSizerTest, RejectsBinaryViewWithoutSizeBuffer) {
  OwnedBatch batch;
  InitPrimitive(batch, NANOARROW_TYPE_BINARY_VIEW);
  Finish(batch);
  const int64_t buffers = batch.array.n_buffers;
  // Two buffers would make nanoarrow read the view buffer as variadic sizes.
  batch.array.n_buffers = 2;
  EXPECT_THAT(Measure(batch), IsError(ErrorKind::kInvalidArrowData));
  batch.array.n_buffers = buffers;
}

TEST(ArrowBatchSizerTest, RejectsVariadicBufferCountOverflow) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_STRING_VIEW),
            NANOARROW_OK);
  const void* buffers[] = {nullptr};
  batch.array.release = ReleaseNothing;
  batch.array.n_buffers = static_cast<int64_t>(std::numeric_limits<int32_t>::max()) + 1;
  batch.array.buffers = buffers;
  auto result = Measure(batch);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArrowData));
  EXPECT_THAT(result, HasErrorMessage("too many variadic buffers"));
}

TEST(ArrowBatchSizerTest, ChildrenNeedNoReleaseCallback) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&batch.schema, 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaInitFromType(batch.schema.children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayAppendInt(batch.array.children[0], 7), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayFinishElement(&batch.array), NANOARROW_OK);
  Finish(batch);

  ICEBERG_UNWRAP_OR_FAIL(auto sizer, ArrowBatchSizer::Make(batch.schema));
  // Only the root's release callback is required; parents release their children.
  auto child_release = batch.array.children[0]->release;
  batch.array.children[0]->release = nullptr;
  EXPECT_THAT(sizer->Measure(batch.array), HasValue(::testing::Eq(4)));
  batch.array.children[0]->release = child_release;
}

TEST(ArrowBatchSizerTest, ErrorsNameTheNestedArray) {
  OwnedBatch batch;
  ASSERT_EQ(ArrowSchemaInitFromType(&batch.schema, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&batch.schema, 2), NANOARROW_OK);
  for (int index = 0; index < 2; ++index) {
    ASSERT_EQ(ArrowSchemaInitFromType(batch.schema.children[index], NANOARROW_TYPE_INT32),
              NANOARROW_OK);
  }
  ASSERT_EQ(ArrowArrayInitFromSchema(&batch.array, &batch.schema, nullptr), NANOARROW_OK);
  ASSERT_EQ(ArrowArrayStartAppending(&batch.array), NANOARROW_OK);
  Finish(batch);
  batch.array.children[1]->length = -1;
  EXPECT_THAT(Measure(batch), HasErrorMessage("root.1"));
  batch.array.children[1]->length = 0;
}

TEST(ArrowBatchSizerTest, ReusesSizerAcrossBatches) {
  OwnedBatch first;
  InitPrimitive(first, NANOARROW_TYPE_INT32);
  ASSERT_EQ(ArrowArrayAppendInt(&first.array, 1), NANOARROW_OK);
  Finish(first);

  OwnedBatch second;
  InitPrimitive(second, NANOARROW_TYPE_INT32);
  for (int32_t value = 0; value < 100; ++value) {
    ASSERT_EQ(ArrowArrayAppendInt(&second.array, value), NANOARROW_OK);
  }
  Finish(second);

  ICEBERG_UNWRAP_OR_FAIL(auto sizer, ArrowBatchSizer::Make(first.schema));
  EXPECT_THAT(sizer->Measure(first.array), HasValue(::testing::Eq(4)));
  EXPECT_THAT(sizer->Measure(second.array), HasValue(::testing::Eq(400)));
  EXPECT_THAT(sizer->Measure(first.array), HasValue(::testing::Eq(4)));
}

}  // namespace iceberg::internal
