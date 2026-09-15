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

#include "iceberg/deletes/dv_writer.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "iceberg/deletes/dv_writer_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/partition_spec.h"
#include "iceberg/result.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_io.h"
#include "iceberg/util/endian.h"

namespace iceberg {

namespace {

Result<std::optional<PositionDeleteIndex>> NoPreviousDeletes(std::string_view) {
  return std::nullopt;
}

}  // namespace

TEST(DVWriterPreflightTest, RejectsLaterOversizedVectorBeforeCreatingOutput) {
  auto io = std::make_shared<MockFileIO>();
  auto spec = PartitionSpec::Unpartitioned();
  const std::string output_path = "memory://oversized.puffin";

  PositionDeleteIndex small;
  small.Delete(0);
  ICEBERG_UNWRAP_OR_FAIL(auto small_blob, small.Serialize());
  const auto small_length = ReadBigEndian<int32_t>(small_blob.data());
  ASSERT_GT(small_length, 0);
  const size_t max_serialized_length = static_cast<size_t>(small_length);

  PositionDeleteIndex large;
  for (int64_t pos = 0; pos < 100; ++pos) {
    large.Delete(pos * 2);
  }
  ICEBERG_UNWRAP_OR_FAIL(auto large_blob, large.Serialize());
  const auto large_length = ReadBigEndian<int32_t>(large_blob.data());
  ASSERT_GT(large_length, 0);
  ASSERT_GT(static_cast<size_t>(large_length), max_serialized_length);

  ICEBERG_UNWRAP_OR_FAIL(
      auto writer,
      internal::DVWriterFactory::Make(
          DVWriterOptions{.path = output_path,
                          .io = io,
                          .load_previous_deletes = NoPreviousDeletes},
          max_serialized_length));
  ASSERT_THAT(writer->Delete("a.parquet", small, spec, PartitionValues{}), IsOk());
  ASSERT_THAT(writer->Delete("b.parquet", large, spec, PartitionValues{}), IsOk());

  EXPECT_THAT(writer->Close(), IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(io->NewInputFile(output_path), IsError(ErrorKind::kNotFound));
}

}  // namespace iceberg
