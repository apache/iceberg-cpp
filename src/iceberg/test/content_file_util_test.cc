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

#include "iceberg/util/content_file_util.h"

#include <gtest/gtest.h>

#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"

namespace iceberg {

TEST(ContentFileUtilTest, ContentSizeInBytesUsesFileSizeForDataFile) {
  DataFile file{
      .content = DataFile::Content::kData,
      .file_path = "data.parquet",
      .file_format = FileFormatType::kParquet,
      .record_count = 10,
      .file_size_in_bytes = 100,
  };

  EXPECT_FALSE(ContentFileUtil::IsDV(file));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(file), 100);
}

TEST(ContentFileUtilTest, ContentSizeInBytesUsesFileSizeForNonDVDeleteFiles) {
  DataFile positional_delete{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "position-deletes.parquet",
      .file_format = FileFormatType::kParquet,
      .record_count = 10,
      .file_size_in_bytes = 100,
  };
  DataFile equality_delete{
      .content = DataFile::Content::kEqualityDeletes,
      .file_path = "equality-deletes.parquet",
      .file_format = FileFormatType::kParquet,
      .record_count = 10,
      .file_size_in_bytes = 200,
      .equality_ids = {1},
  };

  EXPECT_FALSE(ContentFileUtil::IsDV(positional_delete));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(positional_delete), 100);
  EXPECT_FALSE(ContentFileUtil::IsDV(equality_delete));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(equality_delete), 200);
}

TEST(ContentFileUtilTest, ContentSizeInBytesUsesContentSizeForDVFile) {
  DataFile file{
      .content = DataFile::Content::kPositionDeletes,
      .file_path = "deletes.puffin",
      .file_format = FileFormatType::kPuffin,
      .record_count = 10,
      .file_size_in_bytes = 100,
      .referenced_data_file = "data.parquet",
      .content_offset = 4,
      .content_size_in_bytes = 42,
  };

  EXPECT_TRUE(ContentFileUtil::IsDV(file));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(file), 42);
}

}  // namespace iceberg
