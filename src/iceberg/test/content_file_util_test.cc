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

TEST(ContentFileUtilTest, ContentSizeInBytesUsesFileSizeForNonDVFiles) {
  // Regression test: content_size_in_bytes is only meaningful for deletion vectors.
  // A regular data/delete file that happens to carry a content_size_in_bytes value
  // different from file_size_in_bytes must still report file_size_in_bytes.
  DataFile file{
      .content = DataFile::Content::kPositionDeletes,
      .file_format = FileFormatType::kParquet,
      .file_size_in_bytes = 100,
      .content_size_in_bytes = 999,
  };

  EXPECT_FALSE(ContentFileUtil::IsDV(file));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(file), 100);
}

TEST(ContentFileUtilTest, ContentSizeInBytesUsesContentSizeForDVFiles) {
  DataFile file{
      .content = DataFile::Content::kPositionDeletes,
      .file_format = FileFormatType::kPuffin,
      .file_size_in_bytes = 100,
      .content_size_in_bytes = 42,
  };

  EXPECT_TRUE(ContentFileUtil::IsDV(file));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(file), 42);
}

TEST(ContentFileUtilTest, ContentSizeInBytesFallsBackToFileSizeWhenDVSizeMissing) {
  DataFile file{
      .content = DataFile::Content::kPositionDeletes,
      .file_format = FileFormatType::kPuffin,
      .file_size_in_bytes = 100,
  };

  EXPECT_TRUE(ContentFileUtil::IsDV(file));
  EXPECT_EQ(ContentFileUtil::ContentSizeInBytes(file), 100);
}

}  // namespace iceberg
