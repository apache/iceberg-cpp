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

#include "iceberg/util/data_file_set.h"

#include <gtest/gtest.h>

#include "iceberg/file_format.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/row/partition_values.h"

namespace iceberg {

class DataFileSetTest : public ::testing::Test {
 protected:
  std::shared_ptr<DataFile> CreateDataFile(const std::string& path, int64_t size = 100) {
    auto file = std::make_shared<DataFile>();
    file->file_path = path;
    file->file_format = FileFormatType::kParquet;
    file->file_size_in_bytes = size;
    file->record_count = 10;
    file->content = DataFile::Content::kData;
    return file;
  }
};

TEST_F(DataFileSetTest, EmptySet) {
  DataFileSet set;
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
  EXPECT_EQ(set.begin(), set.end());
  EXPECT_TRUE(set.as_span().empty());
}

TEST_F(DataFileSetTest, InsertSingleFile) {
  DataFileSet set;
  auto file = CreateDataFile("/path/to/file.parquet");

  auto [iter, inserted] = set.insert(file);
  EXPECT_TRUE(inserted);
  EXPECT_EQ(*iter, file);
  EXPECT_FALSE(set.empty());
  EXPECT_EQ(set.size(), 1);
}

TEST_F(DataFileSetTest, InsertDuplicateFile) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file.parquet");
  auto file2 = CreateDataFile("/path/to/file.parquet");  // Same path

  auto [iter1, inserted1] = set.insert(file1);
  EXPECT_TRUE(inserted1);

  auto [iter2, inserted2] = set.insert(file2);
  EXPECT_FALSE(inserted2);
  EXPECT_EQ(iter1, iter2);   // Should point to the same element
  EXPECT_EQ(set.size(), 1);  // Should still be size 1
}

TEST_F(DataFileSetTest, InsertDifferentFiles) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");
  auto file3 = CreateDataFile("/path/to/file3.parquet");

  set.insert(file1);
  set.insert(file2);
  set.insert(file3);

  EXPECT_EQ(set.size(), 3);
  EXPECT_FALSE(set.empty());
}

TEST_F(DataFileSetTest, InsertionOrderPreserved) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");
  auto file3 = CreateDataFile("/path/to/file3.parquet");

  set.insert(file1);
  set.insert(file2);
  set.insert(file3);

  // Iterate and verify order
  std::vector<std::string> paths;
  for (const auto& file : set) {
    paths.push_back(file->file_path);
  }

  EXPECT_EQ(paths.size(), 3);
  EXPECT_EQ(paths[0], "/path/to/file1.parquet");
  EXPECT_EQ(paths[1], "/path/to/file2.parquet");
  EXPECT_EQ(paths[2], "/path/to/file3.parquet");
}

TEST_F(DataFileSetTest, AsSpan) {
  DataFileSet set;
  EXPECT_TRUE(set.as_span().empty());

  // Single element
  auto file0 = CreateDataFile("/path/to/file0.parquet");
  set.insert(file0);
  {
    auto span = set.as_span();
    EXPECT_EQ(span.size(), 1);
    EXPECT_EQ(span[0]->file_path, "/path/to/file0.parquet");
    EXPECT_EQ(span[0], file0);  // Same pointer, span is a view
  }

  // Multiple elements
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");
  set.insert(file1);
  set.insert(file2);

  auto span = set.as_span();
  EXPECT_EQ(span.size(), 3);
  EXPECT_EQ(span[0]->file_path, "/path/to/file0.parquet");
  EXPECT_EQ(span[1]->file_path, "/path/to/file1.parquet");
  EXPECT_EQ(span[2]->file_path, "/path/to/file2.parquet");

  // Span matches set iteration order and identity
  size_t i = 0;
  for (const auto& file : set) {
    EXPECT_EQ(span[i], file) << "Span element " << i << " should match set iterator";
    ++i;
  }
  EXPECT_EQ(i, span.size());

  // Span works with range-for
  i = 0;
  for (const auto& file : span) {
    EXPECT_EQ(file->file_path, span[i]->file_path);
    ++i;
  }
  EXPECT_EQ(i, 3);

  set.clear();
  EXPECT_TRUE(set.as_span().empty());
}

TEST_F(DataFileSetTest, InsertDuplicatePreservesOrder) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");
  auto file3 = CreateDataFile("/path/to/file1.parquet");  // Duplicate of file1

  set.insert(file1);
  set.insert(file2);
  set.insert(file3);  // Should not insert, but order should be preserved

  EXPECT_EQ(set.size(), 2);

  std::vector<std::string> paths;
  for (const auto& file : set) {
    paths.push_back(file->file_path);
  }

  EXPECT_EQ(paths[0], "/path/to/file1.parquet");
  EXPECT_EQ(paths[1], "/path/to/file2.parquet");
}

TEST_F(DataFileSetTest, InsertNullFile) {
  DataFileSet set;
  std::shared_ptr<DataFile> null_file = nullptr;

  auto [iter, inserted] = set.insert(null_file);
  EXPECT_FALSE(inserted);
  EXPECT_EQ(iter, set.end());
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
}

TEST_F(DataFileSetTest, InsertMoveSemantics) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");

  // Insert using move
  auto [iter1, inserted1] = set.insert(std::move(file1));
  EXPECT_TRUE(inserted1);
  EXPECT_EQ(file1, nullptr);  // Should be moved

  // Insert using copy
  auto [iter2, inserted2] = set.insert(file2);
  EXPECT_TRUE(inserted2);
  EXPECT_NE(file2, nullptr);  // Should still be valid

  EXPECT_EQ(set.size(), 2);
}

TEST_F(DataFileSetTest, Clear) {
  DataFileSet set;
  set.insert(CreateDataFile("/path/to/file1.parquet"));
  set.insert(CreateDataFile("/path/to/file2.parquet"));

  EXPECT_EQ(set.size(), 2);
  set.clear();
  EXPECT_TRUE(set.empty());
  EXPECT_EQ(set.size(), 0);
  EXPECT_EQ(set.begin(), set.end());
}

TEST_F(DataFileSetTest, IteratorOperations) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file1.parquet");
  auto file2 = CreateDataFile("/path/to/file2.parquet");
  auto file3 = CreateDataFile("/path/to/file3.parquet");

  set.insert(file1);
  set.insert(file2);
  set.insert(file3);

  // Test const iterators
  const auto& const_set = set;
  EXPECT_NE(const_set.begin(), const_set.end());
  EXPECT_NE(const_set.cbegin(), const_set.cend());

  // Test iterator increment
  auto it = set.begin();
  EXPECT_EQ((*it)->file_path, "/path/to/file1.parquet");
  ++it;
  EXPECT_EQ((*it)->file_path, "/path/to/file2.parquet");
  ++it;
  EXPECT_EQ((*it)->file_path, "/path/to/file3.parquet");
  ++it;
  EXPECT_EQ(it, set.end());
}

TEST_F(DataFileSetTest, RangeBasedForLoop) {
  DataFileSet set;
  set.insert(CreateDataFile("/path/to/file1.parquet"));
  set.insert(CreateDataFile("/path/to/file2.parquet"));
  set.insert(CreateDataFile("/path/to/file3.parquet"));

  int count = 0;
  for (const auto& file : set) {
    EXPECT_NE(file, nullptr);
    ++count;
  }
  EXPECT_EQ(count, 3);
}

TEST_F(DataFileSetTest, CaseSensitivePaths) {
  DataFileSet set;
  auto file1 = CreateDataFile("/path/to/file.parquet");
  auto file2 = CreateDataFile("/path/to/FILE.parquet");  // Different case

  set.insert(file1);
  set.insert(file2);

  // Should be treated as different files
  EXPECT_EQ(set.size(), 2);
}

TEST_F(DataFileSetTest, MultipleInsertsSameFile) {
  DataFileSet set;
  auto file = CreateDataFile("/path/to/file.parquet");

  // Insert the same file multiple times
  set.insert(file);
  set.insert(file);
  set.insert(file);

  EXPECT_EQ(set.size(), 1);
}

}  // namespace iceberg
