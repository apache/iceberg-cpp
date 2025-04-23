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

#include <format>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <gmock/gmock-matchers.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/statistics_file.h"
#include "iceberg/util/formatter_internal.h"

namespace iceberg {

// Tests for the std::format specializations
TEST(FormatterTest, VectorFormat) {
  std::vector<int> empty;
  EXPECT_EQ("[]", std::format("{}", empty));

  std::vector<int> nums = {1, 2, 3, 4, 5};
  EXPECT_EQ("[1, 2, 3, 4, 5]", std::format("{}", nums));

  std::vector<std::string> names = {"Alice", "Bob", "Charlie"};
  EXPECT_EQ("[Alice, Bob, Charlie]", std::format("{}", names));
}

TEST(FormatterTest, MapFormat) {
  std::map<std::string, int> empty;
  EXPECT_EQ("{}", std::format("{}", empty));

  std::map<std::string, int> ages = {{"Alice", 30}, {"Bob", 25}, {"Charlie", 35}};
  EXPECT_EQ("{Alice: 30, Bob: 25, Charlie: 35}", std::format("{}", ages));
}

TEST(FormatterTest, UnorderedMapFormat) {
  std::unordered_map<std::string, double> empty;
  EXPECT_EQ("{}", std::format("{}", empty));

  std::unordered_map<std::string, double> scores = {
      {"Alice", 95.5}, {"Bob", 87.0}, {"Charlie", 92.3}};
  std::string str = std::format("{}", scores);
  EXPECT_THAT(str, ::testing::HasSubstr("Alice: 95.5"));
  EXPECT_THAT(str, ::testing::HasSubstr("Bob: 87"));
  EXPECT_THAT(str, ::testing::HasSubstr("Charlie: 92.3"));
}

TEST(FormatterTest, NestedContainersFormat) {
  std::vector<std::map<std::string, int>> nested = {{{"a", 1}, {"b", 2}},
                                                    {{"c", 3}, {"d", 4}}};

  EXPECT_EQ("[{a: 1, b: 2}, {c: 3, d: 4}]", std::format("{}", nested));

  std::map<std::string, std::vector<int>> nested_map = {
      {"primes", {2, 3, 5, 7, 11}}, {"fibonacci", {1, 1, 2, 3, 5, 8, 13}}};
  std::string result = std::format("{}", nested_map);
  EXPECT_THAT(result, ::testing::HasSubstr("primes"));
  EXPECT_THAT(result, ::testing::HasSubstr("fibonacci"));
  EXPECT_THAT(result, ::testing::HasSubstr("[2, 3, 5, 7, 11]"));
  EXPECT_THAT(result, ::testing::HasSubstr("[1, 1, 2, 3, 5, 8, 13]"));
}

TEST(FormatterTest, EdgeCasesFormat) {
  std::vector<int> single_vec = {42};
  EXPECT_EQ("[42]", std::format("{}", single_vec));

  std::map<std::string, int> single_map = {{"key", 42}};
  EXPECT_EQ("{key: 42}", std::format("{}", single_map));

  std::vector<std::vector<int>> nested_empty = {{}, {1, 2}, {}};
  EXPECT_EQ("[[], [1, 2], []]", std::format("{}", nested_empty));
}

TEST(FormatterTest, SmartPointerFormat) {
  std::vector<std::shared_ptr<int>> int_ptrs = {
      std::make_shared<int>(42),
      std::make_shared<int>(123),
      nullptr,
  };
  EXPECT_EQ("[42, 123, null]", std::format("{}", int_ptrs));

  std::vector<std::shared_ptr<std::string>> str_ptrs = {
      std::make_shared<std::string>("hello"),
      std::make_shared<std::string>("world"),
      nullptr,
  };
  EXPECT_EQ("[hello, world, null]", std::format("{}", str_ptrs));

  std::map<std::string, std::shared_ptr<int>> map_with_ptr_values = {
      {"one", std::make_shared<int>(1)},
      {"two", std::make_shared<int>(2)},
      {"null", nullptr},
  };
  EXPECT_EQ("{null: null, one: 1, two: 2}", std::format("{}", map_with_ptr_values));

  std::unordered_map<std::string, std::shared_ptr<double>> scores = {
      {"Alice", std::make_shared<double>(95.5)},
      {"Bob", std::make_shared<double>(87.0)},
      {"Charlie", nullptr},
  };
  std::string str = std::format("{}", scores);
  EXPECT_THAT(str, ::testing::HasSubstr("Alice: 95.5"));
  EXPECT_THAT(str, ::testing::HasSubstr("Bob: 87"));
  EXPECT_THAT(str, ::testing::HasSubstr("Charlie: null"));

  std::vector<std::map<std::string, std::shared_ptr<int>>> nested = {
      {{"a", std::make_shared<int>(1)}, {"b", std::make_shared<int>(2)}},
      {{"c", std::make_shared<int>(3)}, {"d", nullptr}},
  };
  EXPECT_EQ("[{a: 1, b: 2}, {c: 3, d: null}]", std::format("{}", nested));
}

TEST(FormatterTest, StatisticsFileFormat) {
  StatisticsFile statistics_file{
      .snapshot_id = 123,
      .path = "test_path",
      .file_size_in_bytes = 100,
      .file_footer_size_in_bytes = 20,
      .blob_metadata = {BlobMetadata{.type = "type1",
                                     .source_snapshot_id = 1,
                                     .source_snapshot_sequence_number = 1,
                                     .fields = {1, 2, 3},
                                     .properties = {{"key1", "value1"}}},
                        BlobMetadata{.type = "type2",
                                     .source_snapshot_id = 2,
                                     .source_snapshot_sequence_number = 2,
                                     .fields = {4, 5, 6},
                                     .properties = {}}}};

  const std::string expected =
      "StatisticsFile["
      "snapshotId=123,path=test_path,fileSizeInBytes=100,fileFooterSizeInBytes=20,"
      "blobMetadata=["
      "BlobMetadata[type='type1',sourceSnapshotId=1,sourceSnapshotSequenceNumber=1,"
      "fields=[1, 2, 3],properties={key1: value1}], "
      "BlobMetadata[type='type2',sourceSnapshotId=2,sourceSnapshotSequenceNumber=2,"
      "fields=[4, 5, 6],properties={}]"
      "]"
      "]";
  EXPECT_EQ(expected, std::format("{}", statistics_file));
}

}  // namespace iceberg
