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

#include "iceberg/location_provider.h"

#include <gtest/gtest.h>

#include "iceberg/location_provider.h"
#include "iceberg/partition_spec.h"
#include "iceberg/row/partition_values.h"
#include "iceberg/table_properties.h"
#include "iceberg/test/matchers.h"
#include "iceberg/transform.h"

namespace iceberg {

namespace {

// Helper function to split a string by delimiter
std::vector<std::string> SplitString(const std::string& str, char delimiter) {
  std::vector<std::string> result;
  std::stringstream ss(str);
  std::string item;

  while (std::getline(ss, item, delimiter)) {
    result.push_back(item);
  }

  return result;
}

}  // namespace

class LocationProviderTest : public ::testing::Test {
 protected:
  void SetUp() override { table_location_ = "/test/table/location"; }

  std::string table_location_;
  TableProperties properties_;
};

TEST_F(LocationProviderTest, DefaultLocationProvider) {
  properties_ = {};  // Empty properties to use defaults
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  auto location = provider->NewDataLocation("my_file");
  EXPECT_EQ(std::format("{}/data/my_file", table_location_), location);
}

TEST_F(LocationProviderTest, DefaultLocationProviderWithCustomDataLocation) {
  std::ignore =
      properties_.Set(TableProperties::kWriteDataLocation, std::string("new_location"));
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  auto location = provider->NewDataLocation("my_file");
  EXPECT_EQ("new_location/my_file", location);
}

TEST_F(LocationProviderTest, ObjectStorageLocationProvider) {
  std::ignore = properties_.Set(TableProperties::kObjectStoreEnabled, true);
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  auto location = provider->NewDataLocation("test.parquet");
  std::string relative_location = location;
  if (relative_location.starts_with(table_location_)) {
    relative_location = relative_location.substr(table_location_.size());
  }

  std::vector<std::string> parts = SplitString(relative_location, '/');
  ASSERT_EQ(7, parts.size());
  EXPECT_EQ("", parts[0]);
  EXPECT_EQ("data", parts[1]);
  for (int i = 2; i <= 5; i++) {
    EXPECT_FALSE(parts[i].empty());
  }
  EXPECT_EQ("test.parquet", parts[6]);
}

TEST_F(LocationProviderTest, ObjectStorageWithPartition) {
  std::ignore = properties_.Set(TableProperties::kObjectStoreEnabled, true);
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  ICEBERG_UNWRAP_OR_FAIL(
      auto mock_spec,
      PartitionSpec::Make(PartitionSpec::kInitialSpecId,
                          {PartitionField(1, 1, "data#1", Transform::Identity())},
                          PartitionSpec::kInvalidPartitionFieldId + 1));
  PartitionValues mock_partition_data({Literal::String("val#1")});
  ICEBERG_UNWRAP_OR_FAIL(
      auto location,
      provider->NewDataLocation(*mock_spec, mock_partition_data, "test.parquet"));

  std::vector<std::string> parts = SplitString(location, '/');
  ASSERT_GT(parts.size(), 2);
  EXPECT_EQ("data%231=val%231", parts[parts.size() - 2]);
}

TEST_F(LocationProviderTest, ObjectStorageExcludePartitionInPath) {
  std::ignore = properties_.Set(TableProperties::kObjectStoreEnabled, true)
                    .Set(TableProperties::kWriteObjectStorePartitionedPaths, false);
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  auto location = provider->NewDataLocation("test.parquet");

  EXPECT_THAT(location, testing::HasSubstr(table_location_));
  EXPECT_THAT(location, testing::HasSubstr("/data/"));
  EXPECT_THAT(location, testing::HasSubstr("-test.parquet"));
}

TEST_F(LocationProviderTest, HashInjection) {
  std::ignore = properties_.Set(TableProperties::kObjectStoreEnabled, true);
  ICEBERG_UNWRAP_OR_FAIL(auto provider,
                         LocationProvider::Make(table_location_, properties_));

  auto location_a = provider->NewDataLocation("a");
  EXPECT_THAT(location_a, testing::EndsWith("/data/0101/0110/1001/10110010/a"));

  auto location_b = provider->NewDataLocation("b");
  EXPECT_THAT(location_b, testing::EndsWith("/data/1110/0111/1110/00000011/b"));

  auto location_c = provider->NewDataLocation("c");
  EXPECT_THAT(location_c, testing::EndsWith("/data/0010/1101/0110/01011111/c"));

  auto location_d = provider->NewDataLocation("d");
  EXPECT_THAT(location_d, testing::EndsWith("/data/1001/0001/0100/01110011/d"));
}

}  // namespace iceberg
