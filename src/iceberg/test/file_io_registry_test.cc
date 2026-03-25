// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "iceberg/file_io_registry.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"

namespace iceberg {

namespace {

/// A minimal FileIO implementation for testing.
class MockFileIO : public FileIO {
 public:
  Result<std::string> ReadFile(const std::string& /*file_location*/,
                               std::optional<size_t> /*length*/) override {
    return std::string("mock");
  }

  Status WriteFile(const std::string& /*file_location*/,
                   std::string_view /*content*/) override {
    return {};
  }

  Status DeleteFile(const std::string& /*file_location*/) override { return {}; }
};

}  // namespace

TEST(FileIoRegistryTest, RegisterAndLoad) {
  const std::string impl_name = "com.test.MockFileIO";
  FileIORegistry::Register(
      impl_name,
      [](const std::string& /*warehouse*/,
         const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> { return std::make_shared<MockFileIO>(); });

  auto result = FileIORegistry::Load(impl_name, "/test/warehouse", {});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);

  // Verify the loaded FileIO works
  auto read_result = result.value()->ReadFile("any_file", std::nullopt);
  ASSERT_THAT(read_result, IsOk());
  EXPECT_EQ(read_result.value(), "mock");
}

TEST(FileIoRegistryTest, LoadNonExistentReturnsError) {
  auto result = FileIORegistry::Load("com.nonexistent.FileIO", "/test/warehouse", {});
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
  EXPECT_THAT(result, HasErrorMessage("FileIO implementation not found"));
}

TEST(FileIoRegistryTest, OverrideExistingRegistration) {
  const std::string impl_name = "com.test.OverrideFileIO";

  // Register first implementation
  FileIORegistry::Register(
      impl_name,
      [](const std::string& /*warehouse*/,
         const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> { return std::make_shared<MockFileIO>(); });

  // Override with a different factory
  FileIORegistry::Register(
      impl_name,
      [](const std::string& /*warehouse*/,
         const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::shared_ptr<FileIO>> { return std::make_shared<MockFileIO>(); });

  // Should still work (the override replaces the original)
  auto result = FileIORegistry::Load(impl_name, "/test/warehouse", {});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST(FileIoRegistryTest, FactoryReceivesWarehouseAndProperties) {
  const std::string impl_name = "com.test.PropCheckFileIO";
  std::string captured_warehouse;
  std::unordered_map<std::string, std::string> captured_properties;

  FileIORegistry::Register(
      impl_name,
      [&captured_warehouse, &captured_properties](
          const std::string& warehouse,
          const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::shared_ptr<FileIO>> {
        captured_warehouse = warehouse;
        captured_properties = properties;
        return std::make_shared<MockFileIO>();
      });

  std::unordered_map<std::string, std::string> props = {{"key1", "val1"},
                                                        {"key2", "val2"}};
  auto result = FileIORegistry::Load(impl_name, "s3://my-bucket/warehouse", props);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(captured_warehouse, "s3://my-bucket/warehouse");
  EXPECT_EQ(captured_properties.at("key1"), "val1");
  EXPECT_EQ(captured_properties.at("key2"), "val2");
}

}  // namespace iceberg
