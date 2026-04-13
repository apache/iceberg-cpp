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
#include "iceberg/util/file_io_util.h"

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
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  auto result = FileIORegistry::Load(impl_name, {});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);

  // Verify the loaded FileIO works
  auto read_result = result.value()->ReadFile("any_file", std::nullopt);
  ASSERT_THAT(read_result, IsOk());
  EXPECT_EQ(read_result.value(), "mock");
}

TEST(FileIoRegistryTest, LoadNonExistentReturnsError) {
  auto result = FileIORegistry::Load("com.nonexistent.FileIO", {});
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
  EXPECT_THAT(result, HasErrorMessage("FileIO implementation not found"));
}

TEST(FileIoRegistryTest, OverrideExistingRegistration) {
  const std::string impl_name = "com.test.OverrideFileIO";

  // Register first implementation
  FileIORegistry::Register(
      impl_name,
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // Override with a different factory
  FileIORegistry::Register(
      impl_name,
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // Should still work (the override replaces the original)
  auto result = FileIORegistry::Load(impl_name, {});
  ASSERT_THAT(result, IsOk());
  EXPECT_NE(result.value(), nullptr);
}

TEST(FileIoRegistryTest, FactoryReceivesProperties) {
  const std::string impl_name = "com.test.PropCheckFileIO";
  std::unordered_map<std::string, std::string> captured_properties;

  FileIORegistry::Register(
      impl_name,
      [&captured_properties](
          const std::unordered_map<std::string, std::string>& properties)
          -> Result<std::unique_ptr<FileIO>> {
        captured_properties = properties;
        return std::make_unique<MockFileIO>();
      });

  std::unordered_map<std::string, std::string> props = {{"key1", "val1"},
                                                        {"key2", "val2"}};
  auto result = FileIORegistry::Load(impl_name, props);
  ASSERT_THAT(result, IsOk());
  EXPECT_EQ(captured_properties.at("key1"), "val1");
  EXPECT_EQ(captured_properties.at("key2"), "val2");
}

TEST(FileIOUtilTest, DetectFileIONameFromScheme) {
  EXPECT_EQ(FileIOUtil::DetectFileIOName("s3://bucket/path"),
            FileIORegistry::kArrowS3FileIO);
  EXPECT_EQ(FileIOUtil::DetectFileIOName("/tmp/warehouse"),
            FileIORegistry::kArrowLocalFileIO);
  EXPECT_EQ(FileIOUtil::DetectFileIOName("file:///tmp/warehouse"),
            FileIORegistry::kArrowLocalFileIO);
}

TEST(FileIOUtilTest, CreateFileIOMissingImpl) {
  auto result = FileIOUtil::CreateFileIO({});
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST(FileIOUtilTest, CreateFileIORejectsIncompatibleWarehouse) {
  // Register a mock "s3" factory so the registry lookup would succeed
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // io-impl=s3 with a local warehouse path should fail-fast
  std::unordered_map<std::string, std::string> props = {
      {"io-impl", "s3"},
      {"warehouse", "/tmp/warehouse"},
  };
  auto result = FileIOUtil::CreateFileIO(props);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("incompatible"));
}

TEST(FileIOUtilTest, CreateFileIOAllowsCompatibleWarehouse) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowS3FileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // io-impl=s3 with an s3:// warehouse should succeed
  std::unordered_map<std::string, std::string> props = {
      {"io-impl", "s3"},
      {"warehouse", "s3://my-bucket/warehouse"},
  };
  auto result = FileIOUtil::CreateFileIO(props);
  ASSERT_THAT(result, IsOk());
}

TEST(FileIOUtilTest, CreateFileIOPassesThroughCustomImpl) {
  const std::string custom_impl = "com.mycompany.CustomFileIO";
  FileIORegistry::Register(
      custom_impl,
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // Custom io-impl with a local warehouse should NOT be rejected
  std::unordered_map<std::string, std::string> props = {
      {"io-impl", custom_impl},
      {"warehouse", "/tmp/warehouse"},
  };
  auto result = FileIOUtil::CreateFileIO(props);
  ASSERT_THAT(result, IsOk());
}

TEST(FileIOUtilTest, CreateFileIOUnregisteredCustomImplReturnsNotFound) {
  // Unregistered custom io-impl should get NotFound, not InvalidArgument
  std::unordered_map<std::string, std::string> props = {
      {"io-impl", "com.nonexistent.FileIO"},
      {"warehouse", "/tmp/warehouse"},
  };
  auto result = FileIOUtil::CreateFileIO(props);
  EXPECT_THAT(result, IsError(ErrorKind::kNotFound));
}

TEST(FileIOUtilTest, CreateFileIOSkipsCheckWhenWarehouseAbsent) {
  FileIORegistry::Register(
      std::string(FileIORegistry::kArrowLocalFileIO),
      [](const std::unordered_map<std::string, std::string>& /*properties*/)
          -> Result<std::unique_ptr<FileIO>> { return std::make_unique<MockFileIO>(); });

  // No warehouse property — should just go through to the factory
  std::unordered_map<std::string, std::string> props = {
      {"io-impl", "local"},
  };
  auto result = FileIOUtil::CreateFileIO(props);
  ASSERT_THAT(result, IsOk());
}

}  // namespace iceberg
