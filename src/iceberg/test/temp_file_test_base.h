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

#pragma once

#include <filesystem>
#include <format>
#include <fstream>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

namespace iceberg {

/// A base class for tests that need to create and manage temporary files.
/// Provides utilities for creating platform-independent temporary files
/// and ensures proper cleanup after tests run.
///
/// Usage Example:
/// ```
/// class MyTest : public test::TempFileTestBase {
///  protected:
///   void SetUp() override {
///     // Always call base class SetUp first
///     TempFileTestBase::SetUp();
///
///     // Create test resources
///     my_temp_file_ = CreateNewTempFilePath();
///
///     // Additional setup...
///   }
///
///   // Your test-specific members
///   std::string my_temp_file_;
/// };
///
/// TEST_F(MyTest, ExampleTest) {
///   // Use temporary files in your test
///   WriteContentToFile(my_temp_file_, "test content");
///
///   // Files will be automatically cleaned up in TearDown
/// }
/// ```
///
/// Notes:
/// - Always call TempFileTestBase::SetUp() in your derived class's SetUp()
/// - All files created using the provided methods will be automatically cleaned up
/// - You don't need to implement TearDown() unless you need additional cleanup
class TempFileTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    // Base class setup is empty, but provided for derived classes to call
  }

  void TearDown() override {
    // Clean up all temporary files and directories created during the test
    for (const auto& path : created_temp_files_) {
      std::error_code ec;
      if (std::filesystem::is_directory(path, ec)) {
        std::filesystem::remove_all(path, ec);
      } else {
        std::filesystem::remove(path, ec);
      }
    }
  }

  /// \brief Generates a unique temporary filepath that works across platforms
  std::string GenerateUniqueTempFilePath() const {
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path();
    std::string file_name =
        std::format("iceberg_test_{}_{}.tmp", TestInfo(), GenerateRandomString(8));
    return (temp_dir / file_name).string();
  }

  /// \brief Create a temporary filepath with the specified suffix/extension
  std::string GenerateUniqueTempFilePathWithSuffix(const std::string& suffix) {
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path();
    std::string file_name =
        std::format("iceberg_test_{}_{}{}", TestInfo(), GenerateRandomString(8), suffix);
    return (temp_dir / file_name).string();
  }

  /// \brief Helper to generate a random alphanumeric string for unique filenames
  std::string GenerateRandomString(size_t length) const {
    const std::string_view chars =
        "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, static_cast<int>(chars.size() - 1));

    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
      result += chars[dist(gen)];
    }
    return result;
  }

  /// \brief Get the test name for inclusion in the filename
  std::string TestInfo() const {
    if (const auto info = ::testing::UnitTest::GetInstance()->current_test_info(); info) {
      std::string result = std::format("{}_{}", info->test_suite_name(), info->name());
      // Replace slashes (from parameterized tests) with underscores to avoid path issues
      for (auto& c : result) {
        if (c == '/') {
          c = '_';
        }
      }
      return result;
    }
    return "unknown_test";
  }

  /// \brief Creates a new temporary filepath and registers it for cleanup
  std::string CreateNewTempFilePath() {
    std::string filepath = GenerateUniqueTempFilePath();
    created_temp_files_.push_back(filepath);
    return filepath;
  }

  /// \brief Create a temporary filepath with the specified suffix and registers it for
  /// cleanup
  std::string CreateNewTempFilePathWithSuffix(const std::string& suffix) {
    std::string filepath = GenerateUniqueTempFilePathWithSuffix(suffix);
    created_temp_files_.push_back(filepath);
    return filepath;
  }

  /// \brief Creates a temporary directory and registers it for cleanup
  std::string CreateTempDirectory() {
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path();
    std::string dir_name =
        std::format("iceberg_test_dir_{}_{}", TestInfo(), GenerateRandomString(8));
    std::filesystem::path dir_path = temp_dir / dir_name;

    std::error_code ec;
    std::filesystem::create_directory(dir_path, ec);
    if (ec) {
      throw std::runtime_error(
          std::format("Failed to create temporary directory: {}", ec.message()));
    }

    created_temp_files_.push_back(dir_path.string());
    return dir_path.string();
  }

  /// \brief Creates a file with the given content at the specified path
  void WriteContentToFile(const std::string& path, const std::string& content) {
    std::ofstream file(path, std::ios::binary);
    if (!file) {
      throw std::runtime_error(std::format("Failed to open file for writing: {}", path));
    }
    file.write(content.data(), content.size());
    if (!file) {
      throw std::runtime_error(std::format("Failed to write to file: {}", path));
    }
  }

  /// \brief Creates a new temporary file with the given content
  std::string CreateTempFileWithContent(const std::string& content) {
    std::string path = CreateNewTempFilePath();
    WriteContentToFile(path, content);
    return path;
  }

  std::vector<std::string> created_temp_files_;
};

}  // namespace iceberg
