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

#include <format>
#include <memory>
#include <string>

#include <arrow/filesystem/mockfs.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/catalog/memory/in_memory_catalog.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/test_resource.h"
#include "iceberg/util/uuid.h"

namespace iceberg {

// Base test fixture for table update operations
class UpdateTestBase : public ::testing::Test {
 protected:
  void SetUp() override {
    InitializeFileIO();
    RegisterTableFromResource("TableMetadataV2Valid.json");
  }

  /// \brief Initialize file IO and create necessary directories.
  void InitializeFileIO() {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    catalog_ =
        InMemoryCatalog::Make("test_catalog", file_io_, "/warehouse/", /*properties=*/{});

    // Arrow MockFS cannot automatically create directories.
    auto arrow_fs = std::dynamic_pointer_cast<::arrow::fs::internal::MockFileSystem>(
        static_cast<arrow::ArrowFileSystemFileIO&>(*file_io_).fs());
    ASSERT_TRUE(arrow_fs != nullptr);
    ASSERT_TRUE(arrow_fs->CreateDir(table_location_ + "/metadata").ok());
  }

  /// \brief Register a table from a metadata resource file.
  ///
  /// \param resource_name The name of the metadata resource file
  void RegisterTableFromResource(const std::string& resource_name) {
    // Drop existing table if it exists
    std::ignore = catalog_->DropTable(table_ident_, /*purge=*/false);

    // Write table metadata to the table location.
    auto metadata_location = std::format("{}/metadata/00001-{}.metadata.json",
                                         table_location_, Uuid::GenerateV7().ToString());
    ICEBERG_UNWRAP_OR_FAIL(auto metadata, ReadTableMetadataFromResource(resource_name));
    metadata->location = table_location_;
    ASSERT_THAT(TableMetadataUtil::Write(*file_io_, metadata_location, *metadata),
                IsOk());

    // Register the table in the catalog.
    ICEBERG_UNWRAP_OR_FAIL(table_,
                           catalog_->RegisterTable(table_ident_, metadata_location));
  }

  const TableIdentifier table_ident_{.name = "test_table"};
  const std::string table_location_{"/warehouse/test_table"};
  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  std::shared_ptr<Table> table_;
};

}  // namespace iceberg
