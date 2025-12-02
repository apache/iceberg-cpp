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

#include "iceberg/catalog/memory/in_memory_catalog.h"

#include <filesystem>

#include <arrow/filesystem/localfs.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/schema.h"
#include "iceberg/table.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/test_resource.h"

namespace iceberg {

class InMemoryCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
    std::unordered_map<std::string, std::string> properties = {{"prop1", "val1"}};
    catalog_ = std::make_shared<InMemoryCatalog>("test_catalog", file_io_,
                                                 "/tmp/warehouse/", properties);
  }

  void TearDown() override {
    // Clean up the temporary files created for the table metadata
    for (const auto& path : created_temp_paths_) {
      std::error_code ec;
      if (std::filesystem::is_directory(path, ec)) {
        std::filesystem::remove_all(path, ec);
      } else {
        std::filesystem::remove(path, ec);
      }
    }
  }

  std::string GenerateTestTableLocation(std::string table_name) {
    std::filesystem::path temp_dir = std::filesystem::temp_directory_path();
    const auto info = ::testing::UnitTest::GetInstance()->current_test_info();
    auto table_location = std::format("{}/iceberg_test_{}_{}/{}/", temp_dir.string(),
                                      info->test_suite_name(), info->name(), table_name);
    // generate a unique directory for the table
    std::error_code ec;
    std::filesystem::create_directories(table_location, ec);
    if (ec) {
      throw std::runtime_error(
          std::format("Failed to create temporary directory: {}, error message: {}",
                      table_location, ec.message()));
    }

    created_temp_paths_.push_back(table_location);
    return table_location;
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<InMemoryCatalog> catalog_;
  // Used to store temporary paths created during the test
  std::vector<std::string> created_temp_paths_;
};

TEST_F(InMemoryCatalogTest, CatalogName) {
  EXPECT_EQ(catalog_->name(), "test_catalog");
  auto tablesRs = catalog_->ListTables(Namespace{{}});
  EXPECT_THAT(tablesRs, IsOk());
  ASSERT_TRUE(tablesRs->empty());
}

TEST_F(InMemoryCatalogTest, ListTables) {
  auto tablesRs = catalog_->ListTables(Namespace{{}});
  EXPECT_THAT(tablesRs, IsOk());
  ASSERT_TRUE(tablesRs->empty());
}

TEST_F(InMemoryCatalogTest, TableExists) {
  TableIdentifier tableIdent{.ns = {}, .name = "t1"};
  auto result = catalog_->TableExists(tableIdent);
  EXPECT_THAT(result, HasValue(::testing::Eq(false)));
}

TEST_F(InMemoryCatalogTest, RegisterTable) {
  TableIdentifier tableIdent{.ns = {}, .name = "t1"};

  ICEBERG_UNWRAP_OR_FAIL(auto metadata,
                         ReadTableMetadataFromResource("TableMetadataV2Valid.json"));

  auto table_location = GenerateTestTableLocation(tableIdent.name);
  auto metadata_location = std::format("{}v1.metadata.json", table_location);
  auto status = TableMetadataUtil::Write(*file_io_, metadata_location, *metadata);
  EXPECT_THAT(status, IsOk());

  auto table = catalog_->RegisterTable(tableIdent, metadata_location);
  EXPECT_THAT(table, IsOk());
  ASSERT_EQ(table.value()->name().name, "t1");
  ASSERT_EQ(table.value()->location(), "s3://bucket/test/location");
}

TEST_F(InMemoryCatalogTest, RefreshTable) {
  TableIdentifier table_ident{.ns = {}, .name = "t1"};
  auto schema = std::make_shared<Schema>(
      std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64())},
      /*schema_id=*/1);

  std::shared_ptr<FileIO> io;

  auto catalog = std::make_shared<MockCatalog>();
  // Mock 1st call to LoadTable
  EXPECT_CALL(*catalog, LoadTable(::testing::_))
      .WillOnce(::testing::Return(
          std::make_unique<Table>(table_ident,
                                  std::make_shared<TableMetadata>(TableMetadata{
                                      .schemas = {schema},
                                      .current_schema_id = 1,
                                      .current_snapshot_id = 1,
                                      .snapshots = {std::make_shared<Snapshot>(Snapshot{
                                          .snapshot_id = 1,
                                          .sequence_number = 1,
                                      })}}),
                                  "s3://location/1.json", io, catalog)));
  auto load_table_result = catalog->LoadTable(table_ident);
  ASSERT_THAT(load_table_result, IsOk());
  auto loaded_table = std::move(load_table_result.value());
  ASSERT_EQ(loaded_table->current_snapshot().value()->snapshot_id, 1);

  // Mock 2nd call to LoadTable
  EXPECT_CALL(*catalog, LoadTable(::testing::_))
      .WillOnce(::testing::Return(
          std::make_unique<Table>(table_ident,
                                  std::make_shared<TableMetadata>(TableMetadata{
                                      .schemas = {schema},
                                      .current_schema_id = 1,
                                      .current_snapshot_id = 2,
                                      .snapshots = {std::make_shared<Snapshot>(Snapshot{
                                                        .snapshot_id = 1,
                                                        .sequence_number = 1,
                                                    }),
                                                    std::make_shared<Snapshot>(Snapshot{
                                                        .snapshot_id = 2,
                                                        .sequence_number = 2,
                                                    })}}),
                                  "s3://location/2.json", io, catalog)));
  auto refreshed_result = loaded_table->Refresh();
  ASSERT_THAT(refreshed_result, IsOk());
  // check table is refreshed
  ASSERT_EQ(loaded_table->current_snapshot().value()->snapshot_id, 2);
}

TEST_F(InMemoryCatalogTest, DropTable) {
  TableIdentifier tableIdent{.ns = {}, .name = "t1"};
  auto result = catalog_->DropTable(tableIdent, false);
  EXPECT_THAT(result, IsOk());
}

TEST_F(InMemoryCatalogTest, Namespace) {
  Namespace ns{.levels = {"n1", "n2"}};
  std::unordered_map<std::string, std::string> properties = {{"prop1", "val1"},
                                                             {"prop2", "val2"}};
  EXPECT_THAT(catalog_->CreateNamespace(ns, properties), IsOk());
  EXPECT_THAT(catalog_->CreateNamespace(ns, properties),
              IsError(ErrorKind::kAlreadyExists));

  EXPECT_THAT(catalog_->NamespaceExists(ns), HasValue(::testing::Eq(true)));
  EXPECT_THAT(catalog_->NamespaceExists(Namespace{.levels = {"n1", "n3"}}),
              HasValue(::testing::Eq(false)));
  auto childNs = catalog_->ListNamespaces(Namespace{.levels = {"n1"}});
  EXPECT_THAT(childNs, IsOk());
  ASSERT_EQ(childNs->size(), 1U);
  ASSERT_EQ(childNs->at(0).levels.size(), 2U);
  ASSERT_EQ(childNs->at(0).levels.at(1), "n2");

  auto propsRs = catalog_->GetNamespaceProperties(ns);
  EXPECT_THAT(propsRs, IsOk());
  ASSERT_EQ(propsRs->size(), 2U);
  ASSERT_EQ(propsRs.value().at("prop1"), "val1");
  ASSERT_EQ(propsRs.value().at("prop2"), "val2");

  EXPECT_THAT(catalog_->UpdateNamespaceProperties(
                  ns, {{"prop2", "val2-updated"}, {"prop3", "val3"}}, {"prop1"}),
              IsOk());
  propsRs = catalog_->GetNamespaceProperties(ns);
  EXPECT_THAT(propsRs, IsOk());
  ASSERT_EQ(propsRs->size(), 2U);
  ASSERT_EQ(propsRs.value().at("prop2"), "val2-updated");
  ASSERT_EQ(propsRs.value().at("prop3"), "val3");
}

}  // namespace iceberg
