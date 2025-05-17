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

#include "iceberg/catalog/in_memory_catalog.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "matchers.h"

namespace iceberg {

class InMemoryCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_io_ = nullptr;  // TODO(Guotao): A real FileIO instance needs to be constructed.
    std::unordered_map<std::string, std::string> properties = {{"prop1", "val1"}};
    catalog_ = std::make_unique<InMemoryCatalog>("test_catalog", file_io_,
                                                 "/tmp/warehouse/", properties);
  }

  std::shared_ptr<FileIO> file_io_;
  std::unique_ptr<InMemoryCatalog> catalog_;
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

TEST_F(InMemoryCatalogTest, DropTable) {
  TableIdentifier tableIdent{.ns = {}, .name = "t1"};
  auto result = catalog_->DropTable(tableIdent, false);
  EXPECT_THAT(result, IsOk());
}

}  // namespace iceberg
