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

namespace iceberg {

class InMemoryNamespaceTest : public ::testing::Test {
 protected:
  InMemoryNamespace root_namespace_;

  static Namespace MakeNs(std::vector<std::string> levels) {
    return Namespace{std::move(levels)};
  }

  static TableIdentifier MakeTable(const std::vector<std::string>& ns,
                                   const std::string& name) {
    return TableIdentifier{.ns = Namespace{ns}, .name = name};
  }
};

TEST_F(InMemoryNamespaceTest, CreateAndExistsNamespace) {
  const auto ns = MakeNs({"a", "b"});
  EXPECT_FALSE(root_namespace_.NamespaceExists(ns));
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {{"k", "v"}}));
  EXPECT_TRUE(root_namespace_.NamespaceExists(ns));
}

TEST_F(InMemoryNamespaceTest, CreateNamespaceTwiceFails) {
  const auto ns = MakeNs({"x", "y"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {{"a", "b"}}));
  EXPECT_FALSE(root_namespace_.CreateNamespace(ns, {{"a", "b"}}));
}

TEST_F(InMemoryNamespaceTest, DeleteEmptyNamespaceSucceeds) {
  const auto ns = MakeNs({"del", "me"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {}));
  EXPECT_TRUE(root_namespace_.DeleteNamespace(ns));
  EXPECT_FALSE(root_namespace_.NamespaceExists(ns));
}

TEST_F(InMemoryNamespaceTest, DeleteNonEmptyNamespaceFails) {
  const auto ns = MakeNs({"del", "fail"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {}));
  const auto table = MakeTable({"del", "fail"}, "t1");
  EXPECT_TRUE(root_namespace_.RegisterTable(table, "loc1"));
  EXPECT_FALSE(root_namespace_.DeleteNamespace(ns));
}

TEST_F(InMemoryNamespaceTest, ReplaceAndGetProperties) {
  const auto ns = MakeNs({"props"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {{"x", "1"}}));
  EXPECT_TRUE(root_namespace_.ReplaceProperties(ns, {{"x", "2"}, {"y", "3"}}));

  const auto props = root_namespace_.GetProperties(ns);
  ASSERT_TRUE(props.has_value());
  EXPECT_EQ(props->at("x"), "2");
  EXPECT_EQ(props->at("y"), "3");
}

TEST_F(InMemoryNamespaceTest, GetPropertiesNonExistReturnsNullopt) {
  const auto ns = MakeNs({"unknown"});
  EXPECT_FALSE(root_namespace_.GetProperties(ns).has_value());
}

TEST_F(InMemoryNamespaceTest, RegisterAndLookupTable) {
  const auto ns = MakeNs({"tbl", "ns"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {}));
  const auto t1 = MakeTable(ns.levels, "table1");
  EXPECT_TRUE(root_namespace_.RegisterTable(t1, "metadata/path/1"));

  EXPECT_TRUE(root_namespace_.TableExists(t1));
  const auto location = root_namespace_.GetTableMetadataLocation(t1);
  ASSERT_TRUE(location.has_value());
  EXPECT_EQ(location.value(), "metadata/path/1");
}

TEST_F(InMemoryNamespaceTest, UnregisterTable) {
  const auto ns = MakeNs({"tbl", "unreg"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {}));
  const auto t = MakeTable(ns.levels, "t2");
  EXPECT_TRUE(root_namespace_.RegisterTable(t, "meta2"));
  EXPECT_TRUE(root_namespace_.UnregisterTable(t));
  EXPECT_FALSE(root_namespace_.TableExists(t).value());
}

TEST_F(InMemoryNamespaceTest, RegisterTableOnNonExistingNamespaceFails) {
  const auto t = MakeTable({"nonexist", "ns"}, "oops");
  EXPECT_FALSE(root_namespace_.RegisterTable(t, "x"));
}

TEST_F(InMemoryNamespaceTest, ListChildrenNamespaces) {
  EXPECT_TRUE(root_namespace_.CreateNamespace(MakeNs({"a"}), {}));
  EXPECT_TRUE(root_namespace_.CreateNamespace(MakeNs({"a", "b"}), {}));
  EXPECT_TRUE(root_namespace_.CreateNamespace(MakeNs({"a", "c"}), {}));
  const auto children = root_namespace_.ListChildrenNamespaces(MakeNs({"a"}));
  EXPECT_TRUE(children);
  EXPECT_THAT(*children, ::testing::UnorderedElementsAre("b", "c"));
}

TEST_F(InMemoryNamespaceTest, ListTablesReturnsCorrectNames) {
  const auto ns = MakeNs({"list", "tables"});
  EXPECT_TRUE(root_namespace_.CreateNamespace(ns, {}));
  EXPECT_TRUE(root_namespace_.RegisterTable(MakeTable(ns.levels, "a"), "loc_a"));
  EXPECT_TRUE(root_namespace_.RegisterTable(MakeTable(ns.levels, "b"), "loc_b"));
  EXPECT_TRUE(root_namespace_.RegisterTable(MakeTable(ns.levels, "c"), "loc_c"));

  auto tables = root_namespace_.ListTables(ns);
  EXPECT_TRUE(tables);
  EXPECT_THAT(*tables, ::testing::UnorderedElementsAre("a", "b", "c"));
}

class MemoryCatalogTest : public ::testing::Test {
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

TEST_F(MemoryCatalogTest, NameAndInitialization) {
  EXPECT_EQ(catalog_->name(), "test_catalog");
}

}  // namespace iceberg
