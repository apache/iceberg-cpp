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

#include "iceberg/catalog/memory_catalog.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

namespace iceberg {

class NamespaceContainerTest : public ::testing::Test {
 protected:
  NamespaceContainer container;

  static Namespace MakeNs(std::vector<std::string> levels) {
    return Namespace{std::move(levels)};
  }

  static TableIdentifier MakeTable(const std::vector<std::string>& ns,
                                   const std::string& name) {
    return TableIdentifier{.ns = Namespace{ns}, .name = name};
  }
};

TEST_F(NamespaceContainerTest, CreateAndExistsNamespace) {
  Namespace ns = MakeNs({"a", "b"});
  EXPECT_FALSE(container.NamespaceExists(ns));
  EXPECT_TRUE(container.CreateNamespace(ns, {{"k", "v"}}));
  EXPECT_TRUE(container.NamespaceExists(ns));
}

TEST_F(NamespaceContainerTest, CreateNamespaceTwiceFails) {
  Namespace ns = MakeNs({"x", "y"});
  EXPECT_TRUE(container.CreateNamespace(ns, {{"a", "b"}}));
  EXPECT_FALSE(container.CreateNamespace(ns, {{"a", "b"}}));
}

TEST_F(NamespaceContainerTest, DeleteEmptyNamespaceSucceeds) {
  Namespace ns = MakeNs({"del", "me"});
  EXPECT_TRUE(container.CreateNamespace(ns, {}));
  EXPECT_TRUE(container.DeleteNamespace(ns));
  EXPECT_FALSE(container.NamespaceExists(ns));
}

TEST_F(NamespaceContainerTest, DeleteNonEmptyNamespaceFails) {
  Namespace ns = MakeNs({"del", "fail"});
  EXPECT_TRUE(container.CreateNamespace(ns, {}));
  auto table = MakeTable({"del", "fail"}, "t1");
  EXPECT_TRUE(container.RegisterTable(table, "loc1"));
  EXPECT_FALSE(container.DeleteNamespace(ns));
}

TEST_F(NamespaceContainerTest, ReplaceAndGetProperties) {
  Namespace ns = MakeNs({"props"});
  EXPECT_TRUE(container.CreateNamespace(ns, {{"x", "1"}}));
  EXPECT_TRUE(container.ReplaceProperties(ns, {{"x", "2"}, {"y", "3"}}));

  auto props = container.GetProperties(ns);
  ASSERT_TRUE(props.has_value());
  EXPECT_EQ(props->at("x"), "2");
  EXPECT_EQ(props->at("y"), "3");
}

TEST_F(NamespaceContainerTest, GetPropertiesNonExistReturnsNullopt) {
  Namespace ns = MakeNs({"unknown"});
  EXPECT_FALSE(container.GetProperties(ns).has_value());
}

TEST_F(NamespaceContainerTest, RegisterAndLookupTable) {
  Namespace ns = MakeNs({"tbl", "ns"});
  EXPECT_TRUE(container.CreateNamespace(ns, {}));
  auto t1 = MakeTable(ns.levels, "table1");
  EXPECT_TRUE(container.RegisterTable(t1, "metadata/path/1"));

  EXPECT_TRUE(container.TableExists(t1));
  auto location = container.GetTableMetadataLocation(t1);
  ASSERT_TRUE(location.has_value());
  EXPECT_EQ(location.value(), "metadata/path/1");
}

TEST_F(NamespaceContainerTest, UnregisterTable) {
  Namespace ns = MakeNs({"tbl", "unreg"});
  EXPECT_TRUE(container.CreateNamespace(ns, {}));
  auto t = MakeTable(ns.levels, "t2");
  EXPECT_TRUE(container.RegisterTable(t, "meta2"));
  EXPECT_TRUE(container.UnregisterTable(t));
  EXPECT_FALSE(container.TableExists(t));
}

TEST_F(NamespaceContainerTest, RegisterTableOnNonExistingNamespaceFails) {
  auto t = MakeTable({"nonexist", "ns"}, "oops");
  EXPECT_FALSE(container.RegisterTable(t, "x"));
}

TEST_F(NamespaceContainerTest, ListChildrenNamespaces) {
  EXPECT_TRUE(container.CreateNamespace(MakeNs({"a"}), {}));
  EXPECT_TRUE(container.CreateNamespace(MakeNs({"a", "b"}), {}));
  EXPECT_TRUE(container.CreateNamespace(MakeNs({"a", "c"}), {}));
  auto children = container.ListChildrenNamespaces(MakeNs({"a"}));
  EXPECT_THAT(children, ::testing::UnorderedElementsAre("b", "c"));
}

TEST_F(NamespaceContainerTest, ListTablesReturnsCorrectNames) {
  Namespace ns = MakeNs({"list", "tables"});
  EXPECT_TRUE(container.CreateNamespace(ns, {}));
  EXPECT_TRUE(container.RegisterTable(MakeTable(ns.levels, "a"), "loc_a"));
  EXPECT_TRUE(container.RegisterTable(MakeTable(ns.levels, "b"), "loc_b"));
  EXPECT_TRUE(container.RegisterTable(MakeTable(ns.levels, "c"), "loc_c"));

  auto tables = container.ListTables(ns);
  EXPECT_THAT(tables, ::testing::UnorderedElementsAre("a", "b", "c"));
}

class MemoryCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_io_ = nullptr;  // TODO(Guotao): A real FileIO instance needs to be constructed.
    catalog_ = std::make_unique<MemoryCatalog>(file_io_, "/tmp/warehouse/");
    catalog_->Initialize("test_catalog", {{"prop1", "val1"}});
  }

  std::shared_ptr<FileIO> file_io_;
  std::unique_ptr<MemoryCatalog> catalog_;
};

TEST_F(MemoryCatalogTest, NameAndInitialization) {
  EXPECT_EQ(catalog_->name(), "test_catalog");
}

}  // namespace iceberg
