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

#include "iceberg/catalog/sql/sql_catalog.h"

#include <sqlite3.h>

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_io_internal.h"
#include "iceberg/catalog/sql/catalog_store.h"
#include "iceberg/catalog/sql/connection_uri_internal.h"
#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table.h"
#include "iceberg/table_identifier.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::sql {

namespace {

std::filesystem::path SqliteTestPath(std::string_view name) {
  return std::filesystem::temp_directory_path() /
         (std::string("iceberg_sql_catalog_") + std::string(name) + ".db");
}

void ExecSql(const std::filesystem::path& path, std::string_view sql) {
  sqlite3* raw_db = nullptr;
  ASSERT_EQ(sqlite3_open(path.string().c_str(), &raw_db), SQLITE_OK);
  std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db(raw_db, sqlite3_close);

  char* error_message = nullptr;
  const int result =
      sqlite3_exec(db.get(), std::string(sql).c_str(), nullptr, nullptr, &error_message);
  std::unique_ptr<char, decltype(&sqlite3_free)> error(error_message, sqlite3_free);
  ASSERT_EQ(result, SQLITE_OK) << (error ? error.get() : "");
}

std::optional<std::string> QuerySingleText(const std::filesystem::path& path,
                                           std::string_view sql) {
  sqlite3* raw_db = nullptr;
  if (sqlite3_open(path.string().c_str(), &raw_db) != SQLITE_OK) {
    ADD_FAILURE() << "failed to open sqlite database";
    return std::nullopt;
  }
  std::unique_ptr<sqlite3, decltype(&sqlite3_close)> db(raw_db, sqlite3_close);

  sqlite3_stmt* raw_stmt = nullptr;
  if (sqlite3_prepare_v2(db.get(), std::string(sql).c_str(), -1, &raw_stmt, nullptr) !=
      SQLITE_OK) {
    ADD_FAILURE() << sqlite3_errmsg(db.get());
    return std::nullopt;
  }
  std::unique_ptr<sqlite3_stmt, decltype(&sqlite3_finalize)> stmt(raw_stmt,
                                                                  sqlite3_finalize);

  const int step = sqlite3_step(stmt.get());
  if (step != SQLITE_ROW) {
    ADD_FAILURE() << "query returned no row";
    return std::nullopt;
  }

  const auto* text = sqlite3_column_text(stmt.get(), 0);
  if (text == nullptr) {
    return std::nullopt;
  }
  return std::string(reinterpret_cast<const char*>(text));
}

}  // namespace

class SqlCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeLocalFileIO();
    auto store =
        MakeSqliteCatalogStore({.catalog_name = "test_catalog", .uri = ":memory:"});
    ASSERT_THAT(store, IsOk());

    SqlCatalogConfig config{
        .name = "test_catalog",
        .uri = ":memory:",
        .warehouse_location = WarehouseLocation(),
    };
    auto catalog = SqlCatalog::Make(config, file_io_, store.value());
    ASSERT_THAT(catalog, IsOk());
    store_ = store.value();
    catalog_ = catalog.value();
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(WarehouseDir(), ec);
  }

  std::string WarehouseDir() const {
    return (std::filesystem::temp_directory_path() / "iceberg_sql_catalog_test").string();
  }

  std::string WarehouseLocation() const {
    return std::filesystem::path(WarehouseDir()).generic_string();
  }

  // Returns a table base location whose metadata directory already exists, since
  // the local Arrow FileIO does not create parent directories on write.
  std::string MakeTableLocation(const std::string& name) const {
    auto location = std::filesystem::path(WarehouseDir()) / name;
    std::filesystem::create_directories(location / "metadata");
    return location.string();
  }

  std::shared_ptr<Schema> MakeSchema() const {
    return std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "x", int64())},
        /*schema_id=*/1);
  }

  std::shared_ptr<FileIO> file_io_;
  std::shared_ptr<CatalogStore> store_;
  std::shared_ptr<SqlCatalog> catalog_;
};

TEST(ConnectionUriTest, ParsesUserHostPortAndDatabase) {
  auto parsed = ParseConnectionUri("postgresql://alice:secret@db.example:5432/prod");
  ASSERT_THAT(parsed, IsOk());
  EXPECT_EQ(parsed->user, "alice");
  EXPECT_EQ(parsed->password, "secret");
  EXPECT_EQ(parsed->host, "db.example");
  EXPECT_EQ(parsed->port, 5432);
  EXPECT_EQ(parsed->database, "prod");
}

TEST(ConnectionUriTest, RejectsInvalidPort) {
  EXPECT_THAT(ParseConnectionUri("mysql://db.example:/prod"),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ParseConnectionUri("mysql://db.example:abc/prod"),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(ParseConnectionUri("mysql://db.example:4294967296/prod"),
              IsError(ErrorKind::kInvalidArgument));
}

TEST_F(SqlCatalogTest, CatalogName) { EXPECT_EQ(catalog_->name(), "test_catalog"); }

TEST_F(SqlCatalogTest, NamespaceLifecycle) {
  Namespace ns{{"db"}};
  EXPECT_THAT(catalog_->NamespaceExists(ns), HasValue(::testing::Eq(false)));

  EXPECT_THAT(catalog_->CreateNamespace(ns, {{"owner", "alice"}}), IsOk());
  EXPECT_THAT(catalog_->NamespaceExists(ns), HasValue(::testing::Eq(true)));

  // Duplicate creation fails.
  EXPECT_THAT(catalog_->CreateNamespace(ns, {}), IsError(ErrorKind::kAlreadyExists));

  // Properties round-trip, sentinel hidden.
  auto props = catalog_->GetNamespaceProperties(ns);
  ASSERT_THAT(props, IsOk());
  EXPECT_EQ(props->size(), 1);
  EXPECT_EQ(props->at("owner"), "alice");

  // Update + remove.
  EXPECT_THAT(
      catalog_->UpdateNamespaceProperties(ns, {{"owner", "bob"}, {"team", "x"}}, {}),
      IsOk());
  EXPECT_THAT(catalog_->UpdateNamespaceProperties(ns, {}, {"team"}), IsOk());
  props = catalog_->GetNamespaceProperties(ns);
  ASSERT_THAT(props, IsOk());
  EXPECT_EQ(props->at("owner"), "bob");
  EXPECT_FALSE(props->contains("team"));

  EXPECT_THAT(catalog_->DropNamespace(ns), IsOk());
  EXPECT_THAT(catalog_->NamespaceExists(ns), HasValue(::testing::Eq(false)));
}

TEST_F(SqlCatalogTest, ListNamespacesReturnsImmediateChildren) {
  ASSERT_THAT(catalog_->CreateNamespace(Namespace{{"a"}}, {}), IsOk());
  ASSERT_THAT(catalog_->CreateNamespace(Namespace{{"a", "b"}}, {}), IsOk());
  ASSERT_THAT(catalog_->CreateNamespace(Namespace{{"a", "c"}}, {}), IsOk());

  auto children = catalog_->ListNamespaces(Namespace{{"a"}});
  ASSERT_THAT(children, IsOk());
  EXPECT_THAT(*children, ::testing::UnorderedElementsAre(Namespace{{"a", "b"}},
                                                         Namespace{{"a", "c"}}));

  // Parent with children cannot be dropped.
  EXPECT_THAT(catalog_->DropNamespace(Namespace{{"a"}}),
              IsError(ErrorKind::kNamespaceNotEmpty));
}

TEST_F(SqlCatalogTest, TableLifecycle) {
  Namespace ns{{"db"}};
  ASSERT_THAT(catalog_->CreateNamespace(ns, {}), IsOk());

  TableIdentifier ident{.ns = ns, .name = "t1"};
  EXPECT_THAT(catalog_->TableExists(ident), HasValue(::testing::Eq(false)));

  const std::string location = MakeTableLocation("t1");
  auto created =
      catalog_->CreateTable(ident, MakeSchema(), PartitionSpec::Unpartitioned(),
                            SortOrder::Unsorted(), location, {});
  ASSERT_THAT(created, IsOk());
  EXPECT_EQ(created.value()->full_name(), "test_catalog.db.t1");
  EXPECT_THAT(catalog_->TableExists(ident), HasValue(::testing::Eq(true)));

  // Duplicate create fails.
  EXPECT_THAT(catalog_->CreateTable(ident, MakeSchema(), PartitionSpec::Unpartitioned(),
                                    SortOrder::Unsorted(), location, {}),
              IsError(ErrorKind::kAlreadyExists));

  // List + load.
  auto tables = catalog_->ListTables(ns);
  ASSERT_THAT(tables, IsOk());
  ASSERT_EQ(tables->size(), 1);
  EXPECT_EQ((*tables)[0].name, "t1");

  auto loaded = catalog_->LoadTable(ident);
  ASSERT_THAT(loaded, IsOk());
  EXPECT_EQ(loaded.value()->full_name(), "test_catalog.db.t1");

  // Rename.
  TableIdentifier renamed{.ns = ns, .name = "t2"};
  EXPECT_THAT(catalog_->RenameTable(ident, renamed), IsOk());
  EXPECT_THAT(catalog_->TableExists(ident), HasValue(::testing::Eq(false)));
  EXPECT_THAT(catalog_->TableExists(renamed), HasValue(::testing::Eq(true)));
  EXPECT_THAT(catalog_->RenameTable(renamed, renamed), IsOk());

  // Drop.
  EXPECT_THAT(catalog_->DropTable(renamed, /*purge=*/false), IsOk());
  EXPECT_THAT(catalog_->TableExists(renamed), HasValue(::testing::Eq(false)));
  EXPECT_THAT(catalog_->DropTable(renamed, false), IsError(ErrorKind::kNoSuchTable));
}

TEST_F(SqlCatalogTest, CreateTableRequiresNamespace) {
  TableIdentifier ident{.ns = Namespace{{"missing"}}, .name = "t1"};
  EXPECT_THAT(catalog_->CreateTable(ident, MakeSchema(), PartitionSpec::Unpartitioned(),
                                    SortOrder::Unsorted(), "", {}),
              IsError(ErrorKind::kNoSuchNamespace));
}

TEST_F(SqlCatalogTest, CreateTableUsesNamespaceLocationWhenLocationIsMissing) {
  Namespace ns{{"db"}};
  const auto namespace_location = std::filesystem::path(WarehouseDir()) / "custom_db";
  ASSERT_THAT(
      catalog_->CreateNamespace(ns, {{"location", namespace_location.generic_string()}}),
      IsOk());

  const auto table_location = namespace_location / "t1";
  std::filesystem::create_directories(table_location / "metadata");

  TableIdentifier ident{.ns = ns, .name = "t1"};
  auto created = catalog_->CreateTable(
      ident, MakeSchema(), PartitionSpec::Unpartitioned(), SortOrder::Unsorted(), "", {});
  ASSERT_THAT(created, IsOk());
  EXPECT_EQ((*created)->location(), table_location.generic_string());
}

TEST_F(SqlCatalogTest, CreateTableFallsBackToWarehouseNamespacePath) {
  Namespace ns{{"a", "b"}};
  ASSERT_THAT(catalog_->CreateNamespace(ns, {}), IsOk());

  const auto table_location = std::filesystem::path(WarehouseDir()) / "a" / "b" / "t1";
  std::filesystem::create_directories(table_location / "metadata");

  TableIdentifier ident{.ns = ns, .name = "t1"};
  auto created = catalog_->CreateTable(
      ident, MakeSchema(), PartitionSpec::Unpartitioned(), SortOrder::Unsorted(), "", {});
  ASSERT_THAT(created, IsOk());
  EXPECT_EQ((*created)->location(), table_location.generic_string());
}

TEST_F(SqlCatalogTest, StageCreateTableRequiresNamespace) {
  TableIdentifier ident{.ns = Namespace{{"missing"}}, .name = "t1"};
  EXPECT_THAT(
      catalog_->StageCreateTable(ident, MakeSchema(), PartitionSpec::Unpartitioned(),
                                 SortOrder::Unsorted(), "", {}),
      IsError(ErrorKind::kNoSuchNamespace));
}

TEST_F(SqlCatalogTest, NamespaceLevelsCannotContainDots) {
  EXPECT_THAT(catalog_->CreateNamespace(Namespace{{"a.b"}}, {}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(catalog_->ListNamespaces(Namespace{{"a.b"}}),
              IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(
      catalog_->TableExists(TableIdentifier{.ns = Namespace{{"a.b"}}, .name = "t"}),
      IsError(ErrorKind::kInvalidArgument));
}

TEST_F(SqlCatalogTest, ListNamespacesIncludesTableOnlyNamespaces) {
  ASSERT_THAT(store_->InsertTable("orphan", "t", "/loc/v1.metadata.json"), IsOk());

  auto namespaces = catalog_->ListNamespaces(Namespace{});
  ASSERT_THAT(namespaces, IsOk());
  EXPECT_THAT(*namespaces, ::testing::Contains(Namespace{{"orphan"}}));
}

TEST_F(SqlCatalogTest, CustomStoreInjection) {
  // The same catalog logic works against any CatalogStore; here we just verify the
  // injection entry point with the built-in SQLite store.
  auto store = MakeSqliteCatalogStore({.catalog_name = "custom", .uri = ":memory:"});
  ASSERT_THAT(store, IsOk());
  SqlCatalogConfig config{.name = "custom", .warehouse_location = WarehouseDir()};
  auto catalog = SqlCatalog::Make(config, file_io_, store.value());
  ASSERT_THAT(catalog, IsOk());
  EXPECT_EQ(catalog.value()->name(), "custom");
  EXPECT_THAT(catalog.value()->CreateNamespace(Namespace{{"db"}}, {}), IsOk());
}

TEST(SqliteCatalogStoreTest, DuplicateKeyMapsToAlreadyExists) {
  auto store_result = MakeSqliteCatalogStore({.catalog_name = "c", .uri = ":memory:"});
  ASSERT_THAT(store_result, IsOk());
  auto store = store_result.value();
  ASSERT_THAT(store->Initialize(), IsOk());

  EXPECT_THAT(store->InsertTable("db", "t", "/loc/v1.metadata.json"), IsOk());
  EXPECT_THAT(store->InsertTable("db", "t", "/loc/v2.metadata.json"),
              IsError(ErrorKind::kAlreadyExists));
}

TEST(SqliteCatalogStoreTest, TableRowsUseRecordTypeAndReadLegacyNullRows) {
  const auto db_path = SqliteTestPath("record_type");
  std::error_code ec;
  std::filesystem::remove(db_path, ec);

  auto store_result =
      MakeSqliteCatalogStore({.catalog_name = "c", .uri = db_path.string()});
  ASSERT_THAT(store_result, IsOk());
  auto store = store_result.value();
  ASSERT_THAT(store->Initialize(), IsOk());

  ASSERT_THAT(store->InsertTable("db", "t", "/loc/v1.metadata.json"), IsOk());
  auto record_type = QuerySingleText(
      db_path,
      "SELECT iceberg_type FROM iceberg_tables WHERE catalog_name = 'c' AND "
      "table_namespace = 'db' AND table_name = 't'");
  ASSERT_TRUE(record_type.has_value());
  EXPECT_EQ(*record_type, "TABLE");

  ExecSql(db_path,
          "INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, "
          "metadata_location, previous_metadata_location, iceberg_type) "
          "VALUES ('c', 'legacy', 't', '/loc/legacy.metadata.json', NULL, NULL)");

  EXPECT_THAT(store->TableExists("legacy", "t"), HasValue(::testing::Eq(true)));
  auto table_names = store->ListTableNames("legacy");
  ASSERT_THAT(table_names, IsOk());
  EXPECT_THAT(*table_names, ::testing::ElementsAre("t"));

  std::filesystem::remove(db_path, ec);
}

TEST(SqliteCatalogStoreTest, RunInTransactionRollsBackOnError) {
  auto store_result = MakeSqliteCatalogStore({.catalog_name = "tx", .uri = ":memory:"});
  ASSERT_THAT(store_result, IsOk());
  auto store = store_result.value();
  ASSERT_THAT(store->Initialize(), IsOk());

  EXPECT_THAT(store->RunInTransaction([&] {
    ICEBERG_RETURN_UNEXPECTED(
        store->InsertNamespaceProperty("db", "owner", std::string_view{"alice"}));
    return InvalidArgument("abort transaction");
  }),
              IsError(ErrorKind::kInvalidArgument));

  auto props = store->GetNamespaceProperties("db");
  ASSERT_THAT(props, IsOk());
  EXPECT_TRUE(props->empty());
}

// SQLite ignores `max_connections` and always uses a single connection; setting
// it greater than 1 must be harmless and transactions must still roll back.
TEST(SqliteCatalogStoreTest, MaxConnectionsIgnoredAndTransactionRollsBack) {
  const auto db_path = SqliteTestPath("max_connections");
  std::error_code ec;
  std::filesystem::remove(db_path, ec);

  auto store_result = MakeSqliteCatalogStore(
      {.catalog_name = "pool", .uri = db_path.string(), .max_connections = 2});
  ASSERT_THAT(store_result, IsOk());
  auto store = store_result.value();
  ASSERT_THAT(store->Initialize(), IsOk());

  EXPECT_THAT(store->RunInTransaction([&] {
    ICEBERG_RETURN_UNEXPECTED(
        store->InsertNamespaceProperty("db", "owner", std::string_view{"alice"}));
    return InvalidArgument("abort transaction");
  }),
              IsError(ErrorKind::kInvalidArgument));

  auto props = store->GetNamespaceProperties("db");
  ASSERT_THAT(props, IsOk());
  EXPECT_TRUE(props->empty());

  std::filesystem::remove(db_path, ec);
}

TEST(SqliteCatalogStoreTest, UpdateTableMetadataLocationReturnsAffectedRows) {
  auto store_result = MakeSqliteCatalogStore({.catalog_name = "cas", .uri = ":memory:"});
  ASSERT_THAT(store_result, IsOk());
  auto store = store_result.value();
  ASSERT_THAT(store->Initialize(), IsOk());
  ASSERT_THAT(store->InsertTable("db", "t", "/loc/v1.metadata.json"), IsOk());

  EXPECT_THAT(store->UpdateTableMetadataLocation("db", "t", "/loc/v2.metadata.json",
                                                 "/loc/v1.metadata.json",
                                                 "/loc/stale.metadata.json"),
              HasValue(::testing::Eq(0)));
  EXPECT_THAT(store->UpdateTableMetadataLocation("db", "t", "/loc/v2.metadata.json",
                                                 "/loc/v1.metadata.json",
                                                 "/loc/v1.metadata.json"),
              HasValue(::testing::Eq(1)));
}

}  // namespace iceberg::sql
