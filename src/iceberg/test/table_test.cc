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

#include "iceberg/table.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/schema.h"
#include "iceberg/schema_field.h"
#include "iceberg/table_metadata.h"
#include "iceberg/test/matchers.h"
#include "iceberg/test/mock_catalog.h"
#include "iceberg/test/mock_io.h"

namespace iceberg {

template <typename T>
struct TableTraits;

template <>
struct TableTraits<Table> {
  static constexpr bool kRefreshSupported = true;
  static constexpr bool kTransactionSupported = true;

  static Result<std::shared_ptr<Table>> Make(const TableIdentifier& ident,
                                             std::shared_ptr<TableMetadata> metadata,
                                             const std::string& location,
                                             std::shared_ptr<FileIO> io,
                                             std::shared_ptr<Catalog> catalog) {
    return Table::Make(ident, std::move(metadata), location, std::move(io),
                       std::move(catalog));
  }
};

template <>
struct TableTraits<StaticTable> {
  static constexpr bool kRefreshSupported = false;
  static constexpr bool kTransactionSupported = false;

  static Result<std::shared_ptr<StaticTable>> Make(
      const TableIdentifier& ident, std::shared_ptr<TableMetadata> metadata,
      const std::string& location, std::shared_ptr<FileIO> io, std::shared_ptr<Catalog>) {
    return StaticTable::Make(ident, std::move(metadata), location, std::move(io));
  }
};

template <>
struct TableTraits<StagedTable> {
  static constexpr bool kRefreshSupported = true;
  static constexpr bool kTransactionSupported = true;

  static Result<std::shared_ptr<StagedTable>> Make(
      const TableIdentifier& ident, std::shared_ptr<TableMetadata> metadata,
      const std::string& location, std::shared_ptr<FileIO> io,
      std::shared_ptr<Catalog> catalog) {
    return StagedTable::Make(ident, std::move(metadata), location, std::move(io),
                             std::move(catalog));
  }
};

template <typename T>
class TypedTableTest : public ::testing::Test {
 protected:
  using Traits = TableTraits<T>;

  void SetUp() override {
    io_ = std::make_shared<MockFileIO>();
    catalog_ = std::make_shared<MockCatalog>();

    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int64()),
                                 SchemaField::MakeOptional(2, "name", string())},
        1);
    metadata_ = std::make_shared<TableMetadata>(
        TableMetadata{.format_version = 2, .schemas = {schema}, .current_schema_id = 1});
  }

  Result<std::shared_ptr<T>> MakeTable(const std::string& name) {
    TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = name};
    return Traits::Make(ident, metadata_, "s3://bucket/meta.json", io_, catalog_);
  }

  std::shared_ptr<MockFileIO> io_;
  std::shared_ptr<MockCatalog> catalog_;
  std::shared_ptr<TableMetadata> metadata_;
};

using TableTypes = ::testing::Types<Table, StaticTable, StagedTable>;
TYPED_TEST_SUITE(TypedTableTest, TableTypes);

TYPED_TEST(TypedTableTest, BasicMetadata) {
  ICEBERG_UNWRAP_OR_FAIL(auto table, this->MakeTable("test_table"));

  EXPECT_EQ(table->name().name, "test_table");
  EXPECT_EQ(table->name().ns.levels, (std::vector<std::string>{"db"}));
  EXPECT_EQ(table->metadata()->format_version, 2);
  EXPECT_EQ(table->metadata()->schemas.size(), 1);
}

TYPED_TEST(TypedTableTest, Refresh) {
  using Traits = typename TestFixture::Traits;
  ICEBERG_UNWRAP_OR_FAIL(auto table, this->MakeTable("test_table"));

  if constexpr (Traits::kRefreshSupported) {
    if constexpr (std::is_same_v<TypeParam, Table>) {
      TableIdentifier ident{.ns = Namespace{.levels = {"db"}}, .name = "test_table"};
      ICEBERG_UNWRAP_OR_FAIL(auto refreshed,
                             Table::Make(ident, this->metadata_, "s3://bucket/meta2.json",
                                         this->io_, this->catalog_));
      EXPECT_CALL(*this->catalog_, LoadTable(::testing::_))
          .WillOnce(::testing::Return(refreshed));
    }
    EXPECT_THAT(table->Refresh(), IsOk());
  } else {
    EXPECT_THAT(table->Refresh(), IsError(ErrorKind::kNotSupported));
  }
}

TYPED_TEST(TypedTableTest, NewTransaction) {
  using Traits = typename TestFixture::Traits;
  ICEBERG_UNWRAP_OR_FAIL(auto table, this->MakeTable("test_table"));

  if constexpr (Traits::kTransactionSupported) {
    EXPECT_THAT(table->NewTransaction(), IsOk());
  } else {
    EXPECT_THAT(table->NewTransaction(), IsError(ErrorKind::kNotSupported));
  }
}

}  // namespace iceberg
