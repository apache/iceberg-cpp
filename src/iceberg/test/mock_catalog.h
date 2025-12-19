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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "iceberg/catalog.h"

namespace iceberg {

class MockCatalog : public Catalog {
 public:
  MockCatalog() = default;
  ~MockCatalog() override = default;

  MOCK_METHOD(std::string_view, name, (), (const, override));

  MOCK_METHOD(Status, CreateNamespace,
              (const Namespace&, (const std::unordered_map<std::string, std::string>&)),
              (override));

  MOCK_METHOD((Result<std::vector<Namespace>>), ListNamespaces, (const Namespace&),
              (const, override));

  MOCK_METHOD((Result<std::unordered_map<std::string, std::string>>),
              GetNamespaceProperties, (const Namespace&), (const, override));

  MOCK_METHOD(Status, UpdateNamespaceProperties,
              (const Namespace&, (const std::unordered_map<std::string, std::string>&),
               (const std::unordered_set<std::string>&)),
              (override));

  MOCK_METHOD(Status, DropNamespace, (const Namespace&), (override));

  MOCK_METHOD(Result<bool>, NamespaceExists, (const Namespace&), (const, override));

  MOCK_METHOD((Result<std::vector<TableIdentifier>>), ListTables, (const Namespace&),
              (const, override));

  MOCK_METHOD((Result<std::shared_ptr<Table>>), CreateTable,
              (const TableIdentifier&, const std::shared_ptr<Schema>&,
               const std::shared_ptr<PartitionSpec>&, const std::shared_ptr<SortOrder>&,
               const std::string&, (const std::unordered_map<std::string, std::string>&)),
              (override));

  MOCK_METHOD((Result<std::shared_ptr<Table>>), UpdateTable,
              (const TableIdentifier&,
               (const std::vector<std::unique_ptr<TableRequirement>>&),
               (const std::vector<std::unique_ptr<TableUpdate>>&)),
              (override));

  MOCK_METHOD((Result<std::shared_ptr<Transaction>>), StageCreateTable,
              (const TableIdentifier&, const std::shared_ptr<Schema>&,
               const std::shared_ptr<PartitionSpec>&, const std::shared_ptr<SortOrder>&,
               const std::string&, (const std::unordered_map<std::string, std::string>&)),
              (override));

  MOCK_METHOD(Result<bool>, TableExists, (const TableIdentifier&), (const, override));

  MOCK_METHOD(Status, DropTable, (const TableIdentifier&, bool), (override));

  MOCK_METHOD(Status, RenameTable, (const TableIdentifier&, const TableIdentifier&),
              (override));

  MOCK_METHOD((Result<std::shared_ptr<Table>>), LoadTable, (const TableIdentifier&),
              (override));

  MOCK_METHOD((Result<std::shared_ptr<Table>>), RegisterTable,
              (const TableIdentifier&, const std::string&), (override));
};

}  // namespace iceberg
