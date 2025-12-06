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

#include <memory>

#include "iceberg/catalog.h"
#include "iceberg/result.h"
#include "iceberg/table.h"
#include "iceberg/type_fwd.h"

namespace iceberg {

/**
 * @brief Lightweight catalog wrapper for BaseTransaction.
 *
 * For read-only operations, TransactionCatalog simply forwards to the wrapped catalog.
 * For mutating calls such as UpdateTable or HasLastOperationCommitted, it delegates back
 * to the owning BaseTransaction so staged updates remain private until commit.
 */
class ICEBERG_EXPORT TransactionCatalog : public Catalog {
 public:
  TransactionCatalog(std::shared_ptr<Catalog> catalog, BaseTransaction* owner)
      : catalog_impl_(std::move(catalog)), owner_(owner) {}
  ~TransactionCatalog() override = default;

  std::string_view name() const override { return catalog_impl_->name(); };

  Status CreateNamespace(
      const Namespace& ns,
      const std::unordered_map<std::string, std::string>& properties) override {
    return catalog_impl_->CreateNamespace(ns, properties);
  }

  Result<std::vector<Namespace>> ListNamespaces(const Namespace& ns) const override {
    return catalog_impl_->ListNamespaces(ns);
  }

  Result<std::unordered_map<std::string, std::string>> GetNamespaceProperties(
      const Namespace& ns) const override {
    return catalog_impl_->GetNamespaceProperties(ns);
  }

  Status DropNamespace(const Namespace& ns) override {
    // Will do nothing for directly dropping namespaces.
    return NotSupported("DropNamespace is not supported in TransactionCatalog.");
  }

  Result<bool> NamespaceExists(const Namespace& ns) const override {
    return catalog_impl_->NamespaceExists(ns);
  }

  Status UpdateNamespaceProperties(
      const Namespace& ns, const std::unordered_map<std::string, std::string>& updates,
      const std::unordered_set<std::string>& removals) override {
    // Will do nothing for directly updating namespace properties.
    return NotSupported(
        "UpdateNamespaceProperties is not supported in TransactionCatalog.");
  }

  Result<std::vector<TableIdentifier>> ListTables(const Namespace& ns) const override {
    return catalog_impl_->ListTables(ns);
  }

  Result<std::unique_ptr<Table>> CreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override {
    return NotImplemented("CreateTable is not implemented in TransactionCatalog.");
  }

  Result<std::unique_ptr<Table>> UpdateTable(
      const TableIdentifier& identifier,
      std::vector<std::unique_ptr<TableRequirement>> requirements,
      std::vector<std::unique_ptr<TableUpdate>> updates) override;

  Result<std::shared_ptr<Transaction>> StageCreateTable(
      const TableIdentifier& identifier, const Schema& schema, const PartitionSpec& spec,
      const std::string& location,
      const std::unordered_map<std::string, std::string>& properties) override {
    return NotImplemented("StageCreateTable is not implemented in TransactionCatalog.");
  }

  Result<bool> TableExists(const TableIdentifier& identifier) const override {
    return catalog_impl_->TableExists(identifier);
  }

  Status DropTable(const TableIdentifier& identifier, bool purge) override {
    return NotSupported("DropTable is not supported in TransactionCatalog.");
  }

  Status RenameTable(const TableIdentifier& from, const TableIdentifier& to) override {
    return NotImplemented("rename table");
  }

  Result<std::unique_ptr<Table>> LoadTable(const TableIdentifier& identifier) override {
    return catalog_impl_->LoadTable(identifier);
  }

  Result<std::shared_ptr<Table>> RegisterTable(
      const TableIdentifier& identifier,
      const std::string& metadata_file_location) override {
    return NotImplemented("register table");
  }

  void SetLastOperationCommitted(bool committed) override;

  const std::shared_ptr<Catalog>& catalog_impl() const { return catalog_impl_; }

 private:
  std::shared_ptr<Catalog> catalog_impl_;
  BaseTransaction* owner_;
};

}  // namespace iceberg
