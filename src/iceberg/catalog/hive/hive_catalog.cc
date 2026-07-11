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

#include "iceberg/catalog/hive/hive_catalog.h"

#include <memory>
#include <utility>

#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {

constexpr std::string_view kNotImplementedMessage =
    "HiveCatalog method is not yet implemented.";

}  // namespace

HiveCatalog::HiveCatalog(HiveCatalogProperties config)
    : config_(std::move(config)), name_(config_.Get(HiveCatalogProperties::kName)) {}

HiveCatalog::~HiveCatalog() = default;

Result<std::shared_ptr<HiveCatalog>> HiveCatalog::Make(
    const HiveCatalogProperties& config) {
  ICEBERG_RETURN_UNEXPECTED(config.Uri());
  return std::shared_ptr<HiveCatalog>(new HiveCatalog(config));
}

std::string_view HiveCatalog::name() const { return name_; }

Status HiveCatalog::CreateNamespace(
    const Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::vector<Namespace>> HiveCatalog::ListNamespaces(
    const Namespace& /*ns*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::unordered_map<std::string, std::string>> HiveCatalog::GetNamespaceProperties(
    const Namespace& /*ns*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::DropNamespace(const Namespace& /*ns*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<bool> HiveCatalog::NamespaceExists(const Namespace& /*ns*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::UpdateNamespaceProperties(
    const Namespace& /*ns*/,
    const std::unordered_map<std::string, std::string>& /*updates*/,
    const std::unordered_set<std::string>& /*removals*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::vector<TableIdentifier>> HiveCatalog::ListTables(
    const Namespace& /*ns*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::CreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::UpdateTable(
    const TableIdentifier& /*identifier*/,
    const std::vector<std::unique_ptr<TableRequirement>>& /*requirements*/,
    const std::vector<std::unique_ptr<TableUpdate>>& /*updates*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Transaction>> HiveCatalog::StageCreateTable(
    const TableIdentifier& /*identifier*/, const std::shared_ptr<Schema>& /*schema*/,
    const std::shared_ptr<PartitionSpec>& /*spec*/,
    const std::shared_ptr<SortOrder>& /*order*/, const std::string& /*location*/,
    const std::unordered_map<std::string, std::string>& /*properties*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<bool> HiveCatalog::TableExists(const TableIdentifier& /*identifier*/) const {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::DropTable(const TableIdentifier& /*identifier*/, bool /*purge*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Status HiveCatalog::RenameTable(const TableIdentifier& /*from*/,
                                const TableIdentifier& /*to*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::LoadTable(
    const TableIdentifier& /*identifier*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

Result<std::shared_ptr<Table>> HiveCatalog::RegisterTable(
    const TableIdentifier& /*identifier*/,
    const std::string& /*metadata_file_location*/) {
  return NotImplemented("{}", kNotImplementedMessage);
}

}  // namespace iceberg::hive
