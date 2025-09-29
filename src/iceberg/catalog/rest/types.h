// types.h

#pragma once

#include <memory>
#include <string>
#include <vector>

#include "iceberg/partition_spec.h"
#include "iceberg/schema.h"
#include "iceberg/sort_order.h"
#include "iceberg/table_identifier.h"
#include "iceberg/table_metadata.h"

namespace iceberg::rest {

struct ListNamespaceResponse {
  std::vector<std::vector<std::string>> namespaces;
  // TODO(Feiyang Li): Add next_page_token
};

struct UpdateNamespacePropsRequest {
  std::optional<std::vector<std::string>> removals;
  std::optional<std::unordered_map<std::string, std::string>> updates;
};

struct UpdateNamespacePropsResponse {
  std::vector<std::string> updated;
  std::vector<std::string> removed;
  std::optional<std::vector<std::string>> missing;
};

struct ListTableResponse {
  std::vector<TableIdentifier> identifiers;
  // TODO(Feiyang Li): Add next_page_token
};

struct RenameTableRequest {
  TableIdentifier source;
  TableIdentifier destination;
};

struct LoadTableResponse {
  std::optional<std::string> metadata_location;
  TableMetadata metadata;
  std::optional<std::unordered_map<std::string, std::string>> config;
};

struct CreateTableRequest {
  std::string name;
  std::optional<std::string> location;
  std::shared_ptr<Schema> schema;
  std::shared_ptr<PartitionSpec> partition_spec;  // optional
  std::shared_ptr<SortOrder> write_order;         // optional
  std::optional<bool> stage_create;
  std::optional<std::unordered_map<std::string, std::string>> properties;
};

struct RegisterTableRequest {
  std::string name;
  std::string metadata_location;
  std::optional<bool> overwrite;
};

}  // namespace iceberg::rest
