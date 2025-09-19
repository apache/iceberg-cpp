# Catalog API

The Catalog API provides functionality for discovering and managing tables within an Iceberg catalog.

## Overview

The `iceberg::Catalog` class is the main interface for catalog operations. Currently, only the `InMemoryCatalog` implementation is available.

## Basic Usage

```cpp
#include <iceberg/catalog/in_memory_catalog.h>

// Create an in-memory catalog
auto file_io = iceberg::arrow::MakeLocalFileIO();
auto catalog = iceberg::InMemoryCatalog::Make(
    "test_catalog",
    file_io,
    "/path/to/warehouse",
    {}
);

// List tables in a namespace
auto tables = catalog->ListTables(namespace_name);

// Load an existing table
auto table = catalog->LoadTable(table_identifier);
```

## Key Classes

### InMemoryCatalog
In-memory implementation of the catalog interface. Suitable for testing and development.

### TableIdentifier
Represents a fully qualified table name within a catalog.

### Namespace
Represents a namespace within a catalog.

## Examples

### Listing Tables
```cpp
auto tables = catalog->ListTables("my_database");
for (const auto& table_id : tables) {
    std::cout << "Table: " << table_id.ToString() << std::endl;
}
```

### Loading an Existing Table
```cpp
auto table_identifier = iceberg::TableIdentifier{
    .ns = iceberg::Namespace{{"my_database"}},
    .name = "my_table"
};
auto table = catalog->LoadTable(table_identifier);
```

## Configuration

In-memory catalogs are configured with basic properties:

```cpp
std::unordered_map<std::string, std::string> properties = {
    {"warehouse", "/path/to/warehouse"}
};

auto catalog = iceberg::InMemoryCatalog::Make(
    "catalog_name",
    file_io,
    "/path/to/warehouse",
    properties
);
```

## Error Handling

Catalog operations return `Result` types for error handling:

```cpp
auto result = catalog->LoadTable(table_identifier);
if (result.ok()) {
    auto table = std::move(result).value();
    // Use table
} else {
    std::cerr << "Catalog operation failed: " << result.error() << std::endl;
}
```
