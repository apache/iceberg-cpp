# Table API

The Table API provides functionality for working with Iceberg tables, including reading table metadata and creating table scans.

## Overview

The `iceberg::Table` class is the main entry point for table operations. It provides methods for:

- Reading table metadata
- Creating table scans
- Accessing table schema and snapshots

## Basic Usage

```cpp
#include <iceberg/table.h>

// Table is typically created by catalog
auto table = catalog->LoadTable(table_identifier);

// Get table metadata
auto metadata = table->metadata();

// Create a table scan
auto scan = table->NewScan();
```

## Key Classes

### Table
Main table interface for Iceberg operations.

### TableMetadata
Contains table schema, properties, and version information.

### TableScanBuilder
Builder for creating table scan operations.

## Examples

### Reading Table Schema
```cpp
auto schema = table->metadata()->schema();
for (const auto& field : schema->fields()) {
    std::cout << "Field: " << field.name()
              << ", Type: " << field.type()->ToString() << std::endl;
}
```

### Creating a Table Scan
```cpp
auto scan_builder = table->NewScan();
auto scan = scan_builder
    .WithColumnNames({"id", "name", "email"})
    .Build();
```

### Accessing Table Snapshots
```cpp
auto current_snapshot = table->current_snapshot();
if (current_snapshot) {
    auto snapshot = std::move(current_snapshot.value());
    std::cout << "Current snapshot ID: " << snapshot->snapshot_id << std::endl;
}
```

## Error Handling

Table operations return `Result` types for error handling:

```cpp
auto result = table->current_snapshot();
if (result) {
    auto snapshot = std::move(result.value());
    // Use snapshot
} else {
    std::cerr << "Table operation failed: " << result.error() << std::endl;
}
```
