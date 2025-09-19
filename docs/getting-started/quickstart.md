# Quick Start

This guide will help you get started with Iceberg C++ in just a few minutes.

## Connecting to a Catalog

Iceberg leverages the catalog to have one centralized place to organize the tables. This can be a traditional Hive catalog, AWS Glue catalog, or an implementation of Iceberg's own REST protocol.

For demonstration, we'll configure the catalog to use the `InMemoryCatalog` implementation:

```cpp
#include <iceberg/catalog/in_memory_catalog.h>
#include <iceberg/table.h>

// Create an in-memory catalog
auto catalog = iceberg::InMemoryCatalog::Make(
    "default",
    file_io,
    "/tmp/warehouse",
    {}
);

// Create a namespace
catalog->CreateNamespace("default");
```

## Working with Tables

```cpp
#include <iceberg/table.h>
#include <iceberg/schema.h>

// Create a table
auto table = catalog->CreateTable("default.my_table", schema);

// Load table
auto table = catalog->LoadTable("default.my_table");

// Scan table
auto scan_builder = table->NewScan();
auto scan = scan_builder->Build();
```

## Basic Operations

### Creating a Schema

```cpp
#include <iceberg/schema.h>
#include <iceberg/type.h>

// Create a simple schema
auto schema = iceberg::Schema::Make({
    iceberg::NestedField::Make(1, "id", iceberg::LongType::Make(), true),
    iceberg::NestedField::Make(2, "name", iceberg::StringType::Make(), false),
    iceberg::NestedField::Make(3, "age", iceberg::IntType::Make(), false)
});
```

### Writing Data

```cpp
// Create a writer
auto writer = table->NewAppend();

// Write data
writer->AppendFile(data_file);

// Commit
writer->Commit();
```

### Reading Data

```cpp
// Create a scan
auto scan = table->NewScan()
    ->Select({"id", "name", "age"})
    ->Where("age > 18")
    ->Build();

// Execute scan
auto result = scan->ToArrow();
```

## Next Steps

- Learn about [Configuration](../configuration.md)
- Check out [Examples](../examples.md)
