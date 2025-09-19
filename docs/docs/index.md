# Iceberg C++

Iceberg C++ is a C++ library for programmatic access to Iceberg table metadata as well as to table data in Iceberg format. It is a C++ implementation of the Iceberg table spec.

## Features

- **Schema Evolution**: Support for adding, dropping, and renaming columns
- **Partitioning**: Flexible partitioning strategies
- **Time Travel**: Access historical versions of data
- **ACID Transactions**: Atomic operations on tables
- **Multiple File Formats**: Support for Parquet and Avro
- **Arrow Integration**: Seamless integration with Apache Arrow

## Installation

### Requirements

- CMake 3.25 or higher
- C++23 compliant compiler

### Build Core Libraries

```bash
cd iceberg-cpp
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build with Bundled Dependencies

#### Vendored Apache Arrow (default)

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

#### Provided Apache Arrow

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DCMAKE_PREFIX_PATH=/path/to/arrow -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build Examples

After installing the core libraries, you can build the examples:

```bash
cd iceberg-cpp/example
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build
```

If you are using provided Apache Arrow, you need to include `/path/to/arrow` in `CMAKE_PREFIX_PATH`:

```bash
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/arrow"
```

### Dependencies

| Key | Description |
|-----|-------------|
| `arrow` | Support for Apache Arrow integration |
| `avro` | Support for Avro file format |
| `parquet` | Support for Parquet file format |
| `thrift` | Support for Thrift serialization |

## Quick Start

This guide will help you get started with Iceberg C++ in just a few minutes.

### Connecting to a Catalog

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

### Working with Tables

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

### Basic Operations

#### Creating a Schema

```cpp
#include <iceberg/schema.h>
#include <iceberg/type.h>

// Create a simple schema
auto schema = std::make_shared<iceberg::Schema>(std::vector<iceberg::SchemaField>{
    iceberg::SchemaField::MakeRequired(1, "id", iceberg::int64()),
    iceberg::SchemaField::MakeOptional(2, "name", iceberg::string()),
    iceberg::SchemaField::MakeOptional(3, "age", iceberg::int32())
});
```

#### Writing Data

```cpp
// Create a writer
auto writer = table->NewAppend();

// Write data
writer->AppendFile(data_file);

// Commit
writer->Commit();
```

#### Reading Data

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

- Learn about [Configuration](configuration.md)
- Check out [Examples](examples.md)
- Explore the [API Documentation](api/index.md)
- See [Releases](releases.md) for download options
- Get involved in [Contributing](contributing.md)

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/apache/iceberg-cpp/blob/main/LICENSE) for details.
