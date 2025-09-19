# Iceberg C++

Iceberg C++ is a C++ library for programmatic access to Iceberg table metadata as well as to table data in Iceberg format. It is a C++ implementation of the Iceberg table spec.

## Installation

Before installing Iceberg C++, make sure you have the required dependencies:

- CMake 3.25 or higher
- C++17 compatible compiler
- Arrow C++ library
- Avro C++ library
- Thrift library

### Dependencies

| Key | Description |
|-----|-------------|
| `arrow` | Support for Apache Arrow integration |
| `avro` | Support for Avro file format |
| `parquet` | Support for Parquet file format |
| `thrift` | Support for Thrift serialization |

## Quick Start

```cpp
#include <iceberg/iceberg.h>

// Create a catalog
auto catalog = iceberg::InMemoryCatalog::Make("test_catalog", file_io, warehouse_location);

// Create a table
auto table = catalog->CreateTable(table_identifier, schema);
```

## Features

- **Schema Evolution**: Support for adding, dropping, and renaming columns
- **Partitioning**: Flexible partitioning strategies
- **Time Travel**: Access historical versions of data
- **ACID Transactions**: Atomic operations on tables
- **Multiple File Formats**: Support for Parquet and Avro
- **Arrow Integration**: Seamless integration with Apache Arrow

## Documentation

- [Examples](examples.md)
- [Configuration](configuration.md)

## Contributing

We welcome contributions! Please see our [Contributing Guide](https://github.com/apache/iceberg-cpp/blob/main/CONTRIBUTING.md) for details.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](https://github.com/apache/iceberg-cpp/blob/main/LICENSE) for details.
