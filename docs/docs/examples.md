# Examples

This page provides practical examples of using Iceberg C++ in various scenarios.

## Demo Example

Here's a complete working example that demonstrates the basic workflow of using Iceberg C++:

```cpp
#include <iostream>

#include "iceberg/arrow/arrow_file_io.h"
#include "iceberg/avro/avro_register.h"
#include "iceberg/catalog/in_memory_catalog.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/table.h"
#include "iceberg/table_scan.h"

int main(int argc, char** argv) {
  if (argc != 4) {
    std::cerr << "Usage: " << argv[0]
              << " <warehouse_location> <table_name> <table_location>" << std::endl;
    return 0;
  }

  const std::string warehouse_location = argv[1];
  const std::string table_name = argv[2];
  const std::string table_location = argv[3];
  const std::unordered_map<std::string, std::string> properties;

  // Register file format readers
  iceberg::avro::RegisterAll();
  iceberg::parquet::RegisterAll();

  // Create an in-memory catalog
  auto catalog = iceberg::InMemoryCatalog::Make("test", iceberg::arrow::MakeLocalFileIO(),
                                                warehouse_location, properties);

  // Register a table
  auto register_result = catalog->RegisterTable({.name = table_name}, table_location);
  if (!register_result.has_value()) {
    std::cerr << "Failed to register table: " << register_result.error().message
              << std::endl;
    return 1;
  }

  // Load the table
  auto load_result = catalog->LoadTable({.name = table_name});
  if (!load_result.has_value()) {
    std::cerr << "Failed to load table: " << load_result.error().message << std::endl;
    return 1;
  }

  auto table = std::move(load_result.value());

  // Create a table scan
  auto scan_result = table->NewScan()->Build();
  if (!scan_result.has_value()) {
    std::cerr << "Failed to build scan: " << scan_result.error().message << std::endl;
    return 1;
  }

  auto scan = std::move(scan_result.value());

  // Plan the scan to get file tasks
  auto plan_result = scan->PlanFiles();
  if (!plan_result.has_value()) {
    std::cerr << "Failed to plan files: " << plan_result.error().message << std::endl;
    return 1;
  }

  // Print scan tasks
  std::cout << "Scan tasks: " << std::endl;
  auto scan_tasks = std::move(plan_result.value());
  for (const auto& scan_task : scan_tasks) {
    std::cout << " - " << scan_task->data_file()->file_path << std::endl;
  }

  return 0;
}
```

## Running the Example

To compile and run this example:

```bash
# Build the example
cd example
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build

# Run the example
./build/demo_example /path/to/warehouse my_table /path/to/table/metadata.json
```

## What This Example Does

1. **Setup**: Registers Avro and Parquet file format readers
2. **Catalog Creation**: Creates an in-memory catalog with local file I/O
3. **Table Registration**: Registers an existing table with the catalog
4. **Table Loading**: Loads the table metadata from the catalog
5. **Table Scanning**: Creates a scan operation and plans the file tasks
6. **Output**: Prints the data files that would be scanned

## Key Concepts Demonstrated

- **Error Handling**: Uses `Result<T>` types for proper error handling
- **File I/O**: Uses Arrow's local file system implementation
- **Catalog Operations**: Shows how to register and load tables
- **Table Scanning**: Demonstrates the basic scan planning workflow
- **File Format Support**: Registers multiple file format readers

## Next Steps

- Learn about [Configuration](configuration.md)
- Explore the [API Documentation](api/index.md)
- Check out [Contributing](contributing.md) to get involved
