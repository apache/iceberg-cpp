# Examples

This page contains practical examples of using Iceberg C++.

## Basic Table Operations

### Creating a Table

```cpp
#include <iceberg/catalog/in_memory_catalog.h>
#include <iceberg/schema.h>
#include <iceberg/type.h>

// Create catalog
auto catalog = iceberg::InMemoryCatalog::Make(
    "default",
    file_io,
    "/tmp/warehouse",
    {}
);

// Create schema
auto schema = iceberg::Schema::Make({
    iceberg::SchemaField::Make(1, "id", iceberg::LongType::Make(), true),
    iceberg::SchemaField::Make(2, "name", iceberg::StringType::Make(), false),
    iceberg::SchemaField::Make(3, "age", iceberg::IntType::Make(), true)
});

// Create table
auto table = catalog->CreateTable("default.users", schema);
```

### Reading Data

```cpp
#include <iceberg/table_scan.h>

// Create scan builder
auto scan_builder = table->NewScan();

// Add filters
scan_builder->Filter(iceberg::expressions::Equal("age", 25));

// Execute scan
auto scan = scan_builder->Build();
auto scan_task = scan->PlanFiles();

// Read data
for (const auto& task : scan_task) {
    auto data = task->Read();
    // Process data...
}
```

### Writing Data

```cpp
#include <iceberg/writer.h>
#include <arrow/table.h>

// Create writer
auto writer = table->NewWriter();

// Write data
auto arrow_table = /* your Arrow table */;
writer->Write(arrow_table);

// Close writer
writer->Close();
```

## Schema Evolution

### Adding a Column

```cpp
// Get current schema
auto current_schema = table->GetSchema();

// Add new column
auto new_field = iceberg::SchemaField::Make(
    4, "email", iceberg::StringType::Make(), true
);

// Create new schema
auto new_schema = current_schema->AddField(new_field);

// Update table schema
table->UpdateSchema(new_schema);
```

### Removing a Column

```cpp
// Remove column by ID
auto updated_schema = current_schema->RemoveField(3);

// Update table schema
table->UpdateSchema(updated_schema);
```

## Partitioning

### Creating Partitioned Tables

```cpp
#include <iceberg/partition_spec.h>

// Create partition spec
auto partition_spec = iceberg::PartitionSpec::Make({
    iceberg::PartitionField::Make(1, 1000, "age", iceberg::IdentityTransform::Make())
});

// Create table with partitioning
auto table = catalog->CreateTable(
    "default.partitioned_users",
    schema,
    partition_spec
);
```

## File Formats

### Working with Parquet

```cpp
#include <iceberg/parquet/parquet_reader.h>

// Create Parquet reader
auto reader = iceberg::parquet::ParquetReader::Make();

// Configure reader
iceberg::ReaderOptions options;
options.SetFileLocation("/path/to/file.parquet");

// Open and read
reader->Open(options);
auto data = reader->Next();
```

### Working with Avro

```cpp
#include <iceberg/avro/avro_reader.h>

// Create Avro reader
auto reader = iceberg::avro::AvroReader::Make();

// Configure reader
iceberg::ReaderOptions options;
options.SetFileLocation("/path/to/file.avro");

// Open and read
reader->Open(options);
auto data = reader->Next();
```

## Error Handling

```cpp
#include <iceberg/result.h>

// Using Result for error handling
auto result = table->GetSchema();
if (result.IsOk()) {
    auto schema = result.Value();
    // Use schema...
} else {
    auto error = result.Error();
    std::cerr << "Error: " << error.message << std::endl;
}
```

## Configuration

### Setting up FileIO

```cpp
#include <iceberg/arrow/arrow_file_io.h>

// Create Arrow-based FileIO
auto file_io = iceberg::arrow::ArrowFileSystemFileIO::Make(
    std::make_shared<arrow::fs::LocalFileSystem>()
);
```

### Catalog Configuration

```cpp
// Configure catalog properties
std::unordered_map<std::string, std::string> properties = {
    {"warehouse", "/tmp/warehouse"},
    {"default-namespace", "default"}
};

auto catalog = iceberg::InMemoryCatalog::Make(
    "default",
    file_io,
    "/tmp/warehouse",
    properties
);
```
