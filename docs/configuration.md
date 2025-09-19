# Configuration

This page describes how to configure Iceberg C++ for different environments and use cases.

## Catalog Configuration

### In-Memory Catalog

The in-memory catalog is useful for testing and development:

```cpp
#include <iceberg/catalog/in_memory_catalog.h>

auto catalog = iceberg::InMemoryCatalog::Make(
    "catalog_name",           // Catalog name
    file_io,                  // FileIO implementation
    "/path/to/warehouse",     // Warehouse location
    {}                        // Additional properties
);
```

### Hive Catalog

For production environments with Hive Metastore:

```cpp
#include <iceberg/catalog/hive_catalog.h>

std::unordered_map<std::string, std::string> properties = {
    {"uri", "thrift://metastore-host:9083"},
    {"warehouse", "hdfs://namenode:9000/warehouse"}
};

auto catalog = iceberg::HiveCatalog::Make(
    "hive_catalog",
    file_io,
    properties
);
```

### REST Catalog

For REST-based catalog implementations:

```cpp
#include <iceberg/catalog/rest_catalog.h>

std::unordered_map<std::string, std::string> properties = {
    {"uri", "https://catalog-server:8181"},
    {"warehouse", "s3://my-bucket/warehouse"},
    {"s3.endpoint", "https://s3.amazonaws.com"}
};

auto catalog = iceberg::RestCatalog::Make(
    "rest_catalog",
    file_io,
    properties
);
```

## FileIO Configuration

### Local File System

```cpp
#include <iceberg/arrow/arrow_file_io.h>

auto file_system = std::make_shared<arrow::fs::LocalFileSystem>();
auto file_io = iceberg::arrow::ArrowFileSystemFileIO::Make(file_system);
```

### S3 File System

```cpp
#include <iceberg/arrow/arrow_file_io.h>
#include <arrow/filesystem/s3fs.h>

arrow::fs::S3Options s3_options;
s3_options.region = "us-west-2";
s3_options.access_key_id = "your-access-key";
s3_options.secret_key = "your-secret-key";

auto file_system = std::make_shared<arrow::fs::S3FileSystem>(s3_options);
auto file_io = iceberg::arrow::ArrowFileSystemFileIO::Make(file_system);
```

### HDFS File System

```cpp
#include <iceberg/arrow/arrow_file_io.h>
#include <arrow/filesystem/hdfs.h>

arrow::fs::HdfsOptions hdfs_options;
hdfs_options.host = "namenode";
hdfs_options.port = 9000;

auto file_system = std::make_shared<arrow::fs::HadoopFileSystem>(hdfs_options);
auto file_io = iceberg::arrow::ArrowFileSystemFileIO::Make(file_system);
```

## Schema Configuration

### Data Types

Iceberg C++ supports various data types:

```cpp
#include <iceberg/type.h>

// Primitive types
auto int_type = iceberg::IntType::Make();
auto long_type = iceberg::LongType::Make();
auto string_type = iceberg::StringType::Make();
auto binary_type = iceberg::BinaryType::Make();
auto boolean_type = iceberg::BooleanType::Make();
auto float_type = iceberg::FloatType::Make();
auto double_type = iceberg::DoubleType::Make();

// Temporal types
auto date_type = iceberg::DateType::Make();
auto time_type = iceberg::TimeType::Make();
auto timestamp_type = iceberg::TimestampType::Make();
auto timestamp_tz_type = iceberg::TimestampTzType::Make();

// Decimal type
auto decimal_type = iceberg::DecimalType::Make(10, 2);  // precision=10, scale=2

// UUID type
auto uuid_type = iceberg::UuidType::Make();

// Fixed type
auto fixed_type = iceberg::FixedType::Make(16);  // 16 bytes
```

### Nested Types

```cpp
// Struct type
auto struct_type = iceberg::StructType::Make({
    iceberg::SchemaField::Make(1, "field1", iceberg::StringType::Make(), false),
    iceberg::SchemaField::Make(2, "field2", iceberg::IntType::Make(), true)
});

// List type
auto list_type = iceberg::ListType::Make(
    1,  // field ID
    iceberg::StringType::Make()  // element type
);

// Map type
auto map_type = iceberg::MapType::Make(
    1,  // field ID
    iceberg::StringType::Make(),  // key type
    iceberg::IntType::Make()      // value type
);
```

## Partitioning Configuration

### Partition Transforms

```cpp
#include <iceberg/transform.h>

// Identity transform (no transformation)
auto identity = iceberg::IdentityTransform::Make();

// Bucket transform
auto bucket = iceberg::BucketTransform::Make(10);  // 10 buckets

// Truncate transform
auto truncate = iceberg::TruncateTransform::Make(10);  // truncate to 10 chars

// Date/time transforms
auto year = iceberg::YearTransform::Make();
auto month = iceberg::MonthTransform::Make();
auto day = iceberg::DayTransform::Make();
auto hour = iceberg::HourTransform::Make();
```

### Partition Specifications

```cpp
#include <iceberg/partition_spec.h>

// Create partition spec
auto partition_spec = iceberg::PartitionSpec::Make({
    iceberg::PartitionField::Make(
        1,                                    // source field ID
        1000,                                // partition field ID
        "year",                              // field name
        iceberg::YearTransform::Make()       // transform
    ),
    iceberg::PartitionField::Make(
        2,                                    // source field ID
        1001,                                // partition field ID
        "bucket",                            // field name
        iceberg::BucketTransform::Make(10)   // transform
    )
});
```

## Sorting Configuration

### Sort Fields

```cpp
#include <iceberg/sort_field.h>

// Create sort field
auto sort_field = iceberg::SortField::Make(
    1,                                    // source field ID
    iceberg::SortDirection::Ascending,    // direction
    iceberg::NullOrder::NullsFirst        // null order
);
```

### Sort Orders

```cpp
#include <iceberg/sort_order.h>

// Create sort order
auto sort_order = iceberg::SortOrder::Make({
    iceberg::SortField::Make(1, iceberg::SortDirection::Ascending, iceberg::NullOrder::NullsFirst),
    iceberg::SortField::Make(2, iceberg::SortDirection::Descending, iceberg::NullOrder::NullsLast)
});
```

## Reader/Writer Configuration

### Reader Options

```cpp
#include <iceberg/reader.h>

iceberg::ReaderOptions options;
options.SetFileLocation("/path/to/file");
options.SetSchema(schema);
options.SetProjection(projection);
options.SetFilter(filter);
```

### Writer Options

```cpp
#include <iceberg/writer.h>

iceberg::WriterOptions options;
options.SetFileLocation("/path/to/file");
options.SetSchema(schema);
options.SetPartitionSpec(partition_spec);
options.SetSortOrder(sort_order);
```

## Environment Variables

You can configure Iceberg C++ using environment variables:

- `ICEBERG_WAREHOUSE`: Default warehouse location
- `ICEBERG_CATALOG_TYPE`: Default catalog type
- `ICEBERG_CATALOG_URI`: Default catalog URI
- `ICEBERG_LOG_LEVEL`: Logging level (DEBUG, INFO, WARN, ERROR)
