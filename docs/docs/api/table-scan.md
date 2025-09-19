<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Table Scan API

The Table Scan API provides functionality for reading data from Iceberg tables with filtering, projection, and various scan options.

## Overview

The `iceberg::TableScan` class provides methods for:

- Selecting specific columns
- Applying filters
- Configuring scan options
- Reading data as Arrow streams

## Basic Usage

```cpp
#include <iceberg/table_scan.h>

// Create a table scan builder
auto scan_builder = table->NewScan();

// Configure the scan
auto scan_result = scan_builder
    .WithColumnNames({"id", "name", "email"})
    .WithLimit(1000)
    .Build();

if (scan_result) {
    auto scan = std::move(scan_result).value();
    // Use scan
}
```

## Key Classes

### TableScanBuilder
Builder for creating table scan operations.

### TableScan
Main interface for table scanning operations.

### FileScanTask
Represents a scan task for a specific data file.

## Column Selection

### Select Specific Columns
```cpp
auto scan_result = table->NewScan()
    .WithColumnNames({"id", "name", "email"})
    .Build();
```

### Select All Columns
```cpp
auto scan_result = table->NewScan()
    .Build();  // No column names specified = all columns
```

## Filtering

### Apply Filter Expression
```cpp
// Note: Filter expressions need to be implemented
auto filter = /* create filter expression */;
auto scan_result = table->NewScan()
    .WithFilter(filter)
    .Build();
```

## Scan Options

### Limit Results
```cpp
auto scan_result = table->NewScan()
    .WithColumnNames({"id", "name"})
    .WithLimit(100)
    .Build();
```

### Set Scan Properties
```cpp
auto scan_result = table->NewScan()
    .WithColumnNames({"id", "name"})
    .WithOption("read.parquet.vectorization.enabled", "true")
    .Build();
```

### Snapshot Selection
```cpp
auto scan_result = table->NewScan()
    .WithColumnNames({"id", "name"})
    .WithSnapshotId(snapshot_id)
    .Build();
```

## Data Reading

### Read as Arrow Stream
```cpp
auto scan_result = scan->PlanFiles();
if (scan_result) {
    auto tasks = std::move(scan_result).value();
    for (const auto& task : tasks) {
        auto stream_result = task->ToArrow(file_io, projected_schema, filter);
        if (stream_result) {
            auto stream = std::move(stream_result).value();
            // Process Arrow stream
        }
    }
}
```

## Error Handling

Scan operations return `Result` types for error handling:

```cpp
auto scan_result = table->NewScan()
    .WithColumnNames({"id", "name"})
    .Build();

if (!scan_result) {
    std::cerr << "Scan creation failed: " << scan_result.error() << std::endl;
    return;
}

auto scan = std::move(scan_result).value();
auto files_result = scan->PlanFiles();
if (!files_result) {
    std::cerr << "Scan execution failed: " << files_result.error() << std::endl;
}
```

## Performance Tips

1. **Use column pruning** - Only select the columns you need
2. **Apply filters early** - Use filter expressions to reduce data volume
3. **Use snapshot selection** - Scan specific snapshots when possible
4. **Use Arrow format** - Arrow provides efficient columnar data processing
