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

# Schema API

The Schema API provides functionality for working with Iceberg table schemas.

## Overview

The `iceberg::Schema` class represents the structure of an Iceberg table, including field definitions, types, and constraints.

## Basic Usage

```cpp
#include <iceberg/schema.h>

// Create schema fields
std::vector<iceberg::SchemaField> fields = {
    iceberg::SchemaField::MakeRequired(1, "id", iceberg::int64()),
    iceberg::SchemaField::MakeRequired(2, "name", iceberg::string()),
    iceberg::SchemaField::MakeOptional(3, "email", iceberg::string())
};

// Create schema
auto schema = std::make_shared<iceberg::Schema>(std::move(fields));
```

## Key Classes

### Schema
Represents a complete table schema with fields and metadata.

### SchemaField
Represents a single field in the schema.

### Type System
Various type classes for different data types.

## Field Operations

### Accessing Fields
```cpp
// Get field by name
auto field_result = schema->FindFieldByName("name");
if (field_result && field_result.value().has_value()) {
    auto field = field_result.value().value().get();
    std::cout << "Field: " << field.name() << std::endl;
}

// Get field by ID
auto field_result = schema->FindFieldById(1);
if (field_result && field_result.value().has_value()) {
    auto field = field_result.value().value().get();
    std::cout << "Field: " << field.name() << std::endl;
}

// List all fields
for (const auto& field : schema->fields()) {
    std::cout << "Field: " << field.name()
              << ", Type: " << field.type()->ToString() << std::endl;
}
```

## Type System

### Primitive Types
```cpp
// Numeric types
auto int_type = iceberg::int32();
auto long_type = iceberg::int64();
auto float_type = iceberg::float32();
auto double_type = iceberg::float64();

// String types
auto string_type = iceberg::string();
auto binary_type = iceberg::binary();

// Boolean type
auto bool_type = iceberg::boolean();
```

### Complex Types
```cpp
// Struct type
auto struct_type = iceberg::struct_({
    iceberg::SchemaField::MakeRequired(1, "id", iceberg::int64()),
    iceberg::SchemaField::MakeRequired(2, "name", iceberg::string())
});

// List type
auto list_type = iceberg::list(iceberg::SchemaField::MakeRequired(1, "element", iceberg::string()));

// Map type
auto map_type = iceberg::map(
    iceberg::SchemaField::MakeRequired(1, "key", iceberg::string()),
    iceberg::SchemaField::MakeRequired(2, "value", iceberg::string())
);
```

## Error Handling

Schema operations return `Result` types for error handling:

```cpp
auto field_result = schema->FindFieldByName("nonexistent_field");
if (!field_result.ok()) {
    std::cerr << "Schema operation failed: " << field_result.error() << std::endl;
}
```
