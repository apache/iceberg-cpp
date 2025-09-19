# Data Types

Iceberg C++ supports a comprehensive set of data types for table schemas.

## Primitive Types

### Numeric Types

#### Integer Types
```cpp
// 32-bit signed integer
auto int_type = iceberg::int32();

// 64-bit signed integer
auto long_type = iceberg::int64();
```

#### Floating Point Types
```cpp
// 32-bit floating point
auto float_type = iceberg::float32();

// 64-bit floating point
auto double_type = iceberg::float64();
```

#### Decimal Type
```cpp
// Decimal with precision and scale
auto decimal_type = iceberg::decimal(10, 2);
```

### String Types

#### String
```cpp
// Variable-length UTF-8 string
auto string_type = iceberg::string();
```

#### Binary
```cpp
// Variable-length binary data
auto binary_type = iceberg::binary();
```

### Other Primitive Types

#### Boolean
```cpp
// Boolean true/false
auto bool_type = iceberg::boolean();
```

#### Date
```cpp
// Date (days since epoch)
auto date_type = iceberg::date();
```

#### Time
```cpp
// Time (microseconds since midnight)
auto time_type = iceberg::time();
```

#### Timestamp
```cpp
// Timestamp (microseconds since epoch)
auto timestamp_type = iceberg::timestamp();

// Timestamp with timezone
auto timestamp_tz_type = iceberg::timestamp_tz();
```

#### UUID
```cpp
// 128-bit UUID
auto uuid_type = iceberg::uuid();
```

## Complex Types

### Struct Type
```cpp
// Struct with named fields
auto struct_type = iceberg::struct_({
    iceberg::SchemaField::MakeRequired(1, "id", iceberg::int64()),
    iceberg::SchemaField::MakeRequired(2, "name", iceberg::string()),
    iceberg::SchemaField::MakeRequired(3, "age", iceberg::int32())
});
```

### List Type
```cpp
// List of strings
auto list_type = iceberg::list(iceberg::SchemaField::MakeRequired(1, "element", iceberg::string()));

// List of structs
auto struct_list = iceberg::list(iceberg::SchemaField::MakeRequired(1, "element", struct_type));
```

### Map Type
```cpp
// Map from string to string
auto map_type = iceberg::map(
    iceberg::SchemaField::MakeRequired(1, "key", iceberg::string()),
    iceberg::SchemaField::MakeRequired(2, "value", iceberg::string())
);
```

## Working with Values

### Creating Literals
```cpp
// String literal
auto string_literal = iceberg::Literal::String("hello");

// Integer literal
auto int_literal = iceberg::Literal::Int(42);

// Boolean literal
auto bool_literal = iceberg::Literal::Boolean(true);

// Long literal
auto long_literal = iceberg::Literal::Long(123456789L);

// Float literal
auto float_literal = iceberg::Literal::Float(3.14f);

// Double literal
auto double_literal = iceberg::Literal::Double(3.14159);
```

### Type Casting
```cpp
// Cast literal to different type
auto cast_result = literal.CastTo(target_type);
if (cast_result.ok()) {
    auto casted = std::move(cast_result).value();
    // Use casted literal
}
```

## Error Handling

Type operations return `Result` types for error handling:

```cpp
auto decimal_result = iceberg::decimal(10, 2);
if (!decimal_result.ok()) {
    std::cerr << "Type operation failed: " << decimal_result.error() << std::endl;
}
```
