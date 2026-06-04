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

# Getting Started

## Requirements

**Required:**

- C++23 compliant compiler (GCC 14+, Clang 16+, MSVC 2022+)
- CMake 3.25+ or Meson 1.5+
- [Ninja](https://ninja-build.org/) (recommended build backend)

## Quick Start

```bash
git clone https://github.com/apache/iceberg-cpp.git
cd iceberg-cpp
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
```

## Build with CMake

### Core Libraries

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Bundle Library (with vendored dependencies)

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
cmake --install build
```

### Bundle Library (with provided Apache Arrow)

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DCMAKE_PREFIX_PATH=/path/to/arrow -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
cmake --install build
```

### CMake Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `ICEBERG_BUILD_STATIC` | `ON` | Build static library |
| `ICEBERG_BUILD_SHARED` | `OFF` | Build shared library |
| `ICEBERG_BUILD_TESTS` | `ON` | Build tests |
| `ICEBERG_BUILD_BUNDLE` | `ON` | Build the battery-included library |
| `ICEBERG_BUILD_REST` | `ON` | Build REST catalog client |
| `ICEBERG_BUILD_REST_INTEGRATION_TESTS` | `OFF` | Build REST catalog integration tests |
| `ICEBERG_BUILD_HIVE` | `OFF` | Build Hive (HMS) catalog client |
| `ICEBERG_BUILD_SQL_CATALOG` | `OFF` | Build SQL catalog client |
| `ICEBERG_SQL_SQLITE` | `OFF` | Build the SQLite connector for the SQL catalog |
| `ICEBERG_SQL_POSTGRESQL` | `OFF` | Build the PostgreSQL connector for the SQL catalog |
| `ICEBERG_SQL_MYSQL` | `OFF` | Build the MySQL connector for the SQL catalog |
| `ICEBERG_ENABLE_ASAN` | `OFF` | Enable Address Sanitizer |
| `ICEBERG_ENABLE_UBSAN` | `OFF` | Enable Undefined Behavior Sanitizer |

## Build with Meson

```bash
meson setup builddir
meson compile -C builddir
meson test -C builddir --timeout-multiplier 0
```

Meson provides built-in equivalents for several CMake options:

- `--default-library=<shared|static|both>` instead of `ICEBERG_BUILD_STATIC` / `ICEBERG_BUILD_SHARED`
- `-Db_sanitize=address,undefined` instead of `ICEBERG_ENABLE_ASAN` / `ICEBERG_ENABLE_UBSAN`
- `--libdir`, `--bindir`, `--includedir` for install directories

| Option | Default | Description |
|--------|---------|-------------|
| `rest` | `enabled` | Build REST catalog client |
| `rest_integration_test` | `disabled` | Build integration test for REST catalog |
| `tests` | `enabled` | Build tests |

## Running Tests

Run all tests:

```bash
ctest --test-dir build --output-on-failure
```

Run a specific test suite:

```bash
ctest --test-dir build -R schema_test --output-on-failure
```

## Build Examples

After installing the core libraries:

```bash
cd example
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build
```

If using provided Apache Arrow, include both paths:

```bash
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/arrow"
```

## Customizing Dependency URLs

If you experience network issues when downloading dependencies, you can override the download URLs using environment variables:

| Variable | Dependency |
|----------|------------|
| `ICEBERG_ARROW_URL` | Apache Arrow tarball |
| `ICEBERG_AVRO_URL` | Apache Avro tarball |
| `ICEBERG_AVRO_GIT_URL` | Apache Avro git repository |
| `ICEBERG_NANOARROW_URL` | Nanoarrow tarball |
| `ICEBERG_CROARING_URL` | CRoaring tarball |
| `ICEBERG_NLOHMANN_JSON_URL` | nlohmann-json tarball |
| `ICEBERG_CPR_URL` | cpr tarball |

Example:

```bash
export ICEBERG_ARROW_URL="https://your-mirror.com/apache-arrow-22.0.0.tar.gz"
cmake -S . -B build -G Ninja
```
