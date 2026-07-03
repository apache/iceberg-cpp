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

![Iceberg](https://iceberg.apache.org/assets/images/Iceberg-logo.svg)

[![Slack](https://img.shields.io/badge/chat-on%20Slack-brightgreen.svg)](https://apache-iceberg.slack.com/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/apache/iceberg-cpp)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Apache Iceberg™ C++

C++ implementation of [Apache Iceberg™](https://iceberg.apache.org/).

## Requirements

**Required:**

- C++23 compliant compiler (GCC 14+, Clang 18+, MSVC 2022+)
- CMake 3.25+ or Meson 1.5+
- [Ninja](https://ninja-build.org/) (recommended build backend)

**Optional:**

- Python 3 and [pre-commit](https://pre-commit.com/) (for linting)

## Quick Start

```bash
git clone https://github.com/apache/iceberg-cpp.git
cd iceberg-cpp
cmake -S . -B build -G Ninja
cmake --build build
ctest --test-dir build --output-on-failure
```

## Build with CMake

### Build, Run Tests and Install Core Libraries

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

To run a specific test suite:

```bash
ctest --test-dir build -R schema_test --output-on-failure
```

### Build and Install Iceberg Bundle Library

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

Meson-specific options (configured via `-D<option>=<value>`):

| Option | Default | Description |
|--------|---------|-------------|
| `rest` | `enabled` | Build REST catalog client |
| `rest_integration_test` | `disabled` | Build integration test for REST catalog |
| `tests` | `enabled` | Build tests |

### Build Examples

After installing the core libraries, you can build the examples:

```bash
cd example
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build
```

If you are using provided Apache Arrow, include `/path/to/arrow` in `CMAKE_PREFIX_PATH`:

```bash
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/arrow"
```

## Customizing Dependency URLs

If you experience network issues when downloading dependencies, you can customize the download URLs using environment variables:

- `ICEBERG_ARROW_URL`: Apache Arrow tarball URL
- `ICEBERG_AVRO_URL`: Apache Avro tarball URL
- `ICEBERG_AVRO_GIT_URL`: Apache Avro git repository URL
- `ICEBERG_NANOARROW_URL`: Nanoarrow tarball URL
- `ICEBERG_CROARING_URL`: CRoaring tarball URL
- `ICEBERG_UTF8PROC_URL`: utf8proc tarball URL
- `ICEBERG_NLOHMANN_JSON_URL`: nlohmann-json tarball URL
- `ICEBERG_SPDLOG_URL`: spdlog tarball URL
- `ICEBERG_CPR_URL`: cpr tarball URL

Example:

```bash
export ICEBERG_ARROW_URL="https://your-mirror.com/apache-arrow-22.0.0.tar.gz"
cmake -S . -B build
```

## Contribute

Apache Iceberg is an active open-source project, governed under the Apache Software Foundation (ASF). Iceberg-cpp is open to people who want to contribute to it. Here are some ways to get involved:

- Submit [Issues](https://github.com/apache/iceberg-cpp/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](<mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)>) / [unsubscribe](<mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)>) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- Talk to the community directly at [Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A).

The Apache Iceberg community is built on the principles described in the [Apache Way](https://www.apache.org/theapacheway/index.html) and all who engage with the community are expected to be respectful, open, come with the best interests of the community in mind, and abide by the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

In addition, contributors using AI-assisted tools must follow the documented guidelines for AI-assisted contributions available on the Iceberg website: [https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions](https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions).

### Linting

Install the python package `pre-commit` and run once `pre-commit install`.

```bash
pip install pre-commit
pre-commit install
```

This will setup a git pre-commit-hook that is executed on each commit and will report the linting problems. To run all hooks on all files use `pre-commit run -a`.

### Dev Containers

We provide Dev Container configuration file templates.

To use a Dev Container as your development environment, follow the steps below, then select `Dev Containers: Reopen in Container` from VS Code's Command Palette.

```bash
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

If you make improvements that could benefit all developers, please update the template files and submit a pull request.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
