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

# Apache Iceberg™ C++

## Overview

iceberg-cpp is a C++ implementation of [Apache Iceberg™](https://iceberg.apache.org/), an open table format for large analytic datasets. It provides the data structures, algorithms, and catalog integrations required to read, write, and manage Iceberg tables from C++ applications or engines.

## Key Features

- **Modern C++** — Built as C++23 with ranges, concepts, and other modern idioms; public headers require only C++20
- **Cross-Platform** — Builds and runs on Linux, macOS, and Windows
- **Spec Compliance** — Full table spec support today; Puffin, View, and UDF specs are on the roadmap
- **Arrow-Native** — Uses the Arrow C Data Interface as the primary data API
- **Easy Engine Integration** — Interface-oriented, pluggable design for Catalog, FileIO, FileFormat, and more
- **Battery-Included** — Deep integration with Apache Arrow for columnar layout and rich file system support
- **REST Catalog Client** — Connects to any Iceberg REST catalog with pluggable authentication
- **File Format Support** — Built-in readers and writers for Apache Parquet and Apache Avro

## Quick Links

- [Getting Started](getting-started.md) — Build and install the library
- [Contributing](contributing.md) — Development setup and coding standards
- [Releases](releases.md) — Download and release history
- [API Documentation](api/index.html) — Doxygen-generated API reference

## Community

- [Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A)
- [Dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- [GitHub Issues](https://github.com/apache/iceberg-cpp/issues/new)

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
