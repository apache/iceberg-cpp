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

# SQL Catalog

`SqlCatalog` implements the Iceberg `Catalog` API on top of a relational
database. Its schema is compatible with the Apache Iceberg Java `JdbcCatalog`
and stores catalog rows in `iceberg_tables` and
`iceberg_namespace_properties`.

## Build

The SQL catalog is currently available through the CMake build only. Meson does
not build or install it yet.

Enable the catalog at configure time:

```bash
cmake -S . -B build -DICEBERG_BUILD_SQL_CATALOG=ON
```

Built-in connectors are optional:

| CMake option | Default | Native dependency |
|--------------|---------|-------------------|
| `ICEBERG_SQL_SQLITE` | `OFF` | SQLite3 |
| `ICEBERG_SQL_POSTGRESQL` | `OFF` | libpq |
| `ICEBERG_SQL_MYSQL` | `OFF` | libmysqlclient |

The built-in connectors use
[sqlpp23](https://github.com/rbock/sqlpp23), which is fetched by CMake when a
connector is enabled. Projects can also supply their own `CatalogStore`
implementation and disable all built-in connectors.

## Usage

```cpp
#include "iceberg/catalog/sql/sql_catalog.h"

using iceberg::sql::SqlCatalog;
using iceberg::sql::SqlCatalogConfig;

SqlCatalogConfig config{
    .name = "prod",
    .uri = "/var/lib/iceberg/catalog.db",
    .warehouse_location = "s3://my-bucket/warehouse",
};

auto catalog = SqlCatalog::MakeSqliteCatalog(config, file_io).value();
```

Connector factories are always declared in the public headers. If a connector
was not built, its factory returns `ErrorKind::kNotSupported`.
