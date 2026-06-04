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
database. Its on-disk schema is compatible with the Apache Iceberg Java
`JdbcCatalog`: two tables, `iceberg_tables` and `iceberg_namespace_properties`,
scoped by a catalog name so multiple catalogs can share one database.

## Design

`SqlCatalog` owns the Iceberg catalog behavior. It validates namespaces, reads
and writes table metadata files, and performs optimistic-concurrency commits.
Database access is delegated to a small storage interface:

```
Application
    |
    v
SqlCatalog
    |
    | CatalogStore API
    v
CatalogStore implementation
    |
    v
SQL database
  - iceberg_tables
  - iceberg_namespace_properties
```

`CatalogStore` (see [`catalog_store.h`](catalog_store.h)) exposes typed row
operations such as `InsertTable`, `GetTableMetadataLocation`,
`UpdateTableMetadataLocation(expected_current)`, namespace-property CRUD, and
`RunInTransaction`. It exposes no SQL strings or driver-specific types.

The project provides built-in `CatalogStore` implementations for SQLite,
PostgreSQL, and MySQL. They are implemented with
[sqlpp23](https://github.com/rbock/sqlpp23), and the shared query code lives in
`catalog_store_sqlpp23_internal.h`. Users can also provide their own
`CatalogStore` implementation for another database, driver, or connection pool.

sqlpp23 is a build-time-only dependency for the built-in stores. It is compiled
into the connector translation units and does not appear in the installed
interface, so downstream consumers only need the native client libraries.

> The built-in sqlpp23 connectors require CMake >= 3.28 and C++23; sqlpp23 is
> fetched automatically via `FetchContent` when at least one built-in connector
> is enabled. A SQL catalog backed only by a user-supplied `CatalogStore` does not
> need sqlpp23. The SQL catalog is currently wired into the CMake build only;
> the Meson build does not build or install it yet.

## Out-of-the-box usage

Enable the SQL catalog and any built-in connectors at configure time. Built-in
connectors pull in their native client libraries via sqlpp23:

| CMake option                | Default | sqlpp23 target        | Native dependency      |
|-----------------------------|---------|-----------------------|------------------------|
| `ICEBERG_BUILD_SQL_CATALOG` | `OFF`   | -                     | -                      |
| `ICEBERG_SQL_SQLITE`        | `OFF`   | `sqlpp23::sqlite3`    | SQLite3                |
| `ICEBERG_SQL_POSTGRESQL`    | `OFF`   | `sqlpp23::postgresql` | libpq (PostgreSQL)     |
| `ICEBERG_SQL_MYSQL`         | `OFF`   | `sqlpp23::mysql`      | libmysqlclient (MySQL) |

```bash
cmake -S . -B build -DICEBERG_BUILD_SQL_CATALOG=ON -DICEBERG_SQL_SQLITE=ON
```

```cpp
#include "iceberg/catalog/sql/sql_catalog.h"

using iceberg::sql::SqlCatalog;
using iceberg::sql::SqlCatalogConfig;

SqlCatalogConfig config{
    .name = "prod",
    .uri = "/var/lib/iceberg/catalog.db",   // SQLite file path
    .warehouse_location = "s3://my-bucket/warehouse",
};

auto catalog = SqlCatalog::MakeSqliteCatalog(config, file_io).value();
// catalog->CreateNamespace(...), CreateTable(...), LoadTable(...), ...
```

`MakePostgreSqlCatalog` and `MakeMySqlCatalog` use the same
`[scheme://][user[:password]@]host[:port][/database]` URI form. Connector
factories are always declared in the public headers; if a connector was not
built, its factory returns `ErrorKind::kNotSupported`. Each enabled factory
creates the schema if it does not yet exist.

The PostgreSQL and MySQL stores use a single sqlpp23 connection when
`max_connections <= 1` and a bounded sqlpp23 connection pool otherwise.
Transaction bodies reuse the same leased connection for every store operation
issued inside `RunInTransaction`. The SQLite store ignores `max_connections` and
always uses a single connection: a file database only allows one writer (a pool
of write connections would just hit `SQLITE_BUSY`) and a `:memory:` database is
private to each connection.

The backing schema follows the Java/Rust-compatible `iceberg_tables` layout,
including the optional `iceberg_type` column. New table rows write
`iceberg_type = 'TABLE'` as the record type; existing rows with `NULL` remain
readable for compatibility.

## Bring your own store

To use a database, driver, or connection pool that is not built in, implement
`CatalogStore` and inject it. No catalog code changes are required:

```cpp
class MyCatalogStore : public iceberg::sql::CatalogStore {
 public:
  iceberg::Status Initialize() override { /* CREATE TABLE IF NOT EXISTS ... */ }
  iceberg::Result<std::optional<std::string>> GetTableMetadataLocation(
      std::string_view ns, std::string_view name) override { /* ... */ }
  iceberg::Status InsertTable(std::string_view ns, std::string_view name,
                              std::string_view metadata_location) override { /* ... */ }
  // ... the remaining CatalogStore operations ...
  iceberg::Status RunInTransaction(
      const std::function<iceberg::Status()>& body) override { /* ... */ }
};

auto store = std::make_shared<MyCatalogStore>(/* ... */);
auto catalog = SqlCatalog::Make(config, file_io, std::move(store)).value();
```

### Implementation contract

- **Catalog scope**: a store instance is bound to one catalog name; every row it
  reads or writes must be scoped by that name.
- **Namespace identifiers**: namespace levels must not be empty and must not
  contain `.` because the backing schema stores a namespace as a dot-joined
  string.
- **Table rows**: new table inserts should write `iceberg_type = 'TABLE'` as
  the record type.
  Reads should treat both `TABLE` and `NULL` as table rows so older databases
  remain readable.
- **Unique violations**: `InsertTable`, `InsertNamespaceProperty`, and
  `RenameTable` must report a primary-key collision as `ErrorKind::kAlreadyExists`.
  The catalog relies on this as the authoritative signal for concurrent creates.
- **Affected rows**: `UpdateTableMetadataLocation` performs the optimistic
  compare-and-set; it must return the number of rows updated (0 on a stale base).
- **Atomicity**: `RunInTransaction` must commit on success and roll back on any
  error so the database is left unchanged.
- **Threading**: a store may be called from multiple threads; serialize
  internally or use one connection per concurrent operation. The built-in
  PostgreSQL and MySQL stores use a bounded sqlpp23 connection pool when
  `max_connections > 1`; the SQLite store always uses a single connection.
