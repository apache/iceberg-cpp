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

# Library Design

Iceberg C++ splits its code into a few libraries so applications can link only
what they need.

## Library boundaries

### `iceberg`

The core library contains schemas, table metadata, manifests, snapshots, table
updates, catalog interfaces, Puffin files, etc. It also owns the Arrow C Data
interface and the nanoarrow-based utilities used by core metadata and data
processing code. These utilities do not depend on the Arrow C++ library.

### `iceberg_data`

This library handles data-plane processing, including delete filtering, data
writers, and scan task readers. Concrete Avro and Parquet readers and writers
are supplied by the `iceberg_bundle` library.

### `iceberg_bundle`

This library combines `iceberg_data` with the Arrow C++, Avro, Parquet, and
Arrow filesystem integrations. It gives applications one link-time dependency.

### `iceberg_rest`, `iceberg_hive`, `iceberg_sql_catalog`

These optional catalog libraries cover REST, Hive Metastore, and SQL databases.
Each depends on `iceberg` and its own client libraries, not on `iceberg_data` or
`iceberg_bundle`.

## Source and header ownership

Production files are kept in the directory for the library that builds them:

- Core files stay in the `iceberg` root or core subdirectories such as
  `deletes/`, `puffin/`, `manifest/`, and `update/`.
- Files under `data/` are built into `iceberg_data`.
- Files under `arrow/`, `avro/`, and `parquet/` are built into
  `iceberg_bundle`.
- Optional catalog implementations stay under `catalog/rest/`, `catalog/hive/`,
  and `catalog/sql/` and are built into their corresponding catalog libraries.

The Arrow C Data files in the `iceberg` root are core infrastructure and are
separate from the Arrow C++ integration under `arrow/`.

Public headers are installed with their existing `iceberg/...` include paths.
Headers whose names contain `internal` are not installed.

## Runtime registration

The core library defines registries for optional file readers, writers, and
filesystem implementations. Integrations register their implementations as
follows:

- Arrow local and S3 `FileIO` implementations are registered when the Arrow
  integration is loaded. `iceberg::arrow::RegisterAll()` additionally registers
  Arrow-based batch projection.
- Avro readers, writers, and logical types are registered by
  `iceberg::avro::RegisterAll()`.
- Parquet readers and writers are registered by
  `iceberg::parquet::RegisterAll()`.

## Manifest content caching

Tables can cache immutable manifest and manifest-list bytes. Per-table settings come
exclusively from `FileIO::properties()`; table metadata properties do not enable
or override the cache. REST catalogs supply their merged catalog and table I/O
configuration through the table's `FileIO`. Applications that inject a `FileIO`
must expose the desired settings through its `properties()` method.

| Property | Default |
|---|---|
| `io.manifest.cache-enabled` | `false` |
| `io.manifest.cache.max-total-bytes` | `104857600` (100 MiB) |
| `io.manifest.cache.max-content-length` | `8388608` (8 MiB) |

Independent table loads have separate caches and byte budgets. A table and its
manifest I/O adapters share ownership of its cache. The process-wide registry
tracks the cache through a weak reference once it stores its first file.
Removing the last cached file or destroying the last cache handle frees its slot.

By default, at most eight caches retain files at once. Call
`Table::SetManifestCacheLimit(max_caches)` with a positive integer to change the
limit. Lowering it immediately evicts the least recently used caches, even if
their tables are still loaded. The next cacheable insertion repopulates an
evicted cache. Active readers and input files can still hold bytes after
eviction, and concurrent load buffers are outside the retained byte budget.
Call `Table::ClearManifestCache()` to release a table's retained content explicitly.
Manifest cleanup invalidates cached aliases of each deleted file. If storage
cannot identify the deleted file across aliases, cleanup clears that table's cache.

Compatible refreshes and committed table handles share their source table's cache.
S3 access keys, secrets, session tokens, and token expiry can rotate without losing
warm content when the remaining storage configuration matches. Changes to cache
limits or storage configuration, such as an endpoint or region, start a new cache.
Data files and table metadata JSON continue to use storage I/O directly.
