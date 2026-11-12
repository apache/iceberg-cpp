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

Variant values use the `arrow.parquet.variant` extension with binary `metadata`
and `value` storage. Call `iceberg::arrow::RegisterAll()` before importing these
schemas into Arrow C++. Parquet readers and writers also register the extension
when opened.

Parquet variant I/O requires the Arrow variant builders, validation, and shredding
utilities introduced by [Arrow PR #50252](https://github.com/apache/arrow/pull/50252).
The bundled dependency temporarily pins Arrow revision
`0f16412bb1c39611aeed7393eba09e637f4d86fa` from that PR and verifies the source
archive's SHA256. It will be replaced by an Apache Arrow release containing these
APIs. Use `ICEBERG_USE_BUNDLED_ARROW=ON` to force this Arrow source, or provide a
compatible Arrow installation through `CMAKE_PREFIX_PATH`.

`ICEBERG_REQUIRE_ARROW_VARIANT` defaults to `ON`: configuration fails when the
selected Arrow/Parquet lacks the required APIs. CI explicitly uses the pinned
Arrow source and keeps this option enabled; the variant tests fail to compile
if their APIs are missing. To use an older Arrow installation for other
functionality, explicitly set this option
to `OFF`; Parquet variant I/O then returns `NotSupported`.

Variants are written unshredded by default. To shred a field, supply a logical
target keyed by its Iceberg field ID:

```cpp
iceberg::WriterOptions options;
// Set the path, schema, and FileIO as usual.
options.variant_shredding_types.emplace(7, iceberg::int64());
```

Targets can also contain structs and lists. Their field IDs are ignored; they
describe the requested layout inside the variant. The layout stays fixed for
the file, and incompatible values remain in residual storage. Input batches
always use unshredded variants. Readers reconstruct the same unshredded schema
from either file layout. Validation stays enabled, and a null column value is
distinct from an encoded variant null. Avro variant data and variant bounds
metrics are not supported.

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
