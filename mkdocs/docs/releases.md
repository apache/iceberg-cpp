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

# Release History

| Version | Date | Links |
|---------|------|-------|
| 0.4.0 | September 26, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.4.0) · [Source](https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-0.4.0/) |
| 0.3.0 | June 14, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.3.0) · [Source](https://archive.apache.org/dist/iceberg/apache-iceberg-cpp-0.3.0/) · [Blog Post](https://iceberg.apache.org/blog/apache-iceberg-cpp-0.3.0-release/) |
| 0.2.0 | January 26, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.2.0) · [Source](https://archive.apache.org/dist/iceberg/apache-iceberg-cpp-0.2.0/) · [Blog Post](https://iceberg.apache.org/blog/apache-iceberg-cpp-0.2.0-release/) |
| 0.1.0 | September 10, 2025 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0) · [Source](https://archive.apache.org/dist/iceberg/apache-iceberg-cpp-0.1.0/) |

For the full changelog of each release, see the [GitHub Releases page](https://github.com/apache/iceberg-cpp/releases).

## 0.4.0

- New snapshot update operations including merge append, row delta, overwrite files, delete files, rewrite files, and replace partitions
- Initial v3 support including column default values, row lineage, deletion vectors, and geometry and geography types
- Parallel manifest reading and writing, parallel update and scan processing, and lazy streaming scan planning
- REST catalog improvements including session-aware catalogs, OAuth2 token exchange, SigV4 authentication, and vended storage credentials
- ResolvingFileIO to select a FileIO by location scheme, with S3-compatible and OSS schemes in the Arrow FileIO
- Commit and scan metrics reporting integration
- Pluggable logging framework with std::cerr and spdlog backends
- Metadata table interface with a streaming snapshots table
- Groundwork for a Hive Metastore catalog with vendored Thrift bindings and an HMS client
- RenameTable for InMemoryCatalog and purge support in DropTable for InMemoryCatalog and SqlCatalog
- Upgrade to Arrow 25.0.0 and removal of Meson build support

## 0.3.0

- Extend table scan planning with v2 delete support, manifest filtering, and projection
- Incremental scan planning with append-only and basic changelog scan support
- REST catalog improvements including initial OAuth2 support with auto-refresh, basic authentication, snapshot loading mode, namespace separators, and server-side scan planning
- New table metadata update including partition statistics, schema update, and expire snapshots with file cleanup support
- Transaction with retry, and scaffolding work on MergingSnapshotUpdate for update, delete, overwrite, etc.
- SQL catalog support backed by SQLite, PostgreSQL, and MySQL stores
- File scan task reader with v2 deletes support
- V2 data writer support and writer metrics collection
- Groundwork for scan and commit metrics reporting
- Puffin metadata and reader/writer support
- Initial v3 support with the unknown and nanosecond timestamp types
- FileIO enrichment including new InputFile and OutputFile interfaces, bulk delete and S3 integration

## 0.2.0

- Table scan planning with V2 delete and filtering support
- Append table support
- Schema evolution and table metadata update operations
- Transaction API with snapshot management
- REST catalog client with namespace and table CRUD
- Expression system with metrics and residual evaluators
- Meson build system support

## 0.1.0

- Core data types, schema, and table metadata (JSON serde)
- Partition specs, sort orders, and snapshot management
- Basic table scan planning (w/o deletes)
- Avro and Parquet file format support
- Local file I/O via Arrow FileSystem
- In-memory catalog
