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
| 0.3.0 | June 14, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.3.0) · [Source](https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-0.3.0/) · [Blog Post](https://iceberg.apache.org/blog/apache-iceberg-cpp-0.3.0-release/) |
| 0.2.0 | January 26, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.2.0) · [Source](https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-0.2.0/) · [Blog Post](https://iceberg.apache.org/blog/apache-iceberg-cpp-0.2.0-release/) |
| 0.1.0 | September 10, 2025 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0) · [Source](https://archive.apache.org/dist/iceberg/apache-iceberg-cpp-0.1.0/) |

For the full changelog of each release, see the [GitHub Releases page](https://github.com/apache/iceberg-cpp/releases).

## 0.3.0

- Incremental scan APIs, incremental append scans, and incremental changelog scans for planning table changes between snapshots
- Merge-on-read data access with a MOR file scan task reader, delete filter support, and a DeleteLoader for v2 position and equality delete files
- Column selection in table scan planning and ManifestGroup file filtering
- Roaring-based position bitmaps, a position delete index, and range coalescing for position deletes
- MergingSnapshotUpdate lays the groundwork for table overwrite, delete, update, and various maintenance operations.
- SnapshotManager support and retried transaction commits
- Snapshot expiration cleanup strategies for reachable file cleanup and incremental file cleanup
- Partition statistics updates and schema update mapping
- REST catalog improvements including initial OAuth2 support, OAuth2 token auto-refresh, basic authentication, snapshot loading mode, namespace separators, and server-side scan planning endpoints
- S3 FileIO integration built on Arrow filesystem support
- FileIO interface enrichment with new InputFile and OutputFile interfaces and bulk delete support
- SQL catalog support backed by SQLite, PostgreSQL, and MySQL stores
- Metrics reporter support with report JSON serialization and reporter loading
- Avro writer metrics and Parquet writer metrics
- Puffin support with basic data structures, format constants and JSON serialization, and file reader/writer support
- Iceberg v3 support for the unknown type and nanosecond timestamp types
- Expression serialization with operation JSON serialization, expression JSON serialization, and typed literal binding after serialization

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
