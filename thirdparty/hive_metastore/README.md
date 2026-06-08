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

# Vendored Hive Metastore Thrift IDL

This directory contains vendored Thrift interface definitions used to generate
the C++ Hive Metastore (HMS) client consumed by the `iceberg_hive` library. The
files are copied verbatim from upstream Apache projects and remain under the
Apache License, Version 2.0 (see `LICENSE` in the repository root). The only
modification is the repository's pre-commit normalization (trailing whitespace
stripped, single final newline); each file retains its original upstream license
header in place.

## Vendored sources

Sources are pinned to immutable upstream tags and commit SHAs so the provenance
is reproducible and future updates are deterministic.

| File | Upstream | Tag | Commit |
| --- | --- | --- | --- |
| `hive_metastore.thrift` | [apache/hive](https://github.com/apache/hive) — `standalone-metastore/src/main/thrift/hive_metastore.thrift` | `rel/release-3.1.3` | [`04c1b307d1bbd1ae268ad47dc36ca4f50c6d9cd8`](https://github.com/apache/hive/blob/04c1b307d1bbd1ae268ad47dc36ca4f50c6d9cd8/standalone-metastore/src/main/thrift/hive_metastore.thrift) |
| `share/fb303/if/fb303.thrift` | [apache/thrift](https://github.com/apache/thrift) — `contrib/fb303/if/fb303.thrift` | `v0.14.0` | [`8411e189b0af09e5baad34031555870cf692c1ad`](https://github.com/apache/thrift/blob/8411e189b0af09e5baad34031555870cf692c1ad/contrib/fb303/if/fb303.thrift) |

`fb303.thrift` is the Facebook fb303 management service IDL, transitively
included by `hive_metastore.thrift`. It was originally developed at Facebook,
Inc. and contributed to the Apache Software Foundation under the Apache License,
Version 2.0.

## Updating

Keep a single vendored IDL pinned to an immutable Hive release tag or commit.
To update, re-fetch the file at the new upstream ref, refresh the table above
with the new tag and commit SHA, and re-run the pre-commit hooks.
