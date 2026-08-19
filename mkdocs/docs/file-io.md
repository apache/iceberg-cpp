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

# FileIO

`FileIO` reads, writes, and deletes Iceberg data and metadata files.

## Built-in implementations

Call `iceberg::arrow::RegisterAll()` to register the Arrow-backed FileIO
implementations:

| Registry name | Schemes |
|---|---|
| `arrow-fs-local` | paths without a scheme, `file` |
| `arrow-fs-s3` | `s3`, `s3a`, `s3n` |

The S3 implementation requires Arrow S3 support.

## Select an implementation

Load a registered implementation directly:

```cpp
#include "iceberg/arrow/arrow_register.h"
#include "iceberg/arrow/s3/s3_properties.h"
#include "iceberg/file_io_registry.h"
#include "iceberg/resolving_file_io.h"

iceberg::arrow::RegisterAll();

auto file_io = iceberg::FileIORegistry::Load(
    iceberg::FileIORegistry::kArrowS3FileIO,
    {{std::string(iceberg::arrow::S3Properties::kEndpoint),
      "https://s3.example.com"},
     {std::string(iceberg::arrow::S3Properties::kClientRegion), "us-east-1"}});
```

For a REST catalog, set `io-impl` to the registry name. If it is omitted, the
REST catalog uses `ResolvingFileIO` and selects a registered implementation for
each file location's scheme.

## Register a custom FileIO

Register the factory before creating the catalog or resolver:

```cpp
#include <string_view>

iceberg::FileIORegistry::Register(
    "my-file-io",
    {.create = [](const iceberg::FileIORegistry::Properties& properties)
                   -> iceberg::Result<std::unique_ptr<iceberg::FileIO>> {
       return MakeMyFileIO(properties);
     },
     .accepts = [](std::string_view scheme) { return scheme == "myfs"; }});
```

`create` is required. Set `accepts` to enable automatic selection; it receives
the normalized lower-case scheme. Leave it empty for an implementation selected
only by `io-impl`.

```cpp
iceberg::FileIORegistry::Load("my-file-io", {});
auto file_io = std::make_unique<iceberg::ResolvingFileIO>(
    iceberg::FileIORegistry::Properties{});
file_io->NewInputFile("myfs://bucket/path/file.parquet");
```

Registrations are process-wide and must be completed before creating catalogs
or resolvers. When multiple implementations accept the same scheme, the last
registration takes precedence. `ResolvingFileIO` lazily creates and reuses one
FileIO instance per registry name.

## Storage credentials

When a REST catalog returns vended storage credentials for a table, it applies
them to the table's FileIO. A custom FileIO selected through `io-impl` must
implement `SupportsStorageCredentials`; otherwise table access with vended
credentials is unsupported. With automatic resolution, `ResolvingFileIO`
forwards credentials to registered delegates that support them.
