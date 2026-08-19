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
| `arrow-fs-s3` | `s3`, `s3a`, `s3n`, `oss` |

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

## Configure S3

| Key | Example | Description |
|---|---|---|
| `s3.access-key-id` | `admin` | Static access key ID; must be set together with the secret key |
| `s3.secret-access-key` | `password` | Static secret access key |
| `s3.session-token` | `AQoDYXdzEJr...` | Session token, for temporary credentials. Ignored unless both static keys are set |
| `client.region` | `us-east-1` | Region to sign requests for |
| `s3.endpoint` | `https://127.0.0.1:9000` | Endpoint to use instead of the AWS one |
| `s3.path-style-access` | `true` | Address buckets as a path (`endpoint/bucket`) instead of a virtual host (`bucket.endpoint`). Only takes effect together with `s3.endpoint` |
| `s3.ssl.enabled` | `true` | Scheme to use for the endpoint, overriding the one it carries |
| `s3.connect-timeout-ms` | `1000` | Connection timeout |
| `s3.socket-timeout-ms` | `5000` | Request timeout. Ignored outside Windows and macOS |

Without credentials, the AWS default credential chain is used, which covers
environment variables, the shared configuration file, and the various role and
identity providers.

### S3-compatible storage

Stores that speak the S3 API are served by the same implementation. The scheme
selects it; `s3.endpoint` decides where requests actually go. A location keeps
its own scheme and is canonicalized internally, so a credential vended for the
`s3` prefix applies to it.

For Alibaba Cloud OSS, point `s3.endpoint` at the S3-compatible endpoint of the
bucket's region and set `s3.path-style-access` to `false`: with a custom
endpoint, buckets are addressed as a path unless told otherwise, and the
service rejects that with
`SecondLevelDomainForbidden: Please use virtual hosted style to access`:

```cpp
auto file_io = iceberg::FileIORegistry::Load(
    iceberg::FileIORegistry::kArrowS3FileIO,
    {{std::string(iceberg::arrow::S3Properties::kEndpoint),
      "https://s3.oss-cn-hangzhou.aliyuncs.com"},
     {std::string(iceberg::arrow::S3Properties::kClientRegion), "cn-hangzhou"},
     {std::string(iceberg::arrow::S3Properties::kPathStyleAccess), "false"}});

file_io.value()->NewInputFile("oss://bucket/path/to/file.parquet");
```

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
