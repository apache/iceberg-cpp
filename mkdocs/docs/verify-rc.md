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

# Verify a Release Candidate

When a release candidate (RC) is published for a vote, community members are encouraged to verify it before casting their vote.

## Prerequisites

- `curl`
- `gpg`
- `shasum` or `sha512sum`
- `tar`
- CMake 3.25+
- C++23 compliant compiler (GCC 14+, Clang 18+, MSVC 2022+)

## Verification Steps

We provide a script that automates the entire verification process:

```bash
dev/release/verify_rc.sh ${VERSION} ${RC}
```

For example, to verify RC0 of version 0.3.0:

```bash
dev/release/verify_rc.sh 0.3.0 0
```

The script performs the following checks:

1. Downloads the source tarball from ASF's dev distribution
2. Imports the [KEYS file](https://downloads.apache.org/iceberg/KEYS) and verifies the GPG signature
3. Verifies the SHA-512 checksum
4. Extracts the source and builds with CMake
5. Runs the full test suite

If everything passes, you will see:

```
RC looks good!
```

## Manual Verification

If you prefer to verify manually:

### 1. Download the RC

```bash
VERSION=0.3.0
RC=0
BASE_URL="https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${VERSION}-rc${RC}"

curl -O "${BASE_URL}/apache-iceberg-cpp-${VERSION}.tar.gz"
curl -O "${BASE_URL}/apache-iceberg-cpp-${VERSION}.tar.gz.asc"
curl -O "${BASE_URL}/apache-iceberg-cpp-${VERSION}.tar.gz.sha512"
```

### 2. Verify Signature and Checksum

```bash
# Import KEYS
curl https://downloads.apache.org/iceberg/KEYS | gpg --import

# Verify GPG signature
gpg --verify apache-iceberg-cpp-${VERSION}.tar.gz.asc apache-iceberg-cpp-${VERSION}.tar.gz

# Verify SHA-512 checksum
shasum -a 512 -c apache-iceberg-cpp-${VERSION}.tar.gz.sha512
```

### 3. Build and Test

```bash
tar xf apache-iceberg-cpp-${VERSION}.tar.gz
cd apache-iceberg-cpp-${VERSION}

cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
```
