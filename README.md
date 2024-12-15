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

# Apache Iceberg™ C++

C++ implementation of [Apache Iceberg™](https://iceberg.apache.org/).

## Requirements

- CMake 3.25 or higher
- C++20 compliant compiler

## Build

### Build, Run Test and Install Core Libraries

```bash
cd iceberg-cpp
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build and Install Iceberg Arrow Library

#### Vendored Apache Arrow (default)

```bash
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_ARROW=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

#### Provided Apache Arrow

```bash
cmake -S . -B build -DCMAKE_INSTALL_PREFIX=/path/to/install -DCMAKE_PREFIX_PATH=/path/to/arrow -DICEBERG_ARROW=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build Examples

After installing the core libraries, you can build the examples:

```bash
cd iceberg-cpp/example
cmake -S . -B build -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build
```

If you are using provided Apache Arrow, you need to include `/path/to/arrow` in `CMAKE_PREFIX_PATH` as below.

```bash
cmake -S . -B build -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/arrow"
```

## Contribute

Apache Iceberg is an active open-source project, governed under the Apache Software Foundation (ASF). Iceberg-cpp is open to people who want to contribute to it. Here are some ways to get involved:

- Submit [Issues](https://github.com/apache/iceberg-cpp/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](<mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)>) / [unsubscribe](<mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)>) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- Talk to the community directly at [Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A).

The Apache Iceberg community is built on the principles described in the [Apache Way](https://www.apache.org/theapacheway/index.html) and all who engage with the community are expected to be respectful, open, come with the best interests of the community in mind, and abide by the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

### Linting

Install the python package `pre-commit` and run once `pre-commit install`.

```
pip install pre-commit
pre-commit install
```

This will setup a git pre-commit-hook that is executed on each commit and will report the linting problems. To run all hooks on all files use `pre-commit run -a`.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
