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

### Build and Install Core Libraries

```bash
cd iceberg-cpp
mkdir build && cd build
cmake .. -DCMAKE_INSTALL_PREFIX=/tmp/iceberg -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build .
cmake --install .
```

### Build Examples

After installing the core libraries, you can build the examples:

```bash
cd iceberg-cpp/example
mkdir build && cd build
cmake .. -DCMAKE_PREFIX_PATH=/tmp/iceberg
cmake --build .
```

## Contribute

Apache Iceberg is an active open-source project, governed under the Apache Software Foundation (ASF). Iceberg-cpp is open to people who want to contribute to it. Here are some ways to get involved:

- Submit [Issues](https://github.com/apache/iceberg-cpp/issues/new) for bug report or feature requests.
- Discuss at [dev mailing list](mailto:dev@iceberg.apache.org) ([subscribe](<mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)>) / [unsubscribe](<mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)>) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- Talk to the community directly at [Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A).

The Apache Iceberg community is built on the principles described in the [Apache Way](https://www.apache.org/theapacheway/index.html) and all who engage with the community are expected to be respectful, open, come with the best interests of the community in mind, and abide by the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
