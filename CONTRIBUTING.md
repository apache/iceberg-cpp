Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

# Contributing to iceberg-cpp

This document describes how to set up a development environment, run tests, and use existing tooling such as Docker and pre-commit for iceberg-cpp.

## Development environment

The project can be built and developed using the existing Docker-based environment. Please refer to the README and Doxygen documentation for more detailed API and usage information.

### Local prerequisites

- Git
- CMake and a C++17-compatible compiler
- Python (for pre-commit)
- Docker (recommended for a consistent dev environment)

## Building and testing

Typical steps (refer to README for exact commands and options):

```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
ctest
