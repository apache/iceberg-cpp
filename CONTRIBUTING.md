# Contributing to iceberg-cpp

This document describes how to set up a development environment, run tests, and use existing tooling such as Docker and pre-commit for iceberg-cpp. [][]

## Development environment

The project can be built and developed using the existing Docker-based environment. Please refer to the README and Doxygen documentation for more detailed API and usage information. [][][]

### Local prerequisites

- Git
- CMake and a C++17-compatible compiler
- Python (for pre-commit)
- Docker (recommended for a consistent dev environment) [][]

## Building and testing

Typical steps (refer to README for exact commands and options): [][]

```bash
mkdir build
cd build
cmake ..
make -j$(nproc)
ctest
