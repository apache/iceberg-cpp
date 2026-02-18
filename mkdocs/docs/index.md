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

# Contributing

We welcome contributions to Apache Iceberg! To learn more about contributing to Apache Iceberg, please refer to the official Iceberg contribution guidelines. These guidelines are intended as helpful suggestions to make the contribution process as seamless as possible, and are not strict rules.

If you would like to discuss your proposed change before contributing, we encourage you to visit our Community page. There, you will find various ways to connect with the community, including Slack and our mailing lists. Alternatively, you can open a new issue directly in the GitHub repository.

For first-time contributors, feel free to check out our good first issues for an easy way to get started.

In addition, contributors using AI-assisted tools must follow the documented guidelines for AI-assisted contributions available on the Iceberg website: [https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions](https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions).

## Contributing to Iceberg C++

The Iceberg C++ Project is hosted on GitHub at [https://github.com/apache/iceberg-cpp](https://github.com/apache/iceberg-cpp).

### Development Setup

#### Prerequisites

- CMake 3.25 or higher
- C++23 compliant compiler (GCC 14+, Clang 16+, MSVC 2022+)
- Git

#### Building from Source

Clone the repository for local development:

```bash
git clone https://github.com/apache/iceberg-cpp.git
cd iceberg-cpp
```

Build the core libraries:

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

Build with bundled dependencies:

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Code Standards

#### C++ Coding Standards

We follow modern C++ best practices:

- **C++23 Standard**: Use C++23 features where appropriate
- **Naming Conventions**:
  - Classes: `PascalCase` (e.g., `TableScanBuilder`)
  - Functions/Methods: `PascalCase` (e.g., `CreateNamespace`, `ExtractYear`)
  - Trivial getters: `snake_case` (e.g., `name()`, `type_id()`, `is_primitive()`)
  - Variables: `snake_case` (e.g., `file_io`)
  - Constants: `k` prefix with `PascalCase` (e.g., `kHeaderContentType`, `kMaxPrecision`)
- **Memory Management**: Prefer smart pointers (`std::unique_ptr`, `std::shared_ptr`)
- **Error Handling**: Use `Result<T>` types for error propagation
- **Documentation**: Use Doxygen-style comments for public APIs

#### API Compatibility

It is important to keep the C++ public API compatible across versions. Public methods should have no leading underscores and should not be removed without deprecation notice.

If you want to remove a method, please add a deprecation notice:

```cpp
[[deprecated("This method will be removed in version 2.0.0. Use new_method() instead.")]]
void old_method();
```

#### Code Formatting

We use `clang-format` for code formatting. The configuration is defined in `.clang-format` file.

Format your code before submitting:

```bash
clang-format -i src/**/*.{h,cc}
```

### Testing

#### Running Tests

Run all tests:

```bash
ctest --test-dir build --output-on-failure
```

Run specific test:

```bash
ctest --test-dir build -R test_name
```

### Linting

Install the python package `pre-commit` and run once `pre-commit install`:

```bash
pip install pre-commit
pre-commit install
```

This will setup a git pre-commit-hook that is executed on each commit and will report the linting problems. To run all hooks on all files use `pre-commit run -a`.

### Submitting Changes

#### Git Workflow

1. **Fork the repository** on GitHub
2. **Create a feature branch** from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes** following the coding standards
4. **Add tests** for your changes
5. **Run tests** to ensure everything passes
6. **Commit your changes** with a clear commit message
7. **Push to your fork** and create a Pull Request

#### Commit Message Format

Use clear, descriptive commit messages:

```
feat: add support for S3 file system
fix: resolve memory leak in table reader
docs: update API documentation
test: add unit tests for schema validation
```

#### Pull Request Process

1. **Create a Pull Request** with a clear description
2. **Link related issues** if applicable
3. **Ensure CI passes** - all tests must pass
4. **Request review** from maintainers
5. **Address feedback** and update the PR as needed
6. **Squash commits** if requested by reviewers

### Community

The Apache Iceberg community is built on the principles described in the [Apache Way](https://www.apache.org/theapacheway/index.html) and all who engage with the community are expected to be respectful, open, come with the best interests of the community in mind, and abide by the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).

#### Getting Help

- **Submit Issues**: [GitHub Issues](https://github.com/apache/iceberg-cpp/issues/new) for bug reports or feature requests
- **Mailing List**: [dev@iceberg.apache.org](mailto:dev@iceberg.apache.org) for discussions
  - [Subscribe](mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe))
  - [Unsubscribe](mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe))
  - [Archives](https://lists.apache.org/list.html?dev@iceberg.apache.org)
- **Slack**: [Apache Iceberg Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A)

#### Good First Issues

New to the project? Check out our [good first issues](https://github.com/apache/iceberg-cpp/labels/good%20first%20issue) for an easy way to get started.

### Release Process

Releases are managed by the Apache Iceberg project maintainers. For information about the release process, please refer to the main Iceberg project documentation.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0)
