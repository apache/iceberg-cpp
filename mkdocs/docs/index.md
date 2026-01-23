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

## Contributing to Iceberg C++

The Iceberg C++ Project is hosted on GitHub at [https://github.com/apache/iceberg-cpp](https://github.com/apache/iceberg-cpp).

### Development Setup

#### Prerequisites

- CMake 3.25 or higher
- C++23 compliant compiler (GCC 14+, Clang 17+, MSVC 2022+)
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

### AI-Assisted Contributions

The Apache Iceberg C++ community has the following policy for AI-assisted PRs:

- The PR author should **understand the core ideas** behind the implementation **end-to-end**, and be able to justify the design and code during review.
- **Calls out unknowns and assumptions**. It's okay to not fully understand some bits of AI generated code. You should comment on these cases and point them out to reviewers so that they can use their knowledge of the codebase to clear up any concerns. For example, you might comment "calling this function here seems to work but I'm not familiar with how it works internally, I wonder if there's a race condition if it is called concurrently".

#### Why fully AI-generated PRs without understanding are not helpful

Today, AI tools cannot reliably make complex changes to the codebase on their own, which is why we rely on pull requests and code review.

The purposes of code review are:

1. Finish the intended task.
2. Share knowledge between authors and reviewers, as a long-term investment in the project. For this reason, even if someone familiar with the codebase can finish a task quickly, we're still happy to help a new contributor work on it even if it takes longer.

An AI dump for an issue doesn’t meet these purposes. Maintainers could finish the task faster by using AI directly, and the submitters gain little knowledge if they act only as a pass through AI proxy without understanding.

Please understand the reviewing capacity is **very limited** for the project, so large PRs which appear to not have the requisite understanding might not get reviewed, and eventually closed or redirected.

#### Better ways to contribute than an “AI dump”

It's recommended to write a high-quality issue with a clear problem statement and a minimal, reproducible example. This can make it easier for others to contribute.

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
