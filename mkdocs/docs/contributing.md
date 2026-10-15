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

We welcome contributions to Apache Iceberg C++. For general Iceberg contribution guidelines, see the [official guide](https://iceberg.apache.org/contribute/). Contributors using AI-assisted tools must follow the [AI-assisted contribution guidelines](https://iceberg.apache.org/contribute/#guidelines-for-ai-assisted-contributions).

For build and installation instructions, see [Getting Started](getting-started.md).

## Coding Standard

The project follows the same coding standard as [Apache Arrow](https://arrow.apache.org/docs/developers/cpp/development.html#code-style-linting-and-ci) (a variant of the Google’s C++ Style Guide)

### Naming Conventions

| Element | Style | Examples |
|---------|-------|----------|
| Classes / Structs | `PascalCase` | `TableScanBuilder`, `PartitionSpec` |
| Factory methods | `PascalCase` | `CreateNamespace()`, `ExtractYear()` |
| Accessors / Getters | `snake_case` | `name()`, `type_id()`, `partition_spec()` |
| Variables | `snake_case` | `file_io`, `schema_id` |
| Constants | `k` + `PascalCase` | `kHeaderContentType`, `kMaxPrecision` |

### General Practices

- Prefer smart pointers (`std::unique_ptr`, `std::shared_ptr`) for memory management
- Use `Result<T>` for error propagation
- Write Doxygen-style comments (`/// \brief ...`) for all public APIs
- Do not remove public methods without a deprecation cycle:

```cpp
[[deprecated("Use new_method() instead. Will be removed in a future release.")]]
void old_method();
```

## Development Environment

### Code Formatting

Formatting is enforced via `.clang-format` (Google base, `ColumnLimit: 90`). Set up `pre-commit` to run it automatically:

```bash
pip install pre-commit
pre-commit install
```

To run all hooks manually on the entire codebase:

```bash
pre-commit run -a
```

### Public Header Compatibility

Public headers must compile as C++20; the library itself builds as C++23. After
adding, moving, or removing a public header, regenerate the public-header list:

```bash
python ci/scripts/generate_public_headers.py
```

The script updates `src/iceberg/cpp20_compatibility_internal.h` and exits nonzero,
so include the generated change and rerun `pre-commit`. The
`generate-public-headers` hook performs the same step automatically. When adding a
module or install directory, also update the component mapping in
`ci/scripts/generate_public_headers.py`.

### Dev Containers

We provide Dev Container templates for VS Code:

```bash
cd .devcontainer
cp Dockerfile.template Dockerfile
cp devcontainer.json.template devcontainer.json
```

Then select `Dev Containers: Reopen in Container` from the Command Palette.

## Submitting Changes

### Workflow

1. Fork the repository on GitHub
2. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```
3. Make your changes following the coding standards
4. Add or update tests for any behavioral changes
5. Ensure all tests pass and `pre-commit run -a` is clean
6. Push to your fork and open a Pull Request

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add support for S3 file system
fix: resolve memory leak in table reader
docs: update API documentation
test: add unit tests for schema validation
refactor(rest): simplify auth token handling
```

### Pull Request Checklist

- Clear problem/solution description
- Linked issue(s) when applicable
- Tests for behavioral changes
- Passing CI checks (tests, pre-commit, license header, sanitizers)

## Getting Help

- **GitHub Issues** — [Report bugs or request features](https://github.com/apache/iceberg-cpp/issues/new)
- **Good First Issues** — [Browse here](https://github.com/apache/iceberg-cpp/labels/good%20first%20issue)
- **Mailing List** — [dev@iceberg.apache.org](mailto:dev@iceberg.apache.org) ([subscribe](mailto:dev-subscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20subscribe)) / [unsubscribe](mailto:dev-unsubscribe@iceberg.apache.org?subject=(send%20this%20email%20to%20unsubscribe)) / [archives](https://lists.apache.org/list.html?dev@iceberg.apache.org))
- **Slack** — [#cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A)

The Apache Iceberg community follows the [Apache Way](https://www.apache.org/theapacheway/index.html) and the Apache Foundation [Code of Conduct](https://www.apache.org/foundation/policies/conduct.html).
