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

# Releases

Apache Iceberg C++ releases are available for download from our GitHub releases page.

## Latest Release

### Apache Iceberg C++ 0.1.0

**Released:** September 15, 2024

**Download:** [v0.1.0](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0)

#### What's New

This is the first official release of Apache Iceberg C++. This release includes:

- **Core Libraries**: Basic CMake support and iceberg library structure
- **Data Types**: Support for primitive types (int, long, string, boolean, etc.)
- **Schema Management**: Schema field definitions and schema conversion
- **Table Metadata**: Table metadata reading and writing capabilities
- **File I/O**: Local file system support using Arrow FileSystem
- **Avro Support**: Avro file reading and schema conversion
- **Arrow Integration**: Arrow C Data Interface and schema conversion
- **Partitioning**: Partition field and partition spec support
- **Sorting**: Sort order and sort field definitions
- **Expressions**: Basic expression support and literal types
- **Catalog**: In-memory catalog implementation
- **Table Scanning**: Basic table scan planning
- **Testing**: Comprehensive test suite with GoogleTest
- **Documentation**: API documentation generation with Doxygen

#### Key Features

- **C++23 Support**: Modern C++ features and standards
- **Cross-Platform**: Support for Linux, macOS, and Windows
- **Arrow Integration**: Seamless integration with Apache Arrow
- **Avro Compatibility**: Full Avro file format support
- **Memory Safety**: Smart pointer usage and RAII patterns
- **Error Handling**: Comprehensive error handling with Result types
- **Performance**: Optimized for high-performance data processing

#### Installation

Download the release from GitHub and follow the installation instructions in our [Contributing guide](index.md).

#### Breaking Changes

This is the first release, so there are no breaking changes from previous versions.

## All Releases

For a complete list of all releases, including release notes and download links, visit our [GitHub Releases page](https://github.com/apache/iceberg-cpp/releases).

### Release History

| Version | Release Date | Status | Download |
|---------|-------------|--------|----------|
| [v0.1.0](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0) | September 15, 2024 | Latest | [Download](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0) |

## Release Process

Apache Iceberg C++ follows the Apache Software Foundation release process:

1. **Release Candidates**: Pre-release versions for testing
2. **Community Review**: Community feedback and testing period
3. **Vote**: Apache PMC vote on release approval
4. **Release**: Official release announcement and distribution

## Download Options

### Source Code

Download the source code from our GitHub releases page:

```bash
# Download latest release
wget https://github.com/apache/iceberg-cpp/archive/refs/tags/v0.1.0.tar.gz

# Extract and build
tar -xzf v0.1.0.tar.gz
cd iceberg-cpp-0.1.0
```

### Pre-built Binaries

Pre-built binaries are available for select platforms. Check the [GitHub Releases page](https://github.com/apache/iceberg-cpp/releases) for available binary distributions.

### Package Managers

We are working on package manager support. Stay tuned for updates on:

- Conan package manager
- vcpkg package manager
- Homebrew (macOS)
- APT/YUM (Linux distributions)

## Verification

All releases are cryptographically signed. Verify the integrity of downloads using the provided checksums and signatures.

### Checksums

Release checksums are available in the release assets on GitHub.

### GPG Signatures

Releases are signed with the Apache Iceberg project GPG key. Verify signatures using:

```bash
# Import the Apache Iceberg GPG key
gpg --keyserver keyserver.ubuntu.com --recv-keys B5690EEEBB952194

# Verify the signature
gpg --verify iceberg-cpp-0.1.0.tar.gz.asc iceberg-cpp-0.1.0.tar.gz
```

## Support

For questions about releases or to report issues:

- **GitHub Issues**: [Report bugs or request features](https://github.com/apache/iceberg-cpp/issues)
- **Mailing List**: [dev@iceberg.apache.org](mailto:dev@iceberg.apache.org)
- **Slack**: [Apache Iceberg Slack #cpp channel](https://join.slack.com/t/apache-iceberg/shared_invite/zt-1zbov3k6e-KtJfoaxp97YfX6dPz1Bk7A)

## License

All releases are licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
