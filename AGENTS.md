<!--
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
-->

# Apache Iceberg C++ — Agent Instructions

This file provides repository-specific guidance for automated agents working
in this repository.

## Security Model

When assessing potential vulnerabilities or calibrating automated security
findings, use [`SECURITY-THREAT-MODEL.md`](SECURITY-THREAT-MODEL.md) as the
authoritative detailed description of this repository's security boundaries,
trust assumptions, and non-boundaries.

## Build System Consistency

Keep CMake and Meson build definitions in sync. When adding, removing, or
renaming source files, headers, tests, or build targets, or changing dependencies
or build options, update the corresponding CMake (`CMakeLists.txt` and CMake
modules) and Meson (`meson.build` and `meson.options`) definitions in the same
change.

## PR & Commit Conventions

- Use Conventional Commits for commit messages.
- Commit messages describe the what and why, not implementation details.
- Run `pre-commit` for every PR and fix any reported issues.
