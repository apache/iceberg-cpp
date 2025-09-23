#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

echo "Building Iceberg C++ documentation..."
echo "Working directory: $(pwd)"

if ! command -v mkdocs &> /dev/null; then
    echo "Error: mkdocs is not installed"
    exit 1
fi

echo "Installing dependencies..."
pip install mkdocs-material

# Check if doxygen is available
if ! command -v doxygen &> /dev/null; then
    echo "Warning: doxygen is not installed, skipping API documentation generation"
else
    echo "Building API documentation with Doxygen..."
    cd mkdocs
    mkdir -p docs/api
    doxygen Doxyfile
    cd ..
fi

echo "Building MkDocs documentation..."
cd mkdocs
mkdocs build --clean

echo "Documentation build completed successfully!"
echo "MkDocs site: docs/site/"
