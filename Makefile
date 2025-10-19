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

.PHONY: help install-deps build-api-docs build-docs clean-docs serve-docs

help:
	@echo "Available targets:"
	@echo "  install-deps     - Install Python dependencies"
	@echo "  build-api-docs   - Build API documentation with Doxygen"
	@echo "  build-docs       - Build MkDocs documentation"
	@echo "  clean-docs       - Clean documentation build artifacts"
	@echo "  serve-docs       - Serve documentation locally for development"
	@echo "  all              - Build all documentation"

install-deps:
	python -m pip install --upgrade pip
	pip install -r requirements.txt

build-api-docs:
	cd mkdocs && \
	mkdir -p docs/api && \
	doxygen Doxyfile && \
	echo "Doxygen output created in docs/api/"

build-docs:
	cd mkdocs && \
	mkdocs build --clean && \
	echo "MkDocs site built in site/"

clean-docs:
	rm -rf mkdocs/site
	rm -rf mkdocs/docs/api

serve-docs:
	cd mkdocs && mkdocs serve

all: build-api-docs build-docs
