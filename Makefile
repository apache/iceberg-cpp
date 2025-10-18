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
