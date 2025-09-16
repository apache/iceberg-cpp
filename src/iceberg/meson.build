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

iceberg_include_dir = include_directories('..')
iceberg_sources = files(
    'arrow_c_data_guard_internal.cc',
    'catalog/in_memory_catalog.cc',
    'expression/expression.cc',
    'expression/literal.cc',
    'file_reader.cc',
    'file_writer.cc',
    'inheritable_metadata.cc',
    'json_internal.cc',
    'manifest_entry.cc',
    'manifest_list.cc',
    'manifest_reader.cc',
    'manifest_reader_internal.cc',
    'manifest_writer.cc',
    'metadata_columns.cc',
    'name_mapping.cc',
    'partition_field.cc',
    'partition_spec.cc',
    'schema.cc',
    'schema_field.cc',
    'schema_internal.cc',
    'schema_util.cc',
    'snapshot.cc',
    'sort_field.cc',
    'sort_order.cc',
    'statistics_file.cc',
    'table.cc',
    'table_metadata.cc',
    'table_scan.cc',
    'transform.cc',
    'transform_function.cc',
    'type.cc',
    'util/decimal.cc',
    'util/gzip_internal.cc',
    'util/murmurhash3_internal.cc',
    'util/timepoint.cc',
)

nanoarrow_dep = dependency('nanoarrow')
nlohmann_json_dep = dependency('nlohmann_json')
spdlog_dep = dependency('spdlog')
zlib_dep = dependency('zlib')

iceberg_deps = [nanoarrow_dep, nlohmann_json_dep, spdlog_dep, zlib_dep]

iceberg_lib = library(
    'iceberg',
    sources: iceberg_sources,
    dependencies: iceberg_deps,
    include_directories: iceberg_include_dir,
    install: true,
    gnu_symbol_visibility: 'inlineshidden',
    cpp_shared_args: ['-DICEBERG_EXPORTING'],
    cpp_static_args: ['-DICEBERG_STATIC'],
)

iceberg_interface_args = []
if get_option('default_library') != 'shared'
    iceberg_interface_args += ['-DICEBERG_STATIC']
endif

iceberg_dep = declare_dependency(
    link_with: iceberg_lib,
    dependencies: iceberg_deps,
    include_directories: iceberg_include_dir,
    compile_args: iceberg_interface_args,
)
meson.override_dependency('iceberg', iceberg_dep)
pkg = import('pkgconfig')
pkg.generate(iceberg_lib)

install_headers(
    [
        'arrow_c_data.h',
        'catalog.h',
        'constants.h',
        'exception.h',
        'file_format.h',
        'file_io.h',
        'file_reader.h',
        'file_writer.h',
        'iceberg_export.h',
        'inheritable_metadata.h',
        'location_provider.h',
        'manifest_adapter.h',
        'manifest_entry.h',
        'manifest_list.h',
        'manifest_reader.h',
        'manifest_writer.h',
        'metadata_columns.h',
        'metrics.h',
        'name_mapping.h',
        'partition_field.h',
        'partition_spec.h',
        'result.h',
        'schema_field.h',
        'schema.h',
        'schema_util.h',
        'snapshot.h',
        'sort_field.h',
        'sort_order.h',
        'statistics_file.h',
        'table.h',
        'table_identifier.h',
        'table_metadata.h',
        'table_scan.h',
        'transaction.h',
        'transform_function.h',
        'transform.h',
        'type_fwd.h',
        'type.h',
        'v1_metadata.h',
        'v2_metadata.h',
        'v3_metadata.h',
    ],
    subdir: 'iceberg',
)

subdir('catalog')
subdir('expression')
subdir('util')

if get_option('tests').enabled()
    subdir('test')
endif
