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

conf_data = configuration_data()
version = meson.project_version()
components = version.split('.')
assert(
    components.length() >= 3,
    'The version does not contain major, minor and patch',
)
ver_major = components[0]
ver_minor = components[1]
ver_patch = components[2]
conf_data.set('PROJECT_VERSION_MAJOR', ver_major)
conf_data.set('PROJECT_VERSION_MINOR', ver_minor)
conf_data.set('PROJECT_VERSION_PATCH', ver_patch)
conf_data.set('PROJECT_VERSION', version)
conf_data.set('PROJECT_NAME', meson.project_name())
configure_file(
    input: 'version.h.in',
    output: 'version.h',
    configuration: conf_data,
    install: true,
    install_dir: get_option('includedir') / 'iceberg',
)

iceberg_include_dir = include_directories('..')
iceberg_sources = files(
    'arrow_c_data_guard_internal.cc',
    'catalog/memory/in_memory_catalog.cc',
    'expression/aggregate.cc',
    'expression/binder.cc',
    'expression/evaluator.cc',
    'expression/expression.cc',
    'expression/expressions.cc',
    'expression/inclusive_metrics_evaluator.cc',
    'expression/literal.cc',
    'expression/predicate.cc',
    'expression/rewrite_not.cc',
    'expression/term.cc',
    'file_reader.cc',
    'file_writer.cc',
    'inheritable_metadata.cc',
    'json_internal.cc',
    'manifest/manifest_adapter.cc',
    'manifest/manifest_entry.cc',
    'manifest/manifest_list.cc',
    'manifest/manifest_reader.cc',
    'manifest/manifest_reader_internal.cc',
    'manifest/manifest_writer.cc',
    'manifest/v1_metadata.cc',
    'manifest/v2_metadata.cc',
    'manifest/v3_metadata.cc',
    'metadata_columns.cc',
    'metrics_config.cc',
    'name_mapping.cc',
    'partition_field.cc',
    'partition_spec.cc',
    'partition_summary.cc',
    'row/arrow_array_wrapper.cc',
    'row/manifest_wrapper.cc',
    'row/partition_values.cc',
    'row/struct_like.cc',
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
    'table_properties.cc',
    'table_requirement.cc',
    'table_requirements.cc',
    'table_scan.cc',
    'table_update.cc',
    'transform.cc',
    'transform_function.cc',
    'type.cc',
    'update/update_properties.cc',
    'util/bucket_util.cc',
    'util/conversions.cc',
    'util/decimal.cc',
    'util/gzip_internal.cc',
    'util/murmurhash3_internal.cc',
    'util/temporal_util.cc',
    'util/timepoint.cc',
    'util/truncate_util.cc',
    'util/type_util.cc',
    'util/uuid.cc',
)

# CRoaring does not export symbols, so on Windows it must
# be used as a static lib
croaring_needs_static = (
    get_option('default_library') == 'static' or
    host_machine.system() == 'windows'
)
croaring_dep = dependency('croaring', static: croaring_needs_static)
nanoarrow_dep = dependency('nanoarrow')
nlohmann_json_dep = dependency('nlohmann_json')
spdlog_dep = dependency('spdlog')
zlib_dep = dependency('zlib')

iceberg_deps = [
    croaring_dep,
    nanoarrow_dep,
    nlohmann_json_dep,
    spdlog_dep,
    zlib_dep,
]

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
        'metadata_columns.h',
        'metrics.h',
        'metrics_config.h',
        'name_mapping.h',
        'partition_field.h',
        'partition_spec.h',
        'pending_update.h',
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
        'table_requirement.h',
        'table_requirements.h',
        'table_scan.h',
        'table_update.h',
        'transaction.h',
        'transform_function.h',
        'transform.h',
        'type_fwd.h',
        'type.h',
        'update/update_properties.h',
    ],
    subdir: 'iceberg',
)

subdir('catalog')
subdir('expression')
subdir('manifest')
subdir('row')
subdir('util')

if get_option('tests').enabled()
    subdir('test')
endif
