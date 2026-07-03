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
version_suffix = '-SNAPSHOT'
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
conf_data.set('ICEBERG_VERSION_STRING', version + version_suffix)
conf_data.set('PROJECT_NAME', meson.project_name())
configure_file(
    input: 'version.h.in',
    output: 'version.h',
    configuration: conf_data,
    install: true,
    install_dir: get_option('includedir') / 'iceberg',
)

subdir('logging')

iceberg_include_dir = include_directories('..')
iceberg_sources = files(
    'arrow_c_data_guard_internal.cc',
    'arrow_c_data_util.cc',
    'catalog/memory/in_memory_catalog.cc',
    'catalog/session_catalog.cc',
    'catalog/session_context.cc',
    'delete_file_index.cc',
    'expression/aggregate.cc',
    'expression/binder.cc',
    'expression/evaluator.cc',
    'expression/expression.cc',
    'expression/expressions.cc',
    'expression/inclusive_metrics_evaluator.cc',
    'expression/json_serde.cc',
    'expression/literal.cc',
    'expression/manifest_evaluator.cc',
    'expression/predicate.cc',
    'expression/projections.cc',
    'expression/residual_evaluator.cc',
    'expression/rewrite_not.cc',
    'expression/strict_metrics_evaluator.cc',
    'expression/term.cc',
    'file_io.cc',
    'file_io_registry.cc',
    'file_reader.cc',
    'file_writer.cc',
    'inheritable_metadata.cc',
    'inspect/history_table.cc',
    'inspect/metadata_table.cc',
    'inspect/snapshots_table.cc',
    'json_serde.cc',
    'location_provider.cc',
    'manifest/manifest_adapter.cc',
    'manifest/manifest_entry.cc',
    'manifest/manifest_filter_manager.cc',
    'manifest/manifest_group.cc',
    'manifest/manifest_list.cc',
    'manifest/manifest_merge_manager.cc',
    'manifest/manifest_reader.cc',
    'manifest/manifest_util.cc',
    'manifest/manifest_writer.cc',
    'manifest/rolling_manifest_writer.cc',
    'manifest/v1_metadata.cc',
    'manifest/v2_metadata.cc',
    'manifest/v3_metadata.cc',
    'metadata_columns.cc',
    'metrics/commit_report.cc',
    'metrics/counter.cc',
    'metrics/json_serde.cc',
    'metrics/metrics_context.cc',
    'metrics/metrics_reporters.cc',
    'metrics/scan_report.cc',
    'metrics/timer.cc',
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
    'table_identifier.cc',
    'table_metadata.cc',
    'table_properties.cc',
    'table_requirement.cc',
    'table_requirements.cc',
    'table_scan.cc',
    'table_update.cc',
    'transaction.cc',
    'transform.cc',
    'transform_function.cc',
    'type.cc',
    'update/expire_snapshots.cc',
    'update/fast_append.cc',
    'update/merging_snapshot_update.cc',
    'update/pending_update.cc',
    'update/set_snapshot.cc',
    'update/snapshot_manager.cc',
    'update/snapshot_update.cc',
    'update/update_location.cc',
    'update/update_partition_spec.cc',
    'update/update_partition_statistics.cc',
    'update/update_properties.cc',
    'update/update_schema.cc',
    'update/update_snapshot_reference.cc',
    'update/update_sort_order.cc',
    'update/update_statistics.cc',
    'util/base64.cc',
    'util/bucket_util.cc',
    'util/content_file_util.cc',
    'util/conversions.cc',
    'util/decimal.cc',
    'util/gzip_internal.cc',
    'util/murmurhash3_internal.cc',
    'util/property_util.cc',
    'util/retry_util.cc',
    'util/snapshot_util.cc',
    'util/string_util.cc',
    'util/struct_like_set.cc',
    'util/task_group.cc',
    'util/temporal_util.cc',
    'util/timepoint.cc',
    'util/transform_util.cc',
    'util/truncate_util.cc',
    'util/type_util.cc',
    'util/url_encoder.cc',
    'util/uuid.cc',
)

iceberg_data_sources = files(
    'data/data_writer.cc',
    'data/delete_filter.cc',
    'data/delete_loader.cc',
    'data/equality_delete_writer.cc',
    'data/file_scan_task_reader.cc',
    'data/position_delete_writer.cc',
    'data/writer.cc',
    'deletes/position_delete_index.cc',
    'deletes/position_delete_range_consumer.cc',
    'deletes/roaring_position_bitmap.cc',
    'puffin/file_metadata.cc',
    'puffin/json_serde.cc',
    'puffin/puffin_format.cc',
    'puffin/puffin_reader.cc',
    'puffin/puffin_writer.cc',
)

# CRoaring does not export symbols, so on Windows it must
# be used as a static lib
croaring_needs_static = (
    get_option('default_library') == 'static'
    or host_machine.system() == 'windows'
)
croaring_dep = dependency('croaring', static: croaring_needs_static)
nanoarrow_dep = dependency('nanoarrow')
nlohmann_json_dep = dependency('nlohmann_json')
spdlog_dep = dependency('spdlog')
# utf8proc's header declares its functions __declspec(dllimport) on Windows unless
# UTF8PROC_STATIC is defined, and the wrap does not propagate that define to consumers.
# Define it whenever utf8proc is linked statically, so the header's declarations match
# how it is linked. Harmless on other platforms.
utf8proc_needs_static = get_option('default_library') == 'static'
utf8proc_dep = dependency('libutf8proc', static: utf8proc_needs_static)
if utf8proc_needs_static
    utf8proc_dep = declare_dependency(
        compile_args: ['-DUTF8PROC_STATIC'],
        dependencies: utf8proc_dep,
    )
endif
zlib_dep = dependency('zlib')

iceberg_deps = [
    nanoarrow_dep,
    nlohmann_json_dep,
    spdlog_dep,
    utf8proc_dep,
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

iceberg_data_deps = [iceberg_dep, croaring_dep]
iceberg_data_lib = library(
    'iceberg_data',
    sources: iceberg_data_sources,
    dependencies: iceberg_data_deps,
    include_directories: iceberg_include_dir,
    install: true,
    gnu_symbol_visibility: 'inlineshidden',
    cpp_shared_args: ['-DICEBERG_DATA_EXPORTING'],
    cpp_static_args: ['-DICEBERG_DATA_STATIC'],
)

iceberg_data_interface_args = []
if get_option('default_library') == 'static'
    iceberg_data_interface_args += ['-DICEBERG_DATA_STATIC']
endif

iceberg_data_dep = declare_dependency(
    link_with: iceberg_data_lib,
    dependencies: iceberg_data_deps,
    include_directories: iceberg_include_dir,
    compile_args: iceberg_data_interface_args,
)
meson.override_dependency('iceberg-data', iceberg_data_dep)

pkg = import('pkgconfig')
pkg.generate(iceberg_lib)
pkg.generate(iceberg_data_lib)

install_headers(
    [
        'arrow_c_data.h',
        'catalog.h',
        'constants.h',
        'delete_file_index.h',
        'exception.h',
        'file_format.h',
        'file_io.h',
        'file_io_registry.h',
        'file_reader.h',
        'file_writer.h',
        'iceberg_data_export.h',
        'iceberg_export.h',
        'inheritable_metadata.h',
        'location_provider.h',
        'metadata_columns.h',
        'metrics.h',
        'metrics_config.h',
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
        'table_properties.h',
        'table_requirement.h',
        'table_requirements.h',
        'table_scan.h',
        'table_update.h',
        'transaction.h',
        'transform_function.h',
        'transform.h',
        'type_fwd.h',
        'type.h',
    ],
    subdir: 'iceberg',
)

subdir('catalog')
subdir('data')
subdir('deletes')
subdir('encryption')
subdir('expression')
subdir('manifest')
subdir('metrics')
subdir('puffin')
subdir('row')
subdir('update')
subdir('util')
subdir('inspect')

if get_option('tests').enabled()
    subdir('test')
endif
