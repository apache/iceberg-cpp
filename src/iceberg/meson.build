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
version_string = version + version_suffix
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
conf_data.set('ICEBERG_VERSION_STRING', version_string)
git = find_program('git', required: false)
git_commit_id = 'unknown'
if git.found()
    git_result = run_command(
        git,
        '-C',
        meson.project_source_root(),
        'rev-parse',
        'HEAD',
        check: false,
    )
    if git_result.returncode() == 0
        git_commit_id_candidate = git_result.stdout().strip()
        if git_commit_id_candidate != ''
            git_commit_id = git_commit_id_candidate
        endif
    endif
endif
conf_data.set('ICEBERG_GIT_COMMIT_ID', git_commit_id)
conf_data.set(
    'ICEBERG_FULL_VERSION_STRING',
    'Apache Iceberg-CPP ' + version_string + ' (commit ' + git_commit_id + ')',
)
conf_data.set('PROJECT_NAME', 'Iceberg')
configure_file(
    input: 'version.h.in',
    output: 'version.h',
    configuration: conf_data,
    install: true,
    install_dir: get_option('includedir') / 'iceberg',
)

# Resolve spdlog before generating logging/config.h. The feature value controls
# whether a missing dependency is allowed.
spdlog_dep = dependency(
    'spdlog',
    required: get_option('spdlog'),
    static: true,
    default_options: ['default_library=static'],
)
spdlog_enabled = spdlog_dep.found()

subdir('logging')

iceberg_include_dir = include_directories('..')
iceberg_sources = files(
    'arrow_c_data_guard_internal.cc',
    'arrow_c_data_util.cc',
    'arrow_row_builder.cc',
    'catalog/catalog_util.cc',
    'catalog/memory/in_memory_catalog.cc',
    'catalog/session_catalog.cc',
    'catalog/session_context.cc',
    'delete_file_index.cc',
    'deletes/dv_util.cc',
    'deletes/dv_writer.cc',
    'deletes/position_delete_index.cc',
    'deletes/position_delete_range_consumer.cc',
    'deletes/roaring_position_bitmap.cc',
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
    'expression/sanitize_expression.cc',
    'expression/strict_metrics_evaluator.cc',
    'expression/term.cc',
    'file_io.cc',
    'file_io_registry.cc',
    'file_reader.cc',
    'file_writer.cc',
    'geospatial.cc',
    'inheritable_metadata.cc',
    'inspect/history_table.cc',
    'inspect/metadata_table.cc',
    'inspect/snapshots_table.cc',
    'json_serde.cc',
    'location_provider.cc',
    'logging/cerr_logger.cc',
    'logging/logger.cc',
    'logging/loggers.cc',
    'logging/spdlog_logger.cc',
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
    'puffin/file_metadata.cc',
    'puffin/json_serde.cc',
    'puffin/puffin_format.cc',
    'puffin/puffin_reader.cc',
    'puffin/puffin_writer.cc',
    'resolving_file_io.cc',
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
    'update/delete_files.cc',
    'update/expire_snapshots.cc',
    'update/fast_append.cc',
    'update/merge_append.cc',
    'update/merging_snapshot_update.cc',
    'update/overwrite_files.cc',
    'update/pending_update.cc',
    'update/replace_partitions.cc',
    'update/rewrite_files.cc',
    'update/row_delta.cc',
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
    'util/location_util.cc',
    'util/murmurhash3_internal.cc',
    'util/property_util.cc',
    'util/retry_util.cc',
    'util/snapshot_util.cc',
    'util/string_util.cc',
    'util/struct_like_set.cc',
    'util/task_group.cc',
    'util/temporal_util.cc',
    'util/thread_util.cc',
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
)

# Use upstream CMake for dependencies that would otherwise need local Meson ports.
system_format_static = get_option('default_library') != 'shared'
system_format_variant = system_format_static ? 'static' : 'shared'
cmake_lookup_args = host_machine.system() == 'windows' ? [
    '-DZLIB_USE_STATIC_LIBS=ON',
] : []
sqlpp23_components = []
foreach connector, component : {
    'sqlite': 'SQLite3',
    'postgresql': 'PostgreSQL',
    'mysql': 'MySQL',
}
    if not get_option('sql_' + connector).disabled()
        sqlpp23_components += component
    endif
endforeach
cmake_deps = {
    'croaring': {
        'name': 'croaring',
        'project': 'RoaringBitmap',
        'target': 'roaring',
        'version': '4.3.11',
        'options': {
            'ROARING_BUILD_STATIC': true,
            'BUILD_SHARED_LIBS': false,
            'ENABLE_ROARING_TESTS': false,
            'ROARING_USE_CPM': false,
        },
    },
    'arrow': {
        'name': 'Arrow',
        'project': 'IcebergArrow',
        'target': 'arrow_static',
        'version': '25.0.0',
        'enabled': not get_option('bundle').disabled(),
        'required': get_option('bundle'),
        'lookup': {
            'method': 'cmake',
            'modules': ['Arrow::arrow_' + system_format_variant],
            'version': '>=22.0.0',
            'cmake_args': cmake_lookup_args,
        },
        'options': {
            'ARROW_BUILD_SHARED': false,
            'ARROW_BUILD_STATIC': true,
            'ARROW_BUILD_TESTS': false,
            'ARROW_BUILD_BENCHMARKS': false,
            'ARROW_FILESYSTEM': true,
            'ARROW_IPC': true,
            'ARROW_JSON': true,
            'ARROW_PARQUET': true,
            'ARROW_WITH_ZLIB': true,
            'ARROW_S3': get_option('s3').enabled(),
            'ARROW_SIMD_LEVEL': 'NONE',
            'ARROW_RUNTIME_SIMD_LEVEL': 'NONE',
            'ARROW_POSITION_INDEPENDENT_CODE': true,
            'ARROW_DEPENDENCY_SOURCE': 'BUNDLED',
            'ARROW_MIMALLOC': false,
            'ZLIB_SOURCE': 'SYSTEM',
            'AWSSDK_SOURCE': 'SYSTEM',
            'BUILD_WARNING_LEVEL': 'PRODUCTION',
            'CMAKE_CXX_STANDARD': '20',
        },
    },
    'avro': {
        'name': 'avro-cpp',
        'project': 'IcebergAvro',
        'target': 'avrocpp_s',
        'version': '1.13.0',
        'enabled': not get_option('bundle').disabled(),
        'required': get_option('bundle'),
        'lookup': {
            'method': 'cmake',
            'modules': ['avro-cpp::avrocpp_' + system_format_variant],
            'version': '>=1.13.0',
            'cmake_args': cmake_lookup_args,
        },
        'options': {
            'AVRO_USE_BOOST': false,
            'AVRO_BUILD_TESTS': false,
            'AVRO_BUILD_EXECUTABLES': false,
            'AVRO_BUILD_SHARED': false,
            'AVRO_BUILD_STATIC': true,
            'CMAKE_CXX_VISIBILITY_PRESET': 'hidden',
            'CMAKE_VISIBILITY_INLINES_HIDDEN': true,
        },
    },
    'sqlpp23': {
        'name': 'sqlpp23',
        'project': 'sqlpp23',
        'target': 'sqlpp23',
        'version': '0.69',
        'lookup_name': 'Sqlpp23',
        'lookup': {
            'method': 'cmake',
            'modules': ['sqlpp23::core'],
            'components': sqlpp23_components,
        },
        'enabled': get_option('sql_catalog').enabled() and (not get_option(
            'sql_sqlite',
        ).disabled() or not get_option(
            'sql_postgresql',
        ).disabled() or not get_option(
            'sql_mysql',
        ).disabled()),
        'options': {'BUILD_WITH_MODULES': false},
    },
}
# Keep the native CMake configuration consistent with Meson's build type.
cmake_build_types = {
    'debug': 'Debug',
    'debugoptimized': 'RelWithDebInfo',
    'release': 'Release',
    'minsize': 'MinSizeRel',
}
cmake_build_type = cmake_build_types.get(get_option('buildtype'), 'Release')
arrow_fallback_dep = disabler()
avro_fallback_dep = disabler()
arrow_system_lookup = cmake_deps['arrow']['lookup']
avro_system_lookup = cmake_deps['avro']['lookup']
arrow_system_static = system_format_static
avro_system_static = system_format_static
cmake = import('cmake')
fs = import('fs')
pkg = import('pkgconfig')
foreach name, spec : cmake_deps
    if not spec.get('enabled', true)
        continue
    endif
    forced = get_option('wrap_mode') == 'forcefallback' or name in get_option(
        'force_fallback_for',
    ) or spec['name'] in get_option(
        'force_fallback_for',
    )
    if not forced
        variants = ['static']
        if name in ['arrow', 'avro'] and not system_format_static
            variants = ['shared', 'static']
        endif
        foreach variant : variants
            lookup = spec.get('lookup', {})
            if name == 'arrow'
                lookup += {'modules': ['Arrow::arrow_' + variant]}
            elif name == 'avro'
                lookup += {'modules': ['avro-cpp::avrocpp_' + variant]}
            endif
            system_dep = dependency(
                spec.get('lookup_name', spec['name']),
                static: variant == 'static',
                required: false,
                allow_fallback: false,
                kwargs: lookup,
            )
            if name == 'arrow'
                arrow_system_lookup = lookup
                arrow_system_static = variant == 'static'
                if system_dep.found()
                    system_parquet_dep = dependency(
                        'Parquet',
                        method: 'cmake',
                        modules: ['Parquet::parquet_' + variant],
                        static: arrow_system_static,
                        cmake_args: cmake_lookup_args,
                        required: false,
                        allow_fallback: false,
                    )
                    if not system_parquet_dep.found()
                        system_dep = disabler()
                    endif
                endif
            elif name == 'avro'
                avro_system_lookup = lookup
                avro_system_static = variant == 'static'
            endif
            if system_dep.found()
                break
            endif
        endforeach
        if name == 'sqlpp23' and (system_dep.found() or get_option('wrap_mode') == 'nofallback')
            meson.override_dependency('sqlpp23', system_dep, static: true)
        endif
        if system_dep.found() or get_option('wrap_mode') == 'nofallback'
            continue
        endif
    endif
    opts = cmake.subproject_options()
    project_include = 'CMAKE_PROJECT_' + spec['project'] + '_INCLUDE'
    opts.add_cmake_defines(
        {
            project_include: meson.project_source_root() / 'cmake_modules/IcebergMesonSubproject.cmake',
        },
    )
    if host_machine.system() == 'windows'
        opts.add_cmake_defines({'ZLIB_USE_STATIC_LIBS': true})
    endif
    opts.add_cmake_defines(
        {
            'BUILD_TESTING': false,
            'CMAKE_POSITION_INDEPENDENT_CODE': true,
            'CMAKE_BUILD_TYPE': cmake_build_type,
            'CMAKE_INSTALL_LIBDIR': get_option('libdir'),
            'CMAKE_INSTALL_INCLUDEDIR': get_option('includedir'),
        } + spec['options'],
    )
    interface_args = []
    if name == 'avro' and host_machine.system() == 'darwin'
        # Keep std::any type information visible across the shared bundle boundary.
        interface_args = ['-DAVRO_DYN_LINK']
        opts.append_compile_args('cpp', interface_args)
    elif name == 'sqlpp23'
        opts.set_install(false)
    endif
    upstream = cmake.subproject(
        name,
        options: opts,
        required: spec.get('required', true),
    )
    if not upstream.found()
        continue
    endif
    cmake_program = find_program('cmake')
    cmake_build_dir = fs.parent(
        upstream.target('iceberg_meson_paths').full_path(),
    ) / '__CMake_build'
    source_dir = run_command(
        cmake_program,
        '-E',
        'cat',
        cmake_build_dir / 'iceberg-meson-source-dir.txt',
        check: true,
    ).stdout().strip()
    dep = declare_dependency(
        dependencies: upstream.dependency(spec['target']),
        version: spec['version'],
        compile_args: interface_args,
    )
    meson.override_dependency(spec['name'], dep, static: true)

    if name == 'arrow'
        arrow_fallback_dep = dep
        # Meson's object-library conversion drops ExternalProject ordering.
        # xsimd is header-only; let upstream CMake prepare it during setup.
        run_command(
            cmake_program,
            '--build',
            cmake_build_dir,
            '--target',
            'xsimd_ep',
            check: true,
        )
        arrow_thrift_dep = upstream.dependency('thrift')
        meson.override_dependency('arrow-filesystem', dep, static: true)
        meson.override_dependency('arrow-json', dep, static: true)
        meson.override_dependency(
            'parquet',
            upstream.dependency('parquet_static'),
        )
        pkg.generate(
            upstream.target('arrow_static'),
            libraries: upstream.dependency('arrow_static'),
            filebase: 'arrow',
            version: spec['version'],
        )
        pkg.generate(
            upstream.target('parquet_static'),
            libraries: upstream.dependency('parquet_static'),
            filebase: 'parquet',
            version: spec['version'],
        )
        thrift_version = ''
        foreach line : fs.read(source_dir / 'cpp/thirdparty/versions.txt').split(
            '\n',
        )
            if line.startswith('ARROW_THRIFT_BUILD_VERSION=')
                thrift_version = line.split('=')[1].strip()
            endif
        endforeach
        pkg.generate(
            upstream.target('thrift'),
            libraries: arrow_thrift_dep,
            filebase: 'thrift',
            version: thrift_version,
        )
        meson.add_install_script(
            cmake_program,
            '--install',
            cmake_build_dir,
            '--component',
            'iceberg_dependency_headers',
        )
    elif name == 'avro'
        avro_fallback_dep = dep
        pkg.generate(
            upstream.target('avrocpp_s'),
            libraries: upstream.dependency('avrocpp_s'),
            filebase: 'avro-cpp',
            version: spec['version'],
            extra_cflags: interface_args,
        )
        install_subdir(
            source_dir / 'lang/c++/include/avro',
            install_dir: get_option('includedir'),
        )
    elif name == 'croaring'
        pkg.generate(
            upstream.target('roaring'),
            filebase: 'croaring',
            version: spec['version'],
        )
        install_subdir(
            source_dir / 'include/roaring',
            install_dir: get_option('includedir'),
        )
        install_subdir(
            source_dir / 'cpp/roaring',
            install_dir: get_option('includedir'),
        )
    endif
endforeach

# Match CMake's static vendored dependencies in every Iceberg library mode.
# Explicit fallback options avoid accidentally adding runtime DLL dependencies
# when the top-level project builds shared libraries or both variants.
croaring_dep = dependency('croaring', static: true, allow_fallback: false)
nanoarrow_dep = dependency(
    'nanoarrow',
    static: true,
    default_options: ['default_library=static'],
)
nlohmann_json_dep = dependency('nlohmann_json')
utf8proc_dep = dependency(
    'libutf8proc',
    static: true,
    default_options: ['default_library=static'],
)
utf8proc_dep = declare_dependency(
    compile_args: ['-DUTF8PROC_STATIC'],
    dependencies: utf8proc_dep,
)
zlib_dep = dependency('zlib', cmake_args: cmake_lookup_args)

iceberg_deps = [
    nanoarrow_dep,
    nlohmann_json_dep,
    utf8proc_dep,
    zlib_dep,
    croaring_dep,
]

if spdlog_enabled
    iceberg_deps += spdlog_dep
endif

# All project libraries use the same variant and installation rules below.
iceberg_libraries = {
    'iceberg': {'sources': iceberg_sources, 'dependencies': iceberg_deps},
    'iceberg_data': {'sources': iceberg_data_sources, 'parent': 'iceberg'},
}

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
        'geospatial.h',
        'iceberg_bundle_export.h',
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
        'resolving_file_io.h',
        'result.h',
        'schema_field.h',
        'schema.h',
        'schema_util.h',
        'snapshot.h',
        'sort_field.h',
        'sort_order.h',
        'statistics_file.h',
        'storage_credential.h',
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

# Keep the bundle's sources and dependencies aligned with ICEBERG_BUILD_BUNDLE.
# Source fallbacks use static format libraries, as CMake's vendored builds do.
if get_option('sigv4').enabled() and not get_option('rest').enabled()
    error('sigv4 requires rest=enabled')
endif
if get_option('s3').enabled() and get_option('bundle').disabled()
    error('s3 requires bundle=enabled')
endif
s3_enabled = false
bundle_enabled = false
if not get_option('bundle').disabled()
    # CMake package exports carry the full transitive static link interface.
    if arrow_fallback_dep.found()
        arrow_dep = arrow_fallback_dep
    else
        arrow_dep = dependency(
            'Arrow',
            static: arrow_system_static,
            required: get_option('bundle'),
            allow_fallback: false,
            kwargs: arrow_system_lookup,
        )
    endif
    arrow_filesystem_dep = dependency(
        'arrow-filesystem',
        static: arrow_fallback_dep.found() or arrow_system_static,
        required: get_option('bundle'),
        allow_fallback: false,
    )
    arrow_json_dep = dependency(
        'arrow-json',
        static: arrow_fallback_dep.found() or arrow_system_static,
        required: get_option('bundle'),
        allow_fallback: false,
    )
    if arrow_dep.type_name() == 'internal'
        parquet_dep = dependency('parquet', allow_fallback: false)
    else
        parquet_dep = dependency(
            'Parquet',
            method: 'cmake',
            modules: [
                'Parquet::parquet_' + (arrow_system_static ? 'static' : 'shared'),
            ],
            static: arrow_system_static,
            cmake_args: cmake_lookup_args,
            required: get_option('bundle'),
            allow_fallback: false,
        )
    endif
    if avro_fallback_dep.found()
        avro_dep = avro_fallback_dep
    else
        avro_dep = dependency(
            'avro-cpp',
            static: avro_system_static,
            required: get_option('bundle'),
            allow_fallback: false,
            kwargs: avro_system_lookup,
        )
    endif
    bundle_enabled = (
        arrow_dep.found()
        and arrow_filesystem_dep.found()
        and arrow_json_dep.found()
        and parquet_dep.found()
        and avro_dep.found()
    )
endif

if bundle_enabled
    if not get_option('s3').disabled()
        if arrow_dep.type_name() == 'internal'
            s3_enabled = get_option('s3').enabled()
        else
            s3_enabled = cpp.has_header_symbol(
                'arrow/util/config.h',
                'ARROW_S3',
                dependencies: arrow_dep,
            )
        endif
        if get_option('s3').enabled() and not s3_enabled
            error('s3=enabled requires an Arrow build with S3 support')
        endif
    endif
    iceberg_bundle_sources = files(
        'arrow/arrow_c_data_util.cc',
        'arrow/arrow_io.cc',
        'arrow/arrow_register.cc',
        'arrow/literal_util.cc',
        'arrow/metadata_column_util.cc',
        'arrow/s3/arrow_s3_file_io.cc',
        'avro/avro_data_util.cc',
        'avro/avro_direct_decoder.cc',
        'avro/avro_direct_encoder.cc',
        'avro/avro_metrics.cc',
        'avro/avro_reader.cc',
        'avro/avro_register.cc',
        'avro/avro_schema_util.cc',
        'avro/avro_stream_internal.cc',
        'avro/avro_writer.cc',
        'parquet/parquet_data_util.cc',
        'parquet/parquet_metrics.cc',
        'parquet/parquet_reader.cc',
        'parquet/parquet_register.cc',
        'parquet/parquet_schema_util.cc',
        'parquet/parquet_writer.cc',
    )
    iceberg_bundle_format_deps = [
        arrow_dep,
        arrow_filesystem_dep,
        arrow_json_dep,
        parquet_dep,
        avro_dep,
    ]
    avro_shared_dep = avro_dep
    if get_option('default_library') != 'static' and avro_dep.type_name() != 'internal'
        system_avro_shared_dep = dependency(
            'avro-cpp',
            method: 'cmake',
            modules: ['avro-cpp::avrocpp_shared'],
            cmake_args: cmake_lookup_args,
            version: '>=1.13.0',
            static: false,
            required: false,
            allow_fallback: false,
        )
        if system_avro_shared_dep.found()
            avro_shared_dep = system_avro_shared_dep
        endif
    endif
    iceberg_bundle_shared_format_deps = [
        arrow_dep,
        arrow_filesystem_dep,
        arrow_json_dep,
        parquet_dep,
        avro_shared_dep,
    ]
    if get_option('default_library') != 'static' and arrow_dep.type_name() != 'internal'
        # Like CMake, prefer system shared format libraries for the shared
        # bundle. Source fallbacks continue to use static libraries.
        arrow_shared_dep = dependency(
            'Arrow',
            method: 'cmake',
            modules: ['Arrow::arrow_shared'],
            cmake_args: cmake_lookup_args,
            static: false,
            required: false,
            allow_fallback: false,
        )
        parquet_shared_dep = dependency(
            'Parquet',
            method: 'cmake',
            modules: ['Parquet::parquet_shared'],
            cmake_args: cmake_lookup_args,
            static: false,
            required: false,
            allow_fallback: false,
        )
        iceberg_bundle_shared_format_deps = [
            arrow_shared_dep.found() ? arrow_shared_dep : arrow_dep,
            parquet_shared_dep.found() ? parquet_shared_dep : parquet_dep,
            avro_shared_dep,
        ]
    endif
    iceberg_libraries += {
        'iceberg_bundle': {
            'sources': iceberg_bundle_sources,
            'dependencies': iceberg_bundle_format_deps,
            'shared_dependencies': iceberg_bundle_shared_format_deps,
            'public_dependencies': true,
            'parent': 'iceberg_data',
            'compile_args': ['-DICEBERG_S3_ENABLED=' + (s3_enabled ? '1' : '0')],
        },
    }

    subdir('arrow')
    subdir('avro')
    subdir('parquet')
endif

subdir('catalog')

# Keep static and shared dependency graphs separate, including in `both` mode.
# This also propagates Windows *_STATIC macros to static consumers only.
iceberg_variants = get_option('default_library') == 'both' ? [
    'static',
    'shared',
] : [
    get_option('default_library'),
]
iceberg_preferred_variant = get_option('default_library') == 'static' ? 'static' : 'shared'
foreach name, spec : iceberg_libraries
    parent = spec.get('parent', '')
    foreach variant : iceberg_variants
        deps = spec.get(variant + '_dependencies', spec.get('dependencies', []))
        public_deps = spec.get('public_dependencies', false) ? deps : []
        requires = []
        if parent != ''
            deps = [get_variable(parent + '_' + variant + '_dep')] + deps
            requires = [parent + '_' + variant]
        endif
        if name == 'iceberg_bundle' and avro_dep.type_name() == 'internal'
            # The CMake wrapper otherwise makes Avro a private pkg-config requirement.
            requires += ['avro-cpp']
        endif
        interface_args = cpp.get_supported_arguments('/Zc:preprocessor')
        private_args = spec.get('compile_args', [])
        output_name = name
        if variant == 'static'
            if host_machine.system() == 'windows'
                interface_args += '-D' + name.to_upper() + '_STATIC'
                if cpp.get_id() in ['msvc', 'clang-cl']
                    output_name += '_static'
                endif
            endif
        else
            private_args += '-D' + name.to_upper() + '_EXPORTING'
        endif
        lib = build_target(
            output_name,
            target_type: variant + '_library',
            sources: spec['sources'],
            dependencies: deps,
            include_directories: [iceberg_include_dir] + spec.get(
                'include_directories',
                [],
            ),
            install: true,
            gnu_symbol_visibility: variant == 'shared' ? 'inlineshidden' : 'default',
            cpp_args: private_args + interface_args,
        )
        dep = declare_dependency(
            link_with: lib,
            dependencies: deps,
            include_directories: iceberg_include_dir,
            compile_args: interface_args,
        )
        set_variable(name + '_' + variant + '_lib', lib)
        set_variable(name + '_' + variant + '_dep', dep)
        pkg.generate(
            lib,
            libraries: public_deps,
            filebase: name + '_' + variant,
            requires: requires,
            extra_cflags: interface_args,
        )
        meson.override_dependency(
            name.replace('_', '-'),
            dep,
            static: variant == 'static',
        )
        if variant == iceberg_preferred_variant
            set_variable(name + '_lib', lib)
            set_variable(name + '_dep', dep)
            pkg.generate(
                lib,
                libraries: public_deps,
                filebase: name,
                requires: requires,
                extra_cflags: interface_args,
            )
        endif
    endforeach
endforeach

if get_option('tests').enabled()
    subdir('test')
endif

if get_option('benchmarks').enabled()
    subdir('benchmark')
endif
