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

iceberg_rest_sources = files('rest_catalog.cc')
# cpr does not export symbols, so on Windows it must
# be used as a static lib
cpr_needs_static = (
    get_option('default_library') == 'static' or
    host_machine.system() == 'windows'
)
cpr_dep = dependency('cpr', static: cpr_needs_static)

iceberg_rest_build_deps = [iceberg_dep, cpr_dep]
iceberg_rest_lib = library(
    'iceberg_rest',
    sources: iceberg_rest_sources,
    dependencies: iceberg_rest_build_deps,
    gnu_symbol_visibility: 'hidden',
    cpp_shared_args: ['-DICEBERG_REST_EXPORTING'],
    cpp_static_args: ['-DICEBERG_REST_STATIC'],
)

iceberg_rest_compile_args = []
if get_option('default_library') == 'static'
    iceberg_rest_compile_args += ['-DICEBERG_REST_STATIC']
endif
iceberg_rest_dep = declare_dependency(
    link_with: [iceberg_rest_lib],
    dependencies: iceberg_rest_build_deps,
    compile_args: iceberg_rest_compile_args,
)
meson.override_dependency('iceberg-rest', iceberg_rest_dep)
pkg.generate(iceberg_rest_lib)

install_headers(['rest_catalog.h', 'types.h'], subdir: 'iceberg/catalog/rest')
