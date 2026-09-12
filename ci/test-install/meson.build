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

project(
    'iceberg-install-test',
    'cpp',
    default_options: ['cpp_std=c++23,c++latest'],
)

variant = get_option('default_library') == 'static' ? 'static' : 'shared'
libs = [
    dependency(
        'iceberg_data_' + variant,
        method: 'pkg-config',
        static: variant == 'static',
        allow_fallback: false,
    ),
]
args = []
foreach component : ['rest', 'hive', 'sql_catalog']
    dep = dependency(
        'iceberg_' + component + '_' + variant,
        method: 'pkg-config',
        required: get_option(component),
        static: variant == 'static',
        allow_fallback: false,
    )
    if dep.found()
        libs += dep
        args += '-DINSTALL_TEST_' + component.to_upper()
    endif
endforeach
foreach connector : ['sqlite', 'postgresql', 'mysql']
    if get_option('sql_' + connector)
        args += '-DINSTALL_TEST_SQL_' + connector.to_upper()
    endif
endforeach
smoke = executable(
    'library_smoke',
    'library_smoke.cc',
    dependencies: libs,
    cpp_args: args,
)
test('library_smoke', smoke)

bundle = dependency(
    'iceberg_bundle_' + variant,
    method: 'pkg-config',
    required: get_option('bundle'),
    static: variant == 'static',
    allow_fallback: false,
)
if bundle.found()
    smoke = executable('bundle_smoke', 'bundle_smoke.cc', dependencies: bundle)
    test('bundle_smoke', smoke)
endif
