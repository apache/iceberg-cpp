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

fetchcontent_declare(googletest
                     GIT_REPOSITORY https://github.com/google/googletest.git
                     GIT_TAG b514bdc898e2951020cbdca1304b75f5950d1f59 # release-1.15.2
                     FIND_PACKAGE_ARGS
                     NAMES
                     GTest)
fetchcontent_makeavailable(googletest)

add_subdirectory(arrow)
add_subdirectory(avro)
add_subdirectory(core)
