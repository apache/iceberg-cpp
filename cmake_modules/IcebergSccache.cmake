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

if(MSVC_TOOLCHAIN AND "${CMAKE_CXX_COMPILER_LAUNCHER}" STREQUAL "sccache")
  message(STATUS "Configuring sccache for MSVC")

  # Keep MSVC Debug objects cacheable by sccache without affecting Release.
  set(CMAKE_MSVC_DEBUG_INFORMATION_FORMAT "$<$<CONFIG:Debug,RelWithDebInfo>:Embedded>")

  # CMP0141 normally handles these flags; normalize any flags injected elsewhere.
  foreach(flag_var CMAKE_C_FLAGS_DEBUG CMAKE_CXX_FLAGS_DEBUG CMAKE_C_FLAGS_RELWITHDEBINFO
                   CMAKE_CXX_FLAGS_RELWITHDEBINFO)
    string(REGEX REPLACE "/Z[iI]" "/Z7" ${flag_var} "${${flag_var}}")
  endforeach()
endif()
