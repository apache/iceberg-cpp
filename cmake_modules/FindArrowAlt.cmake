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

if(ArrowAlt_FOUND)
  return()
endif()

set(find_package_args)
if(ArrowAlt_FIND_VERSION)
  list(APPEND find_package_args ${ArrowAlt_FIND_VERSION})
endif()
if(ArrowAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(Arrow ${find_package_args})
if(Arrow_FOUND)
  set(ArrowAlt_FOUND TRUE)
  set(ArrowAlt_VERSION ${ARROW_VERSION})
  return()
endif()

if(ARROW_ROOT)
  find_library(ARROW_STATIC_LIB
               NAMES arrow
               PATHS ${ARROW_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${ICEBERG_LIBRARY_PATH_SUFFIXES})
  find_path(ARROW_INCLUDE_DIR
            NAMES arrow/api.h
            PATHS ${ARROW_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ICEBERG_LIBRARY_PATH_SUFFIXES})
else()
  find_library(ARROW_STATIC_LIB
               NAMES arrow
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(ARROW_INCLUDE_DIR
            NAMES arrow/api.h
            PATH_SUFFIXES ${ICEBERG_INCLUDE_PATH_SUFFIXES})
endif()
if(ARROW_INCLUDE_DIR)
  file(READ "${ARROW_INCLUDE_DIR}/arrow/util/config.h" ARROW_CONFIG_HH_CONTENT)
  string(REGEX MATCH "#define ARROW_VERSION_STRING \"[0-9.]+\"" ARROW_VERSION_DEFINITION
               "${ARROW_CONFIG_HH_CONTENT}")
  string(REGEX MATCH "[0-9.]+" ARROW_VERSION "${ARROW_VERSION_DEFINITION}")
endif()

find_package_handle_standard_args(
  ArrowAlt
  REQUIRED_VARS ARROW_STATIC_LIB ARROW_INCLUDE_DIR
  VERSION_VAR ARROW_VERSION)

if(ArrowAlt_FOUND)
  if(NOT TARGET Arrow::arrow_static)
    add_library(Arrow::arrow_static STATIC IMPORTED)
    set_target_properties(Arrow::arrow_static
                          PROPERTIES IMPORTED_LOCATION "${ARROW_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ARROW_INCLUDE_DIR}")
  endif()
    set(ArrowAlt_VERSION ${ARROW_VERSION})
endif()
