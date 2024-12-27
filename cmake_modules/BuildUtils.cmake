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

# Borrowed the file from Apache Arrow:
# https://github.com/apache/arrow/blob/main/cpp/cmake_modules/BuildUtils.cmake

function(ADD_ICEBERG_LIB LIB_NAME)
  set(options)
  set(one_value_args
      BUILD_SHARED
      BUILD_STATIC
      INSTALL_ARCHIVE_DIR
      INSTALL_LIBRARY_DIR
      INSTALL_RUNTIME_DIR
      SHARED_LINK_FLAGS)
  set(multi_value_args
      SOURCES
      OUTPUTS
      STATIC_LINK_LIBS
      SHARED_LINK_LIBS
      SHARED_PRIVATE_LINK_LIBS
      EXTRA_INCLUDES
      PRIVATE_INCLUDES
      DEPENDENCIES
      DEFINITIONS
      SHARED_INSTALL_INTERFACE_LIBS
      STATIC_INSTALL_INTERFACE_LIBS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})
  if(ARG_UNPARSED_ARGUMENTS)
    message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
  endif()

  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS})
  endif()

  # Allow overriding ICEBERG_BUILD_SHARED and ICEBERG_BUILD_STATIC
  if(DEFINED ARG_BUILD_SHARED)
    set(BUILD_SHARED ${ARG_BUILD_SHARED})
  else()
    set(BUILD_SHARED ${ICEBERG_BUILD_SHARED})
  endif()
  if(DEFINED ARG_BUILD_STATIC)
    set(BUILD_STATIC ${ARG_BUILD_STATIC})
  else()
    set(BUILD_STATIC ${ICEBERG_BUILD_STATIC})
  endif()

  # Prepare arguments for separate compilation of static and shared libs below
  set(LIB_DEPS ${ARG_SOURCES})
  set(EXTRA_DEPS ${ARG_DEPENDENCIES})

  if(ARG_EXTRA_INCLUDES)
    set(LIB_INCLUDES ${ARG_EXTRA_INCLUDES})
  else()
    set(LIB_INCLUDES "")
  endif()

  if(ARG_INSTALL_ARCHIVE_DIR)
    set(INSTALL_ARCHIVE_DIR ${ARG_INSTALL_ARCHIVE_DIR})
  else()
    set(INSTALL_ARCHIVE_DIR ${CMAKE_INSTALL_LIBDIR})
  endif()
  if(ARG_INSTALL_LIBRARY_DIR)
    set(INSTALL_LIBRARY_DIR ${ARG_INSTALL_LIBRARY_DIR})
  else()
    set(INSTALL_LIBRARY_DIR ${CMAKE_INSTALL_LIBDIR})
  endif()
  if(ARG_INSTALL_RUNTIME_DIR)
    set(INSTALL_RUNTIME_DIR ${ARG_INSTALL_RUNTIME_DIR})
  else()
    set(INSTALL_RUNTIME_DIR bin)
  endif()

  if(BUILD_SHARED)
    add_library(${LIB_NAME}_shared SHARED)

    if(LIB_DEPS)
      target_sources(${LIB_NAME}_shared PRIVATE ${LIB_DEPS})
    endif()

    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_shared ${EXTRA_DEPS})
    endif()

    if(ARG_DEFINITIONS)
      target_compile_definitions(${LIB_NAME}_shared PRIVATE ${ARG_DEFINITIONS})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_shared)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_shared SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_shared PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    set_target_properties(${LIB_NAME}_shared
                          PROPERTIES LINK_FLAGS "${ARG_SHARED_LINK_FLAGS}" OUTPUT_NAME
                                                                           ${LIB_NAME})

    target_link_libraries(${LIB_NAME}_shared
                          PUBLIC "$<BUILD_INTERFACE:${ARG_SHARED_LINK_LIBS}>"
                                 "$<INSTALL_INTERFACE:${ARG_SHARED_INSTALL_INTERFACE_LIBS}>"
                          PRIVATE ${ARG_SHARED_PRIVATE_LINK_LIBS})

    install(TARGETS ${LIB_NAME}_shared
            EXPORT ${LIB_NAME}_targets
            ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR}
            LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR}
            RUNTIME DESTINATION ${INSTALL_RUNTIME_DIR}
            INCLUDES
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  if(BUILD_STATIC)
    add_library(${LIB_NAME}_static STATIC)

    if(LIB_DEPS)
      target_sources(${LIB_NAME}_static PRIVATE ${LIB_DEPS})
    endif()

    if(EXTRA_DEPS)
      add_dependencies(${LIB_NAME}_static ${EXTRA_DEPS})
    endif()

    if(ARG_DEFINITIONS)
      target_compile_definitions(${LIB_NAME}_static PRIVATE ${ARG_DEFINITIONS})
    endif()

    if(ARG_OUTPUTS)
      list(APPEND ${ARG_OUTPUTS} ${LIB_NAME}_static)
    endif()

    if(LIB_INCLUDES)
      target_include_directories(${LIB_NAME}_static SYSTEM PUBLIC ${ARG_EXTRA_INCLUDES})
    endif()

    if(ARG_PRIVATE_INCLUDES)
      target_include_directories(${LIB_NAME}_static PRIVATE ${ARG_PRIVATE_INCLUDES})
    endif()

    set_target_properties(${LIB_NAME}_static PROPERTIES OUTPUT_NAME ${LIB_NAME})

    if(ARG_STATIC_INSTALL_INTERFACE_LIBS)
      target_link_libraries(${LIB_NAME}_static
                            INTERFACE "$<INSTALL_INTERFACE:${ARG_STATIC_INSTALL_INTERFACE_LIBS}>"
      )
    endif()

    if(ARG_STATIC_LINK_LIBS)
      target_link_libraries(${LIB_NAME}_static
                            PUBLIC "$<BUILD_INTERFACE:${ARG_STATIC_LINK_LIBS}>")
    endif()

    install(TARGETS ${LIB_NAME}_static
            EXPORT ${LIB_NAME}_targets
            ARCHIVE DESTINATION ${INSTALL_ARCHIVE_DIR}
            LIBRARY DESTINATION ${INSTALL_LIBRARY_DIR}
            RUNTIME DESTINATION ${INSTALL_RUNTIME_DIR}
            INCLUDES
            DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  endif()

  string(TOLOWER ${LIB_NAME} LIB_NAME_LOWER_CASE)
  string(REPLACE "_" "-" LIB_NAME_DASH_SEPARATED_LOWER_CASE ${LIB_NAME_LOWER_CASE})
  install(EXPORT ${LIB_NAME}_targets
          DESTINATION "${ICEBERG_INSTALL_CMAKEDIR}/Iceberg"
          NAMESPACE "Iceberg::"
          FILE "${LIB_NAME_DASH_SEPARATED_LOWER_CASE}-targets.cmake")

  # Modify variable in calling scope
  if(ARG_OUTPUTS)
    set(${ARG_OUTPUTS}
        ${${ARG_OUTPUTS}}
        PARENT_SCOPE)
  endif()
endfunction()
