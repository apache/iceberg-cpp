/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

// Export macros for the main `iceberg` library (DLL). iceberg_data uses
// ICEBERG_DATA_* in iceberg_data_export.h — do not use one macro for both DLLs
// on MSVC.

#if defined(_WIN32) || defined(__CYGWIN__)
#  ifdef ICEBERG_STATIC
#    define ICEBERG_EXPORT
#  elif defined(ICEBERG_EXPORTING)
#    define ICEBERG_EXPORT __declspec(dllexport)
#  else
#    define ICEBERG_EXPORT __declspec(dllimport)
#  endif

// dllexport on a class template declaration combined with extern template
// triggers MSVC C4910, so leave the class-template-decl macro empty there.
#  if defined(_MSC_VER)
#    define ICEBERG_TEMPLATE_CLASS_EXPORT
#  else
#    define ICEBERG_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
#  endif

// `extern template` + `dllexport` is contradictory on MSVC (also C4910).
#  if defined(_MSC_VER) && defined(ICEBERG_EXPORTING) && !defined(ICEBERG_STATIC)
#    define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT
#  else
#    define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
#  endif

// Explicit template instantiation definitions in .cc files must be exported on
// MSVC so dependent DLLs can link imported symbols.
#  define ICEBERG_TEMPLATE_INSTANTIATION_EXPORT ICEBERG_EXPORT

#else  // Non-Windows
#  define ICEBERG_EXPORT __attribute__((visibility("default")))
// Default visibility for explicit template instantiations (hidden inlines
// otherwise; needed when iceberg_data links iceberg).
#  define ICEBERG_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
#  define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
// GCC/Clang can warn when attributes appear on explicit template
// instantiation definitions after the class is already defined.
#  define ICEBERG_TEMPLATE_INSTANTIATION_EXPORT
#endif
