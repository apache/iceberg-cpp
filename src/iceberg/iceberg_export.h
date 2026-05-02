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

#if defined(_WIN32) || defined(__CYGWIN__)
#  ifdef ICEBERG_STATIC
#    define ICEBERG_EXPORT
#  elif defined(ICEBERG_EXPORTING)
#    define ICEBERG_EXPORT __declspec(dllexport)
#  else
#    define ICEBERG_EXPORT __declspec(dllimport)
#  endif

#  define ICEBERG_TEMPLATE_EXPORT ICEBERG_EXPORT

// For template class declarations. Empty on MSVC: dllexport on a class template
// declaration combined with extern template triggers C4910.
#  if defined(_MSC_VER)
#    define ICEBERG_TEMPLATE_CLASS_EXPORT
#  else
#    define ICEBERG_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
#  endif

// For extern template declarations. Empty when building the DLL on MSVC:
// `extern` + `dllexport` is contradictory and triggers C4910.
#  if defined(_MSC_VER) && defined(ICEBERG_EXPORTING) && !defined(ICEBERG_STATIC)
#    define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT
#  else
#    define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT ICEBERG_TEMPLATE_EXPORT
#  endif

#else  // Non-Windows
#  ifndef ICEBERG_EXPORT
#    define ICEBERG_EXPORT __attribute__((visibility("default")))
#  endif

#  define ICEBERG_TEMPLATE_EXPORT
#  define ICEBERG_TEMPLATE_CLASS_EXPORT ICEBERG_EXPORT
#  define ICEBERG_EXTERN_TEMPLATE_CLASS_EXPORT ICEBERG_TEMPLATE_CLASS_EXPORT
#endif
