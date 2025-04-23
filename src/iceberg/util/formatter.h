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

/// \file iceberg/util/formatter.h
/// A specialization of std::formatter for Formattable objects.  This header
/// is separate from iceberg/util/formattable.h so that the latter (which is
/// meant to be included widely) does not leak <format> unnecessarily into
/// other headers.  You must include this header to format a Formattable.

#include <concepts>
#include <format>
#include <string_view>

#include "iceberg/util/formattable.h"

/// \brief Make all classes deriving from iceberg::util::Formattable
///   formattable with std::format.
template <std::derived_from<iceberg::util::Formattable> Derived>
struct std::formatter<Derived> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const iceberg::util::Formattable& obj, FormatContext& ctx) const {
    return std::formatter<string_view>::format(obj.ToString(), ctx);
  }
};

/// \brief std::formatter specialization for any type that has a ToString function
template <typename T>
  requires requires(const T& t) {
    { ToString(t) } -> std::convertible_to<std::string>;
  }
struct std::formatter<T> : std::formatter<std::string_view> {
  template <class FormatContext>
  auto format(const T& value, FormatContext& ctx) const {
    return std::formatter<std::string_view>::format(ToString(value), ctx);
  }
};
