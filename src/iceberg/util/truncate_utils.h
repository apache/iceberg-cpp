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

#include <cstdint>
#include <string>

#include "iceberg/iceberg_export.h"

namespace iceberg {

ICEBERG_EXPORT class TruncateUtils {
 public:
  static std::string TruncateUTF8(std::string&& source, size_t L) {
    size_t code_point_count = 0;
    size_t safe_point = 0;

    for (size_t i = 0; i < source.size(); ++i) {
      // Start of a new UTF-8 code point
      if ((source[i] & 0xC0) != 0x80) {
        code_point_count++;
        if (code_point_count > L) {
          safe_point = i;
          break;
        }
      }
    }

    if (safe_point != 0) {
      // Resize the string to the safe point
      source.resize(safe_point);
    }

    return std::move(source);
  }
};

}  // namespace iceberg
