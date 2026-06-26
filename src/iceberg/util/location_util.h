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

#include <string>
#include <string_view>

#include "iceberg/iceberg_export.h"

namespace iceberg {

class ICEBERG_EXPORT LocationUtil {
 public:
  static std::string_view StripTrailingSlash(std::string_view path) {
    if (path.empty()) {
      return "";
    }

    while (path.ends_with("/") && !path.ends_with("://")) {
      path.remove_suffix(1);
    }
    return path;
  }

  /// \brief Rewrites S3-compatible schemes (s3a://, s3n://, oss://) to s3:// so
  /// locations and credential prefixes can be prefix-matched uniformly.
  static std::string CanonicalizeS3Scheme(std::string_view location) {
    for (std::string_view scheme : {"s3a://", "s3n://", "oss://"}) {
      if (location.starts_with(scheme)) {
        return std::string("s3://").append(location.substr(scheme.size()));
      }
    }
    return std::string(location);
  }

  /// \brief True if `prefix` matches `path` at a path boundary (equal, next char
  /// '/', or a bare scheme), so `s3://bucket` does not match `s3://bucket-x`.
  static bool PathHasPrefix(std::string_view path, std::string_view prefix) {
    if (!path.starts_with(prefix)) {
      return false;
    }
    return path.size() == prefix.size() || path[prefix.size()] == '/' ||
           prefix.find("://") == std::string_view::npos;
  }
};

}  // namespace iceberg
