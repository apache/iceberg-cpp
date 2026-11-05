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

#include <set>
#include <string>
#include <utility>

#include "iceberg/file_io.h"
#include "iceberg/result.h"

namespace iceberg::internal {

// Try every owned output, keeping failed paths for a subsequent cleanup attempt.
inline Status CleanupOutputFiles(FileIO& io, std::set<std::string>& paths) {
  Status first_error;
  for (auto it = paths.begin(); it != paths.end();) {
    auto status = io.DeleteFile(*it);
    if (status.has_value()) {
      it = paths.erase(it);
    } else {
      if (first_error.has_value()) first_error = std::move(status);
      ++it;
    }
  }
  return first_error;
}

// Preserve the operation's error kind while reporting a cleanup failure, if any.
inline Status FailWithOutputCleanup(Error error, FileIO& io,
                                    std::set<std::string>& paths) {
  auto cleanup = CleanupOutputFiles(io, paths);
  if (!cleanup.has_value()) {
    error.message += "; output cleanup failed: " + cleanup.error().message;
  }
  return std::unexpected(std::move(error));
}

}  // namespace iceberg::internal
