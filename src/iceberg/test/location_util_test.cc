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

#include "iceberg/util/location_util.h"

#include <gtest/gtest.h>

namespace iceberg {

TEST(LocationUtilTest, StripTrailingSlash) {
  // Test normal paths with trailing slashes
  ASSERT_EQ("/path/to/dir", LocationUtil::StripTrailingSlash("/path/to/dir/"));
  ASSERT_EQ("/path/to/dir", LocationUtil::StripTrailingSlash("/path/to/dir//"));
  ASSERT_EQ("/path/to/dir", LocationUtil::StripTrailingSlash("/path/to/dir///"));

  // Test paths without trailing slashes
  ASSERT_EQ("/path/to/dir", LocationUtil::StripTrailingSlash("/path/to/dir"));
  ASSERT_EQ("/path/to/file.txt", LocationUtil::StripTrailingSlash("/path/to/file.txt"));

  // Test root path
  ASSERT_EQ("", LocationUtil::StripTrailingSlash("/"));
  ASSERT_EQ("", LocationUtil::StripTrailingSlash("//"));

  // Test empty string
  ASSERT_EQ("", LocationUtil::StripTrailingSlash(""));

  // Test URLs with protocols
  ASSERT_EQ("http://example.com",
            LocationUtil::StripTrailingSlash("http://example.com/"));
  ASSERT_EQ("https://example.com/path",
            LocationUtil::StripTrailingSlash("https://example.com/path/"));

  // Test that protocol endings are preserved
  ASSERT_EQ("http://", LocationUtil::StripTrailingSlash("http://"));
  ASSERT_EQ("https://", LocationUtil::StripTrailingSlash("https://"));
  ASSERT_EQ("s3://", LocationUtil::StripTrailingSlash("s3://"));

  // Test paths with protocol-like substrings in the middle
  ASSERT_EQ("/path/http://test", LocationUtil::StripTrailingSlash("/path/http://test/"));
  ASSERT_EQ("/path/https://test",
            LocationUtil::StripTrailingSlash("/path/https://test/"));

  // Test multiple slashes not at the end
  ASSERT_EQ("/path//to/dir", LocationUtil::StripTrailingSlash("/path//to/dir/"));
  ASSERT_EQ("/path///to/dir", LocationUtil::StripTrailingSlash("/path///to/dir/"));
}

}  // namespace iceberg
