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

TEST(LocationUtilTest, ParseScheme) {
  auto s3 = LocationUtil::ParseScheme("S3://bucket/path");
  EXPECT_EQ(s3, "S3");

  auto no_scheme = LocationUtil::ParseScheme("/tmp/file.parquet");
  EXPECT_TRUE(no_scheme.empty());

  auto empty_scheme = LocationUtil::ParseScheme("://bucket/path");
  EXPECT_TRUE(empty_scheme.empty());

  // Not syntactically a scheme -> a path; the extended-length Windows form.
  EXPECT_TRUE(LocationUtil::ParseScheme("\\\\?\\C:\\long\\file.parquet").empty());
  EXPECT_TRUE(LocationUtil::ParseScheme("1:/file.parquet").empty());

#ifdef _WIN32
  // Drive letters are drives; both slash directions occur.
  EXPECT_TRUE(LocationUtil::ParseScheme("C:/tmp/file.parquet").empty());
  EXPECT_TRUE(LocationUtil::ParseScheme("D:\\a\\file.parquet").empty());
#else
  // Elsewhere a single letter stays a scheme for registered implementations.
  EXPECT_EQ(LocationUtil::ParseScheme("C:/tmp/file.parquet"), "C");
#endif
}

TEST(LocationUtilTest, HasScheme) {
  EXPECT_TRUE(LocationUtil::HasScheme("s3://bucket/db/table"));
  EXPECT_TRUE(LocationUtil::HasScheme("hdfs://wh/db/table"));
  EXPECT_TRUE(LocationUtil::HasScheme("file:/tmp/table"));
  EXPECT_TRUE(LocationUtil::HasScheme("S3A://bucket/path"));
  EXPECT_TRUE(LocationUtil::HasScheme("a+b-c.d1:rest"));
  // Strict on every platform, unlike ParseScheme: a drive letter is a scheme.
  EXPECT_TRUE(LocationUtil::HasScheme("C:/tmp/file.parquet"));
  EXPECT_TRUE(LocationUtil::HasScheme("x:"));

  EXPECT_FALSE(LocationUtil::HasScheme(""));
  EXPECT_FALSE(LocationUtil::HasScheme("data/00000-0.parquet"));
  EXPECT_FALSE(LocationUtil::HasScheme("/wh/db/table"));
  EXPECT_FALSE(LocationUtil::HasScheme("://bucket/path"));
  EXPECT_FALSE(LocationUtil::HasScheme(":"));
  // The first character must be a letter.
  EXPECT_FALSE(LocationUtil::HasScheme("1s3://bucket"));
  EXPECT_FALSE(LocationUtil::HasScheme("+s3://bucket"));
  // Characters outside the scheme grammar before the colon.
  EXPECT_FALSE(LocationUtil::HasScheme("data/part:1.parquet"));
  EXPECT_FALSE(LocationUtil::HasScheme("my_scheme://bucket"));
  EXPECT_FALSE(LocationUtil::HasScheme("\\\\?\\C:\\long\\file.parquet"));
}

// Every example from the spec's "Path Resolution" table.
TEST(LocationUtilTest, ResolveLocationSpecExamples) {
  // Relative path: parts are joined on `/`.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://bucket/db/table", "data/00000-0.parquet"),
            "s3://bucket/db/table/data/00000-0.parquet");
  // Absolute path is used.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://bucket/db/table",
                                          "hdfs://wh/db/table/data/00000-0.parquet"),
            "hdfs://wh/db/table/data/00000-0.parquet");
  // Duplicate separator: the table location ends in `/`, which the spec
  // recommends against. No second separator is added (see ResolveLocation).
  EXPECT_EQ(
      LocationUtil::ResolveLocation("s3://bucket/db/table/", "data/00000-0.parquet"),
      "s3://bucket/db/table/data/00000-0.parquet");
  // Duplicate separator: the relative path starts with `/`.
  EXPECT_EQ(
      LocationUtil::ResolveLocation("s3://bucket/db/table", "/data/00000-0.parquet"),
      "s3://bucket/db/table//data/00000-0.parquet");
  // Fully-qualified (v3 and earlier) path is used.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://bucket/db/table",
                                          "s3://bucket/db/table/data/00000-0.parquet"),
            "s3://bucket/db/table/data/00000-0.parquet");
  // Missing scheme (v3 and earlier): prepending a scheme to a pre-v4 path is
  // not resolution and is not done here; such a path is not absolute.
  EXPECT_FALSE(LocationUtil::HasScheme("/wh/db/table/data/00000-0.parquet"));
}

TEST(LocationUtilTest, ResolveLocation) {
  // No `.` or `..` normalization.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "./data/f.parquet"),
            "s3://b/t/./data/f.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "../other/f.parquet"),
            "s3://b/t/../other/f.parquet");
  // A trailing separator on the table location is not doubled; only one
  // trailing separator is consumed, and a leading one on the path is kept.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t/", "f.parquet"),
            "s3://b/t/f.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t//", "f.parquet"),
            "s3://b/t//f.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t/", "/f.parquet"),
            "s3://b/t//f.parquet");
  // A scheme-less table location is joined the same way.
  EXPECT_EQ(LocationUtil::ResolveLocation("/wh/t", "data/f.parquet"),
            "/wh/t/data/f.parquet");
  // A different scheme is absolute.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "gs://other/f.parquet"),
            "gs://other/f.parquet");
  // A colon after the first segment does not make a path absolute.
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "data/a:b.parquet"),
            "s3://b/t/data/a:b.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", ""), "s3://b/t/");
}

TEST(LocationUtilTest, RelativizeLocation) {
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://bucket/db/table",
                                             "s3://bucket/db/table/data/00000-0.parquet"),
            "data/00000-0.parquet");
  // Only one separator is removed.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://bucket/db/table",
                                             "s3://bucket/db/table//data/f.parquet"),
            "/data/f.parquet");
  // Outside the table location: unchanged.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://bucket/db/table",
                                             "s3://bucket/other/f.parquet"),
            "s3://bucket/other/f.parquet");
  // A sibling that shares the prefix is not under the table location.
  EXPECT_EQ(LocationUtil::RelativizeLocation("/t", "/t2/data/f.parquet"),
            "/t2/data/f.parquet");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "s3://b/t2/f.parquet"),
            "s3://b/t2/f.parquet");
  // The table location itself is not under it.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "s3://b/t"), "s3://b/t");
  // A different scheme does not match.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "s3a://b/t/f.parquet"),
            "s3a://b/t/f.parquet");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "gs://b/t/f.parquet"),
            "gs://b/t/f.parquet");
  // Matching is case-sensitive.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "S3://b/t/f.parquet"),
            "S3://b/t/f.parquet");
  // A table location ending in `/` supplies the separator itself.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t/", "s3://b/t/f.parquet"),
            "f.parquet");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t/", "s3://b/t//f.parquet"),
            "/f.parquet");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t/", "s3://b/t/"), "s3://b/t/");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t/", "s3://b/t2/f.parquet"),
            "s3://b/t2/f.parquet");
  // Already relative: unchanged.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "data/f.parquet"),
            "data/f.parquet");
}

// Paths are opaque strings: percent-encoded, reserved and non-ASCII
// characters are copied as-is, never decoded or re-encoded.
TEST(LocationUtilTest, EscapedCharactersArePreserved) {
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "data/a%20b%2Fc.parquet"),
            "s3://b/t/data/a%20b%2Fc.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/my%20t", "data/x=a b/f.parquet"),
            "s3://b/my%20t/data/x=a b/f.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "data/k=v?#&+/f.parquet"),
            "s3://b/t/data/k=v?#&+/f.parquet");
  EXPECT_EQ(LocationUtil::ResolveLocation("s3://b/t", "data/\xc3\xa9t\xc3\xa9.parquet"),
            "s3://b/t/data/\xc3\xa9t\xc3\xa9.parquet");

  EXPECT_EQ(
      LocationUtil::RelativizeLocation("s3://b/my%20t", "s3://b/my%20t/a%2Fb.parquet"),
      "a%2Fb.parquet");
  // An encoded and a decoded form are different paths: no match.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/my%20t", "s3://b/my t/f.parquet"),
            "s3://b/my t/f.parquet");
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/my t", "s3://b/my%20t/f.parquet"),
            "s3://b/my%20t/f.parquet");
  // An encoded separator is not a separator.
  EXPECT_EQ(LocationUtil::RelativizeLocation("s3://b/t", "s3://b/t%2Ff.parquet"),
            "s3://b/t%2Ff.parquet");
}

TEST(LocationUtilTest, RelativizeThenResolveRoundTrips) {
  for (std::string_view table : {"s3://bucket/db/table", "s3://bucket/db/table/"}) {
    for (std::string_view path :
         {"s3://bucket/db/table/data/00000-0.parquet",
          "s3://bucket/db/table/metadata/snap-1.parquet",
          "s3://bucket/db/table//x.parquet", "s3://bucket/other/f.parquet",
          "hdfs://wh/db/table/f.parquet"}) {
      EXPECT_EQ(LocationUtil::ResolveLocation(
                    table, LocationUtil::RelativizeLocation(table, path)),
                path)
          << "table: " << table;
    }
  }
}

}  // namespace iceberg
