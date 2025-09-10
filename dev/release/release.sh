#!/usr/bin/env bash
#
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

set -eu

for cmd in git gh svn; do
  if ! command -v ${cmd} &> /dev/null; then
    echo "This script requires '${cmd}' but it's not installed. Aborting."
    exit 1
  fi
done

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <rc>"
  echo " e.g.: $0 0.1.0 0"
  exit 1
fi

version=$1
rc=$2

git_origin_url="$(git remote get-url origin)"
repository="${git_origin_url#*github.com?}"
repository="${repository%.git}"

if [ "${git_origin_url}" != "git@github.com:apache/iceberg-cpp.git" ]; then
  echo "This script must be ran with a working copy of apache/iceberg-cpp."
  echo "The origin's URL: ${git_origin_url}"
  exit 1
fi

tag="v${version}"
rc_tag="${tag}-rc${rc}"
echo "Tagging for release: ${tag}"
git tag "${tag}" "${rc_tag}^{}" -m "Release ${tag}"
git push origin "${tag}"

release_id="apache-iceberg-cpp-${version}"
dist_url="https://dist.apache.org/repos/dist/release/iceberg"
dist_dev_url="https://dist.apache.org/repos/dist/dev/iceberg"

svn \
  mv "${dist_dev_url}/${release_id}-rc${rc}/" \
  "${dist_url}/${release_id}" \
  -m "Apache Iceberg C++ ${version}"

svn co "${dist_url}/${release_id}"
pushd "${release_id}"

echo "Renaming artifacts to their final release names..."
for fname in ./*; do
  mv "${fname}" "${fname//-rc${rc}/}"
done
echo "Renamed files:"
ls -l

gh release create "${tag}" \
  --repo "${repository}" \
  --title "Apache Iceberg C++ ${version}" \
  --generate-notes \
  --verify-tag \
  *.tar.gz \
  *.tar.gz.asc \
  *.tar.gz.sha512
popd

rm -rf "${release_id}"

echo "Keep only the latest versions"
old_releases=$(
  svn ls "${dist_url}" |
  grep -E '^apache-iceberg-cpp-' |
  sort --version-sort --reverse |
  tail -n +2
)
for old_release_version in ${old_releases}; do
  echo "Remove old release ${old_release_version}"
  svn \
    delete \
    -m "Remove old Apache Iceberg C++ release: ${old_release_version}" \
    "https://dist.apache.org/repos/dist/release/iceberg/${old_release_version}"
done

echo "Success! The release is available here:"
echo "  https://dist.apache.org/repos/dist/release/iceberg/${release_id}"
echo
echo "Add this release to ASF's report database:"
echo "  https://reporter.apache.org/addrelease.html?iceberg"

echo "Draft email for announcement"
echo ""
echo "---------------------------------------------------------"
cat <<MAIL
To: dev@iceberg.apache.org
Cc: announce@apache.org
Hello everyone,

I'm pleased to announce the release of Apache Iceberg C++ v${version}!

Apache Iceberg is an open table format for huge analytic datasets,
Iceberg delivers high query performance for tables with tens of
petabytes of data, along with atomic commits, concurrent writes, and
SQL-compatible table evolution.

This release contains <COMMIT_COUNT> commits from <CONTRIBUTOR_COUNT> unique contributors. Among
the changes in this release are the following highlights:

- <FEATURE_1>
- <FEATURE_2>
- ...
- <FEATURE_N>

This release is hosted at: https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-${version}

For release details and downloads, please visit: https://github.com/apache/iceberg-cpp/releases/tag/apache-iceberg-cpp-${version}

Thanks to everyone for all your contributions!

<AUTHOR>
MAIL
echo "---------------------------------------------------------"
