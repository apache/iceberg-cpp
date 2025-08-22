<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Release Process

This document describes how the release manager releases a new version in accordance with the Apache requirements.

## Introduction

`Source Release` is the key point which Apache values, and is also necessary for an ASF release.

Please remember that publishing software has legal consequences.

This guide complements the foundation-wide policies and guides:

- [Release Policy](https://www.apache.org/legal/release-policy.html)
- [Release Distribution Policy](https://infra.apache.org/release-distribution)
- [Release Creation Process](https://infra.apache.org/release-publishing.html)

## Terminology

In the context of our release, we use several terms to describe different stages of the release process:

- `iceberg_version`: the version of Iceberg to be released, like `0.1.0`.
- `release_version`: the version of release candidate, like `0.1.0-rc.1`.
- `rc_version`: the minor version for voting round, like `rc.1`.

## Preparation

<div class="warning">
This section is the requirements for individuals who are new to the role of release manager.
</div>

Refer to [Setup GPG Key](reference/setup_gpg.md) to make sure the GPG key has been set up.

## Start a tracking issue about the next release

Start a tracking issue on GitHub for the upcoming release to track all tasks that need to be completed.

Title:
```
Tracking issues of Iceberg C++ ${iceberg_version} Release
```

Content:
```markdown
This issue is used to track tasks of the iceberg C++ ${iceberg_version} release.

## Tasks

### Blockers

> Blockers are the tasks that must be completed before the release.

### Build Release

#### GitHub Side

- [ ] Bump version in project
- [ ] Update docs
- [ ] Update CHANGELOG.md
- [ ] Push release candidate tag to GitHub

#### ASF Side

- [ ] Create an ASF Release
- [ ] Upload artifacts to the SVN dist repo

### Voting

- [ ] Start VOTE at iceberg community

### Official Release

- [ ] Push the release git tag
- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Change Iceberg C++ Website download link
- [ ] Send the announcement

For details of each step, please refer to: https://cpp.iceberg.apache.org/release
```

## GitHub Side

### Bump version in project

Bump all components' version in the project to the new iceberg version.
Please note that this version is the exact version of the release, not the release candidate version.

- C++ core: bump version in `CMakeLists.txt`

### Update docs

- Update `CHANGELOG.md` by Drafting a new release [note on Github Releases](https://github.com/apache/iceberg-cpp/releases/new)

### Push release candidate tag

After bump version PR gets merged, we can create a GitHub release for the release candidate:

- Create a tag at `main` branch on the `Bump Version` / `Patch up version` commit: `git tag -s "v0.1.0-rc.1"`, please correctly check out the corresponding commit instead of directly tagging on the main branch.
- Push tags to GitHub: `git push --tags`.

## ASF Side

If any step in the ASF Release process fails and requires code changes,
we will abandon that version and prepare for the next one.
Our release page will only display ASF releases instead of GitHub Releases.

### Create an ASF Release

After GitHub Release has been created, we can start to create ASF Release.

- Checkout to released tag. (e.g. `git checkout v0.1.0-rc.1`, tag is created in the previous step)
- Use the release script to create a new release: `dev/release/release_rc.sh <iceberg_version> <rc_version>`(e.g. `dev/release/release_rc.sh 0.1.0 1`)
  - This script will do the following things:
    - Create and push a release candidate tag
    - Wait for GitHub Actions to build the source tarball
    - Sign the artifacts with GPG
    - Upload to ASF dist dev repository
    - Generate draft email for voting

This script will create a GitHub release with the following artifacts:
- `apache-iceberg-cpp-${release_version}.tar.gz`
- `apache-iceberg-cpp-${release_version}.tar.gz.asc`
- `apache-iceberg-cpp-${release_version}.tar.gz.sha512`

### Upload artifacts to the SVN dist repo

The release script automatically handles SVN upload. The artifacts are uploaded to:
`https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${release_version}/`

Visit that URL to make sure the artifacts are uploaded correctly.

### Rescue

If you accidentally published wrong or unexpected artifacts, like wrong signature files, wrong sha512 files,
please cancel the release for the current `release_version`,
increase the RC counting and re-initiate a release with the new `release_version`.
And remember to delete the wrong artifacts from the SVN dist repo.

## Voting

Iceberg Community Vote should send email to: <dev@iceberg.apache.org>:

Title:
```
[VOTE] Release Apache Iceberg C++ ${release_version} RC1
```

Content:
```
Hello, Apache Iceberg Community,

This is a call for a vote to release Apache Iceberg C++ version ${iceberg_version}.

The tag to be voted on is ${iceberg_version}.

The release candidate:

https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${release_version}/

Keys to verify the release candidate:

https://downloads.apache.org/iceberg/KEYS

Git tag for the release:

https://github.com/apache/iceberg-cpp/releases/tag/v${iceberg_version}

Please download, verify, and test.

The VOTE will be open for at least 72 hours and until the necessary
number of votes are reached.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about Apache Iceberg, please see https://cpp.iceberg.apache.org/

Checklist for reference:

[ ] Download links are valid.
[ ] Checksums and signatures.
[ ] LICENSE/NOTICE files exist
[ ] No unexpected binary files
[ ] All source files have ASF headers
[ ] Can compile from source

More details please refer to https://cpp.iceberg.apache.org/release.html#how-to-verify-a-release.

Thanks

${name}
```

After at least 3 `+1` binding vote (from Iceberg PMC member), claim the vote result:

Title:
```
[RESULT][VOTE] Release Apache Iceberg C++ ${release_version} RC1
```

Content:
```
Hello, Apache Iceberg Community,

The vote to release Apache Iceberg C++ ${release_version} has passed.

The vote PASSED with 3 +1 binding and 1 +1 non-binding votes, no +0 or -1 votes:

Binding votes:

- xxx
- yyy
- zzz

Non-Binding votes:

- aaa

Vote thread: ${vote_thread_url}

Thanks

${name}
```

## How to verify a release

### Validating a source release

A release contains links to following things:

* A source tarball
* A signature(.asc)
* A checksum(.sha512)

After downloading them, here are the instructions on how to verify them.

* Import keys:

```bash
curl https://downloads.apache.org/iceberg/KEYS -o KEYS
gpg --import KEYS
```
* Verify the `.asc` file: `gpg --verify apache-iceberg-cpp-${iceberg_version}.tar.gz.asc`
* Verify the checksums: `shasum -a 512 -c apache-iceberg-cpp-${iceberg_version}.tar.gz.sha512`
* Verify build and test:
```bash
tar -xzf apache-iceberg-cpp-${iceberg_version}.tar.gz
cd apache-iceberg-cpp-${iceberg_version}
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel $(nproc)
ctest --test-dir build --output-on-failure
```

## Official Release

### Push the release git tag

```shell
# Checkout the tags that passed VOTE
git checkout ${release_version}
# Tag with the iceberg version
git tag -s ${iceberg_version}
# Push tags to github to trigger releases
git push origin ${iceberg_version}
```

### Publish artifacts to SVN RELEASE branch

```shell
svn mv https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-cpp-${release_version} https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-${iceberg_version} -m "Release Apache Iceberg C++ ${iceberg_version}"
```

### Change Iceberg C++ Website download link

Update the download link in `website/src/download.md` to the new release version.

### Create a GitHub Release

- Click [here](https://github.com/apache/iceberg-cpp/releases/new) to create a new release.
- Pick the git tag of this release version from the dropdown menu.
- Make sure the branch target is `main`.
- Generate the release note by clicking the `Generate release notes` button.
- Add the release note from `CHANGELOG.md` if there are breaking changes before the content generated by GitHub. Check them carefully.
- Publish the release.

### Send the announcement

Send the release announcement to `dev@iceberg.apache.org` and CC `announce@apache.org`.

Title:
```
[ANNOUNCE] Release Apache Iceberg C++ ${iceberg_version}
```

Content:
```
Hi all,

The Apache Iceberg C++ community is pleased to announce
that Apache Iceberg C++ ${iceberg_version} has been released!

Apache Iceberg is a high-performance format for huge analytic tables that
supports modern analytical data lake architectures. Iceberg C++ provides
a native implementation with excellent performance for C++ applications.

The notable changes since ${previous_version} include:
1. xxxxx
2. yyyyyy
3. zzzzzz

Please refer to the change log for the complete list of changes:
https://github.com/apache/iceberg-cpp/releases/tag/v${iceberg_version}

Apache Iceberg C++ website: https://cpp.iceberg.apache.org/

Download Links: https://cpp.iceberg.apache.org/download

Iceberg Resources:
- Issue: https://github.com/apache/iceberg-cpp/issues
- Mailing list: dev@iceberg.apache.org

Thanks
On behalf of Apache Iceberg Community
```
