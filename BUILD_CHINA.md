# Building in China

This guide helps developers in China build iceberg-cpp when network access to GitHub and other international sites is limited.

## Mirror Support

The build system automatically tries alternative download mirrors when the primary URL fails. All third-party dependencies have been configured with China-based mirrors.

### Available Mirrors

Dependencies are automatically downloaded from these mirror sites:

**Apache Projects (Arrow, Nanoarrow):**
- Tsinghua University: https://mirrors.tuna.tsinghua.edu.cn/apache/
- USTC: https://mirrors.ustc.edu.cn/apache/

**GitHub Projects (CRoaring, nlohmann-json, spdlog, cpr):**
- Gitee: https://gitee.com/mirrors/
- FastGit: https://hub.fastgit.xyz/

**Note**: Avro requires a git repository (unreleased version). Automatic mirror fallback is not available for git repositories, but you can specify a custom git mirror using the `ICEBERG_AVRO_GIT_URL` environment variable.

### Custom Mirror URLs

To override the default mirrors, set environment variables before running CMake:

```bash
export ICEBERG_ARROW_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/arrow/arrow-22.0.0/apache-arrow-22.0.0.tar.gz"
export ICEBERG_NANOARROW_URL="https://mirrors.tuna.tsinghua.edu.cn/apache/arrow/apache-arrow-nanoarrow-0.7.0/apache-arrow-nanoarrow-0.7.0.tar.gz"
export ICEBERG_CROARING_URL="https://gitee.com/mirrors/CRoaring/repository/archive/v4.3.11.tar.gz"
export ICEBERG_NLOHMANN_JSON_URL="https://gitee.com/mirrors/JSON-for-Modern-CPP/releases/download/v3.11.3/json.tar.xz"
export ICEBERG_SPDLOG_URL="https://gitee.com/mirrors/spdlog/repository/archive/v1.15.3.tar.gz"
export ICEBERG_CPR_URL="https://gitee.com/mirrors/cpr/repository/archive/1.12.0.tar.gz"

# For Avro, you can use either a tarball URL or a git repository URL:
export ICEBERG_AVRO_URL="https://example.com/avro.tar.gz"  # if you have a tarball
# OR
export ICEBERG_AVRO_GIT_URL="https://gitee.com/mirrors/avro.git"  # for git mirror
```

Then build as usual:

```bash
cmake -S . -B build
cmake --build build
```

## Troubleshooting

**Download failures:**
- Try setting a specific mirror using environment variables
- Use a VPN or proxy: `export https_proxy=http://proxy:port`
- Pre-download tarballs to `~/.cmake/Downloads/`

**Slow downloads:**
- The build will automatically retry with different mirrors
- Consider using Meson build system as an alternative

**Still having issues?**
Open an issue at https://github.com/apache/iceberg-cpp/issues with details about which dependency failed and the error message.
