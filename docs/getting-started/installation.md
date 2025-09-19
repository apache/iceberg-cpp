# Installation

Before installing Iceberg C++, make sure you have the required dependencies:

- CMake 3.25 or higher
- C++17 compatible compiler
- Arrow C++ library
- Avro C++ library
- Thrift library

## Build from Source

### Prerequisites

Install the required dependencies:

```bash
# macOS
brew install cmake arrow avro-cpp thrift

# Ubuntu/Debian
sudo apt-get install cmake libarrow-dev libavro-dev libthrift-dev

# CentOS/RHEL
sudo yum install cmake arrow-devel avro-cpp-devel thrift-devel
```

### Build Instructions

1. Clone the repository:

```bash
git clone https://github.com/apache/iceberg-cpp.git
cd iceberg-cpp
```

2. Create build directory:

```bash
mkdir build && cd build
```

3. Configure with CMake:

```bash
cmake .. -DCMAKE_BUILD_TYPE=Release
```

4. Build:

```bash
make -j$(nproc)
```

5. Install:

```bash
sudo make install
```

## Package Managers

### vcpkg

```bash
vcpkg install iceberg-cpp
```

### Conan

```bash
conan install iceberg-cpp/0.1.0@apache/stable
```

## Dependencies

| Key | Description |
|-----|-------------|
| `arrow` | Support for Apache Arrow integration |
| `avro` | Support for Avro file format |
| `parquet` | Support for Parquet file format |
| `thrift` | Support for Thrift serialization |
