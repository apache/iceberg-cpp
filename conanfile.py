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
import os

from conan import ConanFile
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
from conan.tools.files import (
    copy,
    rm,
    rmdir,
)

required_conan_version = ">=2.1.0"


class IcebergCppConan(ConanFile):
    name = "iceberg-cpp"
    description = "Apache Iceberg C++ client library"
    license = "Apache-2.0" f
    homepage = "https://github.com/redpanda-data/iceberg-cpp"
    url = "https://github.com/redpanda-data/iceberg-cpp"
    package_type = "static-library"
    settings = "os", "arch", "compiler", "build_type"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    def export_sources(self):
        for pattern in [
            "CMakeLists.txt",
            "LICENSE",
            "NOTICE",
            "cmake_modules/*",
            "src/*",
        ]:
            copy(self, pattern, src=self.recipe_folder, dst=self.export_sources_folder)

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        self.options["arrow/*"].with_json = True

    def layout(self):
        cmake_layout(self)

    def requirements(self):
        self.requires("arrow/22.0.0", transitive_headers=True)
        self.requires("libcurl/[>=7.78 <9]")
        self.requires("openssl/[>=1.1 <4]")
        self.requires("zlib/[>=1.2.11 <2]")
        self.requires("snappy/[>=1.1 <2]")
        self.requires("zstd/[>=1.5 <2]")

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["ICEBERG_BUILD_BUNDLE"] = True
        tc.variables["ICEBERG_BUILD_TESTS"] = False
        tc.variables["ICEBERG_BUILD_REST"] = True
        tc.variables["ICEBERG_BUILD_STATIC"] = True
        tc.variables["ICEBERG_BUILD_SHARED"] = False
        tc.variables["CMAKE_FIND_USE_PACKAGE_REGISTRY"] = False
        # GCC false positive: inlining std::expected<T, Error> destructor in
        # json_internal.cc makes GCC think Error::~Error() frees a non-heap
        # pointer.  Same family as https://gcc.gnu.org/bugzilla/show_bug.cgi?id=118867
        if self.settings.compiler == "gcc":
            tc.extra_cxxflags = ["-Wno-free-nonheap-object"]
        tc.generate()
        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(
            self,
            "LICENSE",
            src=self.source_folder,
            dst=os.path.join(self.package_folder, "licenses"),
        )
        cmake = CMake(self)
        cmake.install()
        # Remove vendored Arrow/Parquet — consumers use Conan's Arrow package.
        rm(
            self,
            "libiceberg_vendored_arrow.a",
            os.path.join(self.package_folder, "lib"),
        )
        rm(
            self,
            "libiceberg_vendored_parquet.a",
            os.path.join(self.package_folder, "lib"),
        )
        rm(
            self,
            "libarrow_bundled_dependencies.a",
            os.path.join(self.package_folder, "lib"),
        )
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        # Vendored libraries (built and packaged by iceberg-cpp)
        self.cpp_info.components["vendored_nanoarrow"].libs = [
            "iceberg_vendored_nanoarrow",
        ]

        self.cpp_info.components["vendored_croaring"].libs = [
            "iceberg_vendored_croaring",
        ]

        self.cpp_info.components["vendored_avrocpp"].libs = [
            "iceberg_vendored_avrocpp",
        ]
        self.cpp_info.components["vendored_avrocpp"].requires = [
            "zlib::zlib",
            "snappy::snappy",
            "zstd::zstd",
        ]

        self.cpp_info.components["vendored_cpr"].libs = ["iceberg_vendored_cpr"]
        self.cpp_info.components["vendored_cpr"].requires = [
            "libcurl::libcurl",
            "openssl::openssl",
        ]

        # Core iceberg library
        self.cpp_info.components["iceberg"].libs = ["iceberg"]
        self.cpp_info.components["iceberg"].requires = [
            "vendored_nanoarrow",
            "vendored_croaring",
        ]

        # REST catalog support
        self.cpp_info.components["iceberg_rest"].libs = ["iceberg_rest"]
        self.cpp_info.components["iceberg_rest"].requires = [
            "iceberg",
            "vendored_cpr",
        ]

        # Bundle (Arrow/Parquet integration)
        self.cpp_info.components["iceberg_bundle"].libs = ["iceberg_bundle"]
        self.cpp_info.components["iceberg_bundle"].requires = [
            "iceberg",
            "arrow::libarrow",
            "arrow::libparquet",
            "vendored_avrocpp",
        ]
