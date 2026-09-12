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

"""Build and run a separate consumer against a Meson installation."""

import argparse
import json
import os
import subprocess
import tempfile
from pathlib import Path


def run(*args, **kwargs):
    subprocess.run(args, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("builddir", type=Path)
    parser.add_argument("--bundle", choices=["enabled", "disabled"], default="enabled")
    args = parser.parse_args()
    options = {
        item["name"]: item["value"]
        for item in json.loads(
            subprocess.check_output(
                ["meson", "introspect", str(args.builddir), "--buildoptions"], text=True
            )
        )
    }
    prefix = Path(options["prefix"])
    run("meson", "install", "-C", str(args.builddir), "--no-rebuild")
    pkgdir = prefix / options["libdir"] / "pkgconfig"
    bundle_pc = pkgdir / "iceberg_bundle.pc"
    if args.bundle == "disabled":
        if bundle_pc.exists():
            raise RuntimeError("The disabled bundle must not be installed")
    elif not bundle_pc.is_file():
        raise RuntimeError("The bundle pkg-config file was not installed")
    env = os.environ.copy()
    additions = {
        "PKG_CONFIG_PATH": [str(pkgdir)],
        "LD_LIBRARY_PATH": [str(prefix / options["libdir"])],
        "DYLD_LIBRARY_PATH": [str(prefix / options["libdir"])],
        "PATH": [str(prefix / options["bindir"]), str(prefix / options["libdir"])],
    }
    for key, paths in additions.items():
        env[key] = os.pathsep.join(paths + ([env[key]] if env.get(key) else []))
    env["PKG_CONFIG_PATH"] = os.pathsep.join(
        [env["PKG_CONFIG_PATH"], *options.get("pkg_config_path", [])]
    )
    source = Path(__file__).resolve().parents[1] / "test-install"
    libraries = [options["default_library"]]
    if libraries == ["both"]:
        libraries = ["shared", "static"]
    for library in libraries:
        with tempfile.TemporaryDirectory(prefix="iceberg-consumer-") as tmp:
            command = [
                "meson",
                "setup",
                tmp,
                str(source),
                "--default-library=" + library,
                "--buildtype=" + options["buildtype"],
                "-Dbundle=" + args.bundle,
            ]
            for component in ("rest", "hive", "sql_catalog"):
                command.append("-D" + component + "=" + options.get(component, "disabled"))
            for connector in ("sqlite", "postgresql", "mysql"):
                option = "sql_" + connector
                command.append("-D" + option + "=" + str(options.get(option) == "enabled").lower())
            if os.name == "nt":
                command.append("--vsenv")
            run(*command, env=env)
            run("meson", "test", "-C", tmp, "--print-errorlogs", env=env)


if __name__ == "__main__":
    main()
