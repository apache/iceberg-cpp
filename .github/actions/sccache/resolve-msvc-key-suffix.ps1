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

# Resolve the MSVC toolset version for use as an sccache cache key suffix.
# GitHub runner images roll out new cl.exe builds roughly weekly; keying the
# cache on the toolset version avoids silent misses after each bump.

$PSNativeCommandUseErrorActionPreference = $false

function Resolve-Suffix {
  # Locate vswhere, which ships with every VS installation.
  $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
  if (-not (Test-Path $vswhere)) { return $null }

  # Ask vswhere for the newest installation that includes the C++ toolset.
  $installationPath = (& $vswhere -latest -products * `
    -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
    -property installationPath | Select-Object -First 1)
  if (-not $installationPath) { return $null }

  # Read the default toolset version (e.g. "14.44.35217").
  $versionFile = Join-Path $installationPath.Trim() 'VC\Auxiliary\Build\Microsoft.VCToolsVersion.default.txt'
  if (-not (Test-Path $versionFile)) { return $null }

  $version = (Get-Content -Path $versionFile -Raw).Trim()
  if ($version) { return "-$version" }
  return $null
}

$suffix = Resolve-Suffix
if (-not $suffix) {
  # Degrade gracefully: a shared bucket is better than a failed build.
  $suffix = '-unknown'
  Write-Host "::warning::could not resolve MSVC toolset version for sccache key, using '$suffix'"
}

# Export to both step outputs (for setup-sccache's restore key) and env (for
# save-sccache's save key later in the same job).
Add-Content -Path $env:GITHUB_OUTPUT -Value "suffix=$suffix"
Add-Content -Path $env:GITHUB_ENV -Value "SCCACHE_KEY_SUFFIX=$suffix"
Write-Host "Resolved SCCACHE_KEY_SUFFIX=$suffix"
