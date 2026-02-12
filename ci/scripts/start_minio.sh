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

set -eux

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minio}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minio123}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:RELEASE.2024-12-18T00-00-00Z}"
MINIO_CONTAINER_NAME="${MINIO_CONTAINER_NAME:-iceberg-minio}"
MINIO_PORT="${MINIO_PORT:-9000}"
MINIO_CONSOLE_PORT="${MINIO_CONSOLE_PORT:-9001}"
MINIO_BUCKET="${MINIO_BUCKET:-iceberg-test}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://127.0.0.1:${MINIO_PORT}}"

wait_for_minio() {
  for i in {1..30}; do
    if curl -fsS "${MINIO_ENDPOINT}/minio/health/ready" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_minio_docker() {
  if ! command -v docker >/dev/null 2>&1; then
    return 1
  fi

  if docker ps -a --format '{{.Names}}' | grep -q "^${MINIO_CONTAINER_NAME}\$"; then
    docker rm -f "${MINIO_CONTAINER_NAME}"
  fi

  docker run -d --name "${MINIO_CONTAINER_NAME}" \
    -p "${MINIO_PORT}:9000" -p "${MINIO_CONSOLE_PORT}:9001" \
    -e "MINIO_ROOT_USER=${MINIO_ROOT_USER}" \
    -e "MINIO_ROOT_PASSWORD=${MINIO_ROOT_PASSWORD}" \
    "${MINIO_IMAGE}" \
    server /data --console-address ":${MINIO_CONSOLE_PORT}"

  wait_for_minio
}

start_minio_macos() {
  if ! command -v brew >/dev/null 2>&1; then
    echo "brew is required to start MinIO on macOS without Docker" >&2
    return 1
  fi

  brew install minio
  minio server /tmp/minio --console-address ":${MINIO_CONSOLE_PORT}" &
  wait_for_minio
}

download_mc() {
  local uname_out
  uname_out="$(uname -s)"

  local mc_dir
  mc_dir="${RUNNER_TEMP:-/tmp}"
  mkdir -p "${mc_dir}"

  case "${uname_out}" in
    Linux*)
      MC_BIN="${mc_dir}/mc"
      curl -sSL "https://dl.min.io/client/mc/release/linux-amd64/mc" -o "${MC_BIN}"
      chmod +x "${MC_BIN}"
      ;;
    Darwin*)
      MC_BIN="${mc_dir}/mc"
      curl -sSL "https://dl.min.io/client/mc/release/darwin-amd64/mc" -o "${MC_BIN}"
      chmod +x "${MC_BIN}"
      ;;
    MINGW*|MSYS*|CYGWIN*)
      MC_BIN="${mc_dir}/mc.exe"
      curl -sSL "https://dl.min.io/client/mc/release/windows-amd64/mc.exe" -o "${MC_BIN}"
      ;;
    *)
      echo "Unsupported OS for mc: ${uname_out}" >&2
      return 1
      ;;
  esac
}

create_bucket() {
  download_mc
  for i in {1..30}; do
    if "${MC_BIN}" alias set local "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"; then
      break
    fi
    sleep 1
  done
  "${MC_BIN}" mb --ignore-existing "local/${MINIO_BUCKET}"
}

case "$(uname -s)" in
  Darwin*)
    if ! start_minio_docker; then
      start_minio_macos
    fi
    ;;
  Linux*|MINGW*|MSYS*|CYGWIN*)
    start_minio_docker
    ;;
  *)
    echo "Unsupported OS: $(uname -s)" >&2
    exit 1
    ;;
esac

create_bucket
