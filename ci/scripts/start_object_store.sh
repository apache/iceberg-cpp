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

set -euxo pipefail

OBJECT_STORE_VERSION="${OBJECT_STORE_VERSION:-1.0.0}"
OBJECT_STORE_IMAGE="${OBJECT_STORE_IMAGE:-rustfs/rustfs:${OBJECT_STORE_VERSION}}"
OBJECT_STORE_CONTAINER_NAME="${OBJECT_STORE_CONTAINER_NAME:-iceberg-object-store}"
OBJECT_STORE_ACCESS_KEY="${AWS_ACCESS_KEY_ID:-admin}"
OBJECT_STORE_SECRET_KEY="${AWS_SECRET_ACCESS_KEY:-password}"
OBJECT_STORE_PORT="${OBJECT_STORE_PORT:-9000}"
OBJECT_STORE_BUCKET="${OBJECT_STORE_BUCKET:-iceberg-test}"
OBJECT_STORE_ENDPOINT="${AWS_ENDPOINT_URL:-http://127.0.0.1:${OBJECT_STORE_PORT}}"
OBJECT_STORE_DIR="${RUNNER_TEMP:-/tmp}/iceberg-object-store"
OBJECT_STORE_LOG=""

wait_for_object_store() {
  for ((attempt = 0; attempt < 60; attempt++)); do
    if curl -fs --connect-timeout 1 --max-time 2 "${OBJECT_STORE_ENDPOINT}/health/ready" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Object store did not become ready at ${OBJECT_STORE_ENDPOINT}." >&2
  if [ -n "${OBJECT_STORE_LOG}" ]; then
    cat "${OBJECT_STORE_LOG}" >&2
  else
    docker logs "${OBJECT_STORE_CONTAINER_NAME}" >&2 || true
  fi
  return 1
}

start_object_store_docker() {
  if docker container inspect "${OBJECT_STORE_CONTAINER_NAME}" >/dev/null 2>&1; then
    docker rm -f "${OBJECT_STORE_CONTAINER_NAME}"
  fi

  docker run -d --name "${OBJECT_STORE_CONTAINER_NAME}" \
    -p "${OBJECT_STORE_PORT}:9000" \
    -e "RUSTFS_ACCESS_KEY=${OBJECT_STORE_ACCESS_KEY}" \
    -e "RUSTFS_SECRET_KEY=${OBJECT_STORE_SECRET_KEY}" \
    -e RUSTFS_CONSOLE_ENABLE=false \
    -e RUSTFS_OBS_LOG_STDOUT_ENABLED=true \
    "${OBJECT_STORE_IMAGE}" /data
}

start_object_store_native() {
  local platform binary
  case "$(uname -s)-$(uname -m)" in
    Darwin-arm64)
      platform=macos-aarch64
      binary=rustfs
      ;;
    MINGW*-x86_64|MSYS*-x86_64|CYGWIN*-x86_64)
      platform=windows-x86_64
      binary=rustfs.exe
      ;;
    *)
      echo "Use Docker to run RustFS on $(uname -s)-$(uname -m)." >&2
      return 1
      ;;
  esac

  local archive="rustfs-${platform}-v${OBJECT_STORE_VERSION}.zip"
  local release_url="https://github.com/rustfs/rustfs/releases/download/${OBJECT_STORE_VERSION}"
  mkdir -p "${OBJECT_STORE_DIR}/data"
  curl -fsSL --retry 3 \
    "${release_url}/${archive}" -o "${OBJECT_STORE_DIR}/${archive}"
  curl -fsSL --retry 3 "${release_url}/SHA256SUMS" -o "${OBJECT_STORE_DIR}/SHA256SUMS"
  (
    cd "${OBJECT_STORE_DIR}"
    grep -F "  ${archive}" SHA256SUMS > rustfs.sha256
    if command -v sha256sum >/dev/null 2>&1; then
      sha256sum --check rustfs.sha256
    else
      shasum -a 256 --check rustfs.sha256
    fi
  )
  unzip -o "${OBJECT_STORE_DIR}/${archive}" -d "${OBJECT_STORE_DIR}"
  chmod +x "${OBJECT_STORE_DIR}/${binary}"
  OBJECT_STORE_LOG="${OBJECT_STORE_DIR}/rustfs.log"
  RUSTFS_ACCESS_KEY="${OBJECT_STORE_ACCESS_KEY}" \
    RUSTFS_SECRET_KEY="${OBJECT_STORE_SECRET_KEY}" \
    RUSTFS_ADDRESS=":${OBJECT_STORE_PORT}" \
    RUSTFS_CONSOLE_ENABLE=false \
    "${OBJECT_STORE_DIR}/${binary}" "${OBJECT_STORE_DIR}/data" \
    >"${OBJECT_STORE_LOG}" 2>&1 &
}

create_bucket() {
  export AWS_ACCESS_KEY_ID="${OBJECT_STORE_ACCESS_KEY}"
  export AWS_SECRET_ACCESS_KEY="${OBJECT_STORE_SECRET_KEY}"
  export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
  # The health endpoint can pass before S3 requests are accepted.
  for ((attempt = 0; attempt < 5; attempt++)); do
    if aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api head-bucket --bucket "${OBJECT_STORE_BUCKET}" >/dev/null 2>&1 || \
      aws --endpoint-url "${OBJECT_STORE_ENDPOINT}" s3api create-bucket --bucket "${OBJECT_STORE_BUCKET}"; then
      return 0
    fi
    sleep 2
  done
  echo "Failed to create bucket ${OBJECT_STORE_BUCKET} at ${OBJECT_STORE_ENDPOINT}." >&2
  return 1
}

if ! command -v aws >/dev/null 2>&1; then
  echo "AWS CLI is required to create the test bucket." >&2
  exit 1
fi

if command -v docker >/dev/null 2>&1 && [ "$(docker info --format '{{.OSType}}' 2>/dev/null)" = linux ]; then
  start_object_store_docker
else
  start_object_store_native
fi

wait_for_object_store
create_bucket
