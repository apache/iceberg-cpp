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

#include "iceberg/catalog/hive/hms_client.h"

#include <cctype>
#include <charconv>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include <thrift/Thrift.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportException.h>

#include "ThriftHiveMetastore.h"
#include "iceberg/util/macros.h"

namespace iceberg::hive {

namespace {

constexpr std::string_view kThriftPrefix = "thrift://";

std::string_view StripScheme(std::string_view spec) {
  if (spec.starts_with(kThriftPrefix)) {
    return spec.substr(kThriftPrefix.size());
  }
  return spec;
}

std::string_view Trim(std::string_view spec) {
  while (!spec.empty() && (std::isspace(static_cast<unsigned char>(spec.front())) != 0)) {
    spec.remove_prefix(1);
  }
  while (!spec.empty() && (std::isspace(static_cast<unsigned char>(spec.back())) != 0)) {
    spec.remove_suffix(1);
  }
  return spec;
}

Result<HmsEndpoint> ParseSingleEndpoint(std::string_view spec) {
  spec = Trim(spec);
  spec = StripScheme(spec);
  spec = Trim(spec);
  if (spec.empty()) {
    return InvalidArgument("Empty HMS endpoint in URI list.");
  }

  HmsEndpoint endpoint;
  const auto colon = spec.rfind(':');
  if (colon == std::string_view::npos) {
    endpoint.host = std::string(spec);
    endpoint.port = kDefaultHmsPort;
    return endpoint;
  }

  endpoint.host = std::string(spec.substr(0, colon));
  if (endpoint.host.empty()) {
    return InvalidArgument("HMS endpoint has empty host: '{}'.", spec);
  }

  const auto port_str = spec.substr(colon + 1);
  if (port_str.empty()) {
    endpoint.port = kDefaultHmsPort;
    return endpoint;
  }

  int port = 0;
  const auto* const port_end = port_str.data() + port_str.size();
  const auto [ptr, ec] = std::from_chars(port_str.data(), port_end, port);
  if (ec != std::errc() || ptr != port_end || port <= 0 || port > 65535) {
    return InvalidArgument("Invalid HMS port in endpoint '{}'.", spec);
  }
  endpoint.port = port;
  return endpoint;
}

}  // namespace

Result<std::vector<HmsEndpoint>> ParseHmsUris(std::string_view uri) {
  std::vector<HmsEndpoint> endpoints;
  if (Trim(uri).empty()) {
    return InvalidArgument("HMS URI is empty.");
  }

  std::size_t pos = 0;
  while (pos <= uri.size()) {
    const auto comma = uri.find(',', pos);
    const auto piece = uri.substr(
        pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos);
    ICEBERG_ASSIGN_OR_RAISE(auto endpoint, ParseSingleEndpoint(piece));
    endpoints.push_back(std::move(endpoint));
    if (comma == std::string_view::npos) {
      break;
    }
    pos = comma + 1;
  }
  return endpoints;
}

// Fields are declared in dependency order (socket <- transport <- protocol
// <- client) so the client tears down before the transport it borrows.
class HmsClient::Impl {
 public:
  std::shared_ptr<apache::thrift::transport::TSocket> socket;
  std::shared_ptr<apache::thrift::transport::TTransport> transport;
  std::shared_ptr<apache::thrift::protocol::TProtocol> protocol;
  std::unique_ptr<Apache::Hadoop::Hive::ThriftHiveMetastoreClient> client;
};

HmsClient::HmsClient(std::unique_ptr<Impl> impl) : impl_(std::move(impl)) {}

HmsClient::~HmsClient() {
  if (impl_ && impl_->transport && impl_->transport->isOpen()) {
    try {
      impl_->transport->close();
    } catch (const apache::thrift::TException&) {
      // Best-effort close on teardown; ignore exceptions.
    }
  }
}

Result<std::unique_ptr<HmsClient>> HmsClient::Connect(
    const HiveCatalogProperties& config) {
  ICEBERG_ASSIGN_OR_RAISE(auto uri, config.Uri());
  ICEBERG_ASSIGN_OR_RAISE(auto endpoints, ParseHmsUris(uri));
  ICEBERG_ASSIGN_OR_RAISE(auto transport_mode, config.ThriftTransport());

  if (endpoints.size() > 1) {
    return InvalidArgument(
        "Multi-endpoint HMS URIs are not yet supported; HA failover is not "
        "implemented. Configure a single endpoint. Got {} endpoints.",
        endpoints.size());
  }
  const HmsEndpoint& endpoint = endpoints.front();
  ICEBERG_ASSIGN_OR_RAISE(auto connect_timeout_ms, config.ConnectTimeoutMs());
  ICEBERG_ASSIGN_OR_RAISE(auto socket_timeout_ms, config.SocketTimeoutMs());

  auto socket =
      std::make_shared<apache::thrift::transport::TSocket>(endpoint.host, endpoint.port);
  socket->setConnTimeout(connect_timeout_ms);
  socket->setRecvTimeout(socket_timeout_ms);
  socket->setSendTimeout(socket_timeout_ms);

  std::shared_ptr<apache::thrift::transport::TTransport> transport;
  switch (transport_mode) {
    case HiveThriftTransport::kBuffered:
      transport = std::make_shared<apache::thrift::transport::TBufferedTransport>(socket);
      break;
    case HiveThriftTransport::kFramed:
      transport = std::make_shared<apache::thrift::transport::TFramedTransport>(socket);
      break;
  }

  auto protocol = std::make_shared<apache::thrift::protocol::TBinaryProtocol>(transport);
  auto client =
      std::make_unique<Apache::Hadoop::Hive::ThriftHiveMetastoreClient>(protocol);

  try {
    transport->open();
  } catch (const apache::thrift::transport::TTransportException& e) {
    return IOError("Failed to connect to HMS at {}:{} : {}", endpoint.host, endpoint.port,
                   e.what());
  } catch (const apache::thrift::TException& e) {
    return IOError("Thrift error contacting HMS at {}:{} : {}", endpoint.host,
                   endpoint.port, e.what());
  }

  auto impl = std::make_unique<HmsClient::Impl>();
  impl->socket = std::move(socket);
  impl->transport = std::move(transport);
  impl->protocol = std::move(protocol);
  impl->client = std::move(client);
  return std::unique_ptr<HmsClient>(new HmsClient(std::move(impl)));
}

}  // namespace iceberg::hive
