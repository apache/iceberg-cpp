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

#include "client.h"

#include <cpr/cpr.h>

namespace iceberg::catalog::rest {

// PIMPL (Pointer to Implementation) 模式的实现
struct HttpClient::Impl {
  cpr::Url base_uri_;
  cpr::Header common_headers_;

  Impl(const std::string& base_uri, const HttpHeaders& common_headers)
      : base_uri_{base_uri},
        common_headers_{common_headers.begin(), common_headers.end()} {}

  // 这个模板函数是 Rust 版本 `query` 和 `execute` 方法的核心逻辑的 C++ 对等实现。
  // - `SuccessType` 对应 Rust 的 `R` (如果是 void，则对应 `execute` 的 `()`)。
  // - `SuccessCode` 对应 Rust 的 `SUCCESS_CODE`。
  template <typename SuccessType, int SuccessCode>
  SuccessType process_response(const cpr::Response& resp) {
    if (resp.status_code == SuccessCode) {
      // 对应 Rust 的 `if resp.status().as_u16() == SUCCESS_CODE` 分支

      // `if constexpr` 允许我们在编译时根据 SuccessType 是否为 void
      // 来选择不同的代码路径。 这完美地模拟了 `query` (返回 body) 和 `execute` (返回
      // void) 的区别。
      if constexpr (std::is_same_v<SuccessType, void>) {
        return;  // 成功且不需要返回 body
      } else {
        // 尝试解析成功的响应体
        nlohmann::json result = nlohmann::json::parse(resp.text, false);
        if (result.is_discarded()) {
          // 对应 Rust 中解析成功响应体失败的错误路径
          throw ResponseParseException("Failed to parse successful response from server!",
                                       resp.text);
        }
        return result;
      }
    } else {
      // 对应 Rust 的 `else` 分支，处理错误响应
      nlohmann::json error_payload = nlohmann::json::parse(resp.text, false);
      if (error_payload.is_discarded()) {
        // 对应 Rust 中解析错误响应体失败的错误路径
        throw ResponseParseException("Failed to parse error response from server!",
                                     resp.text);
      }
      // 抛出包含详细信息的异常，对应 `Err(e.into())`
      throw ServerErrorException(resp.status_code, resp.text, error_payload);
    }
  }
};

HttpClient::HttpClient(const std::string& base_uri, const HttpHeaders& common_headers)
    : pimpl_(std::make_unique<Impl>(base_uri, common_headers)) {}

HttpClient::~HttpClient() = default;

nlohmann::json HttpClient::Get(const std::string& path, const HttpHeaders& headers) {
  cpr::Header final_headers = pimpl_->common_headers_;
  final_headers.insert(headers.begin(), headers.end());

  cpr::Response r = cpr::Get(pimpl_->base_uri_ + cpr::Url{path}, final_headers);

  // 调用通用处理逻辑，期望返回 json，成功状态码为 200 (OK)
  return pimpl_->process_response<nlohmann::json, 200>(r);
}

nlohmann::json HttpClient::Post(const std::string& path, const HttpHeaders& headers,
                                const nlohmann::json& body) {
  cpr::Header final_headers = pimpl_->common_headers_;
  final_headers.insert(headers.begin(), headers.end());
  // 确保 Content-Type 设置为 application/json
  if (final_headers.find("Content-Type") == final_headers.end()) {
    final_headers["Content-Type"] = "application/json";
  }

  cpr::Response r = cpr::Post(pimpl_->base_uri_ + cpr::Url{path}, cpr::Body{body.dump()},
                              final_headers);

  // 调用通用处理逻辑，期望返回 json，成功状态码通常为 201 (Created) 或 200 (OK)，这里以
  // 200 为例
  return pimpl_->process_response<nlohmann::json, 200>(r);
}

void HttpClient::Delete(const std::string& path, const HttpHeaders& headers) {
  cpr::Header final_headers = pimpl_->common_headers_;
  final_headers.insert(headers.begin(), headers.end());

  cpr::Response r = cpr::Delete(pimpl_->base_uri_ + cpr::Url{path}, final_headers);

  // 调用通用处理逻辑，期望返回 void，成功状态码为 204 (No Content)
  return pimpl_->process_response<void, 204>(r);
}

}  // namespace iceberg::catalog::rest
