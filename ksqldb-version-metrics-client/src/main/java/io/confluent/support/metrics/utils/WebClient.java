/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.support.metrics.utils;

import io.confluent.support.metrics.submitters.ResponseHandler;
import java.io.IOException;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.entity.mime.HttpMultipartMode;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.routing.DefaultProxyRoutePlanner;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WebClient {

  private static final Logger log = LoggerFactory
      .getLogger(io.confluent.support.metrics.utils.WebClient.class);
  private static final int REQUEST_TIMEOUT_MS = 2000;
  public static final int DEFAULT_STATUS_CODE = HttpStatus.SC_BAD_GATEWAY;

  private WebClient() {
    throw new IllegalStateException("Utility class should not be instantiated");
  }

  /**
   * Sends a POST request to a web server
   * This method requires a pre-configured http client instance
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @param proxy a http (passive) proxy
   * @param httpClient http client instance configured by caller
   * @return an HTTP Status code
   * @see #send(String, byte[], HttpPost, ResponseHandler)
   */
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "checkstyle:FinalParameters"})
  protected static int send(
      final String customerId, final byte[] bytes, final HttpPost httpPost, final HttpHost proxy,
      CloseableHttpClient httpClient, final ResponseHandler responseHandler
  ) {
    int statusCode = DEFAULT_STATUS_CODE;
    if (bytes != null && bytes.length > 0 && httpPost != null && customerId != null) {

      // add the body to the request
      final MultipartEntityBuilder builder = MultipartEntityBuilder.create();
      builder.setMode(HttpMultipartMode.LEGACY);
      builder.addTextBody("cid", customerId);
      builder.addBinaryBody("file", bytes, ContentType.DEFAULT_BINARY, "filename");
      httpPost.setEntity(builder.build());
      httpPost.addHeader("api-version", "phone-home-v1");

      // set the HTTP config
      RequestConfig config = RequestConfig.custom()
          .setConnectTimeout(Timeout.ofMilliseconds(REQUEST_TIMEOUT_MS))
          .setConnectionRequestTimeout(Timeout.ofMilliseconds(REQUEST_TIMEOUT_MS))
          .setResponseTimeout(Timeout.ofMilliseconds(REQUEST_TIMEOUT_MS))
          .build();

      CloseableHttpResponse response = null;

      try {
        if (proxy != null) {
          log.debug("setting proxy to {}", proxy);
          config = RequestConfig.copy(config).setProxy(proxy).build();
          httpPost.setConfig(config);
          final DefaultProxyRoutePlanner routePlanner = new DefaultProxyRoutePlanner(proxy);
          if (httpClient == null) {
            httpClient = HttpClientBuilder
                .create()
                .setRoutePlanner(routePlanner)
                .setDefaultRequestConfig(config)
                .build();
          }
        } else {
          if (httpClient == null) {
            httpClient = HttpClientBuilder.create().setDefaultRequestConfig(config).build();
          }
        }

        response = httpClient.execute(httpPost);
        if (responseHandler != null) {
          responseHandler.handle(response);
        }

        // send request
        log.debug("POST request returned {}", new StatusLine(response).toString());
        statusCode = response.getCode();
      } catch (IOException e) {
        log.error("Could not submit metrics to Confluent: {}", e.getMessage());
      } finally {
        if (httpClient != null) {
          try {
            httpClient.close();
          } catch (IOException e) {
            log.warn("could not close http client", e);
          }
        }
        if (response != null) {
          try {
            response.close();
          } catch (IOException e) {
            log.warn("could not close http response", e);
          }
        }
      }
    } else {
      statusCode = HttpStatus.SC_BAD_REQUEST;
    }
    return statusCode;
  }

  /**
   * Sends a POST request to a web server via a HTTP proxy
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @param proxy a http (passive) proxy
   * @return an HTTP Status code
   */
  public static int send(
      final String customerId,
      final byte[] bytes,
      final HttpPost httpPost,
      final HttpHost proxy,
      final ResponseHandler responseHandler
  ) {
    return send(customerId, bytes, httpPost, proxy, null, responseHandler);
  }

  /**
   * Sends a POST request to a web server
   *
   * @param customerId customer Id on behalf of which the request is sent
   * @param bytes request payload
   * @param httpPost A POST request structure
   * @return an HTTP Status code
   */
  public static int send(
      final String customerId,
      final byte[] bytes,
      final HttpPost httpPost,
      final ResponseHandler responseHandler
  ) {
    return send(customerId, bytes, httpPost, null, responseHandler);
  }

}
