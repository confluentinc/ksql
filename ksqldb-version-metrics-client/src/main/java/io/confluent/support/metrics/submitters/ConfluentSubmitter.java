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

package io.confluent.support.metrics.submitters;

import io.confluent.support.metrics.BaseSupportConfig;
import io.confluent.support.metrics.utils.StringUtils;
import io.confluent.support.metrics.utils.WebClient;
import java.net.URI;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfluentSubmitter implements Submitter {

  private static final Logger log = LogManager
      .getLogger(io.confluent.support.metrics.submitters.ConfluentSubmitter.class);

  private final String customerId;
  private final String endpointHttp;
  private final String endpointHttps;
  private HttpHost proxy;
  private ResponseHandler responseHandler;

  public String getProxy() {
    return proxy == null ? null : proxy.toString();
  }

  public void setProxy(final String name, final int port, final String scheme) {
    this.proxy = new HttpHost(scheme, name, port);
  }

  /**
   * Class that decides how to send data to Confluent.
   *
   * @param endpointHttp HTTP endpoint for the Confluent support service. Can be null.
   * @param endpointHttps HTTPS endpoint for the Confluent support service. Can be null.
   */
  public ConfluentSubmitter(final String customerId, final String endpointHttp,
      final String endpointHttps) {
    this(customerId, endpointHttp, endpointHttps, null, null);
  }

  /**
   * Constructor for phone-home clients which use ConfluentSubmitter directly instead of
   * BaseMetricsReporter and, as a result, don't need PhoneHomeConfig.
   * Sets customerId = "anonymous"
   */
  public ConfluentSubmitter(final String componentId, final ResponseHandler responseHandler) {
    this(BaseSupportConfig.CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT, componentId, responseHandler);
  }

  /**
   * Also constructor for phone-home clients which use ConfluentSubmitter directly, but lets set
   * customer ID.
   * Common use-case is customerId = "anonymous" (BaseSupportConfig
   * .CONFLUENT_SUPPORT_CUSTOMER_ID_DEFAULT) for production code, and "c0"
   * (BaseSupportConfig.CONFLUENT_SUPPORT_TEST_ID_DEFAULT) for internal testing. E.g. if we don't
   * want to include any internal testing data to aggregations and analysis we do on S3 data that
   * the server stores.
   * If customerID is "anonymous", then the endpoint path ends in "/anon", and if customerId is
   * "c0", then the endpoint path ends in "/test". CustomerId is also included in the body of
   * the phone-home ping.
   */
  public ConfluentSubmitter(
      final String customerId,
      final String componentId,
      final ResponseHandler responseHandler
  ) {
    this(customerId,
         BaseSupportConfig.getEndpoint(false, customerId, componentId),
         BaseSupportConfig.getEndpoint(true, customerId, componentId),
         BaseSupportConfig.CONFLUENT_SUPPORT_PROXY_DEFAULT,
         responseHandler);
  }

  public ConfluentSubmitter(
      final String customerId,
      final String endpointHttp,
      final String endpointHttps,
      final String proxyUriString,
      final ResponseHandler responseHandler
  ) {

    if (StringUtils.isNullOrEmpty(endpointHttp) && StringUtils.isNullOrEmpty(endpointHttps)) {
      throw new IllegalArgumentException("must specify endpoints");
    }
    if (!StringUtils.isNullOrEmpty(endpointHttp)) {
      if (!endpointHttp.startsWith("http://")) {
        throw new IllegalArgumentException("invalid HTTP endpoint " + endpointHttp);
      }
    }
    if (!StringUtils.isNullOrEmpty(endpointHttps)) {
      if (!endpointHttps.startsWith("https://")) {
        throw new IllegalArgumentException("invalid HTTPS endpoint " + endpointHttps);
      }
    }
    if (!BaseSupportConfig.isSyntacticallyCorrectCustomerId(customerId)) {
      throw new IllegalArgumentException("invalid customer ID "  + customerId);
    }
    this.endpointHttp = endpointHttp;
    this.endpointHttps = endpointHttps;
    this.customerId = customerId;
    this.responseHandler = responseHandler;

    if (!StringUtils.isNullOrEmpty(proxyUriString)) {
      final URI proxyUri = URI.create(proxyUriString);
      this.setProxy(proxyUri.getHost(), proxyUri.getPort(), proxyUri.getScheme());
    }
  }

  /**
   * Submits metrics to Confluent via the Internet.  Ignores null or empty inputs.
   */
  @Override
  public void submit(final byte[] bytes) {
    if (bytes != null && bytes.length > 0) {
      if (isSecureEndpointEnabled()) {
        if (!submittedSuccessfully(sendSecurely(bytes))) {
          if (isInsecureEndpointEnabled()) {
            log.error(
                "Failed to submit metrics via secure endpoint, falling back to insecure endpoint"
            );
            submitToInsecureEndpoint(bytes);
          } else {
            log.error(
                "Failed to submit metrics via secure endpoint={} -- giving up",
                endpointHttps
            );
          }
        } else {
          log.info("Successfully submitted metrics to Confluent via secure endpoint");
        }
      } else {
        if (isInsecureEndpointEnabled()) {
          submitToInsecureEndpoint(bytes);
        } else {
          log.error("Metrics will not be submitted because all endpoints are disabled");
        }
      }
    } else {
      log.error("Could not submit metrics to Confluent (metrics data missing)");
    }
  }

  private void submitToInsecureEndpoint(final byte[] encodedMetricsRecord) {
    final int statusCode = sendInsecurely(encodedMetricsRecord);
    if (submittedSuccessfully(statusCode)) {
      log.info("Successfully submitted metrics to Confluent via insecure endpoint");
    } else {
      log.error(
          "Failed to submit metrics to Confluent via insecure endpoint={} -- giving up",
          endpointHttp
      );
    }
  }

  private boolean isSecureEndpointEnabled() {
    return !endpointHttps.isEmpty();
  }

  private boolean isInsecureEndpointEnabled() {
    return !endpointHttp.isEmpty();
  }

  /**
   * Getters for testing
   */
  String getEndpointHttp() {
    return endpointHttp;
  }

  String getEndpointHttps() {
    return endpointHttps;
  }

  private boolean submittedSuccessfully(final int statusCode) {
    return statusCode == HttpStatus.SC_OK;
  }

  private int sendSecurely(final byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHttps);
  }

  private int sendInsecurely(final byte[] encodedMetricsRecord) {
    return send(encodedMetricsRecord, endpointHttp);
  }

  private int send(final byte[] encodedMetricsRecord, final String endpoint) {
    return WebClient
        .send(customerId, encodedMetricsRecord, new HttpPost(endpoint), proxy, responseHandler);
  }
}
