/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.connect;

import io.confluent.ksql.security.KsqlPrincipal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

/**
 * Custom extension to allow for more fine-grained control of connector requests
 * made by ksqlDB.
 */
public interface ConnectRequestHeadersExtension {

  /**
   * Returns whether to use the custom auth header returned from
   * {@link ConnectRequestHeadersExtension#getAuthHeader(List)} instead of
   * ksqlDB's default auth header for ksqlDB connector requests.
   *
   * <p>If {@code true}, then the auth header configured by this extension
   * takes precedence over any other custom auth configurations for ksqlDB
   * connector requests, including basic auth configured in ksqlDB server
   * properties.
   *
   * <p>Set this to {@code false} in order to use this
   * {@code ConnectRequestHeadersExtension} for additional custom headers
   * (via {@link ConnectRequestHeadersExtension#getHeaders(Optional)}) only, without
   * impacting ksqlDB's default behavior for the auth header.
   *
   * @return whether to use the custom auth header returned from
   *         {@link ConnectRequestHeadersExtension#getAuthHeader(List)} instead of
   *         ksqlDB's default auth header for ksqlDB connector requests
   */
  default boolean shouldUseCustomAuthHeader() {
    return true;
  }

  /**
   * Specifies the custom auth header value to be sent with connector requests made
   * by ksqlDB. If empty, no auth header will be sent. This replaces the default
   * auth header sent by ksqlDB with connector requests by default. Only applicable if
   * {@link ConnectRequestHeadersExtension#shouldUseCustomAuthHeader()} is {@code true}.
   *
   * @param incomingHeaders request headers from the incoming ksql request that
   *                        triggered this outgoing connector request from ksqlDB
   * @return custom auth header value to be sent with connector requests made by ksqlDB
   */
  Optional<String> getAuthHeader(List<Entry<String, String>> incomingHeaders);

  /**
   * Creates custom headers to be included with all connector requests made by ksqlDB.
   * Note that this is in addition to any headers already sent by ksqlDB by default,
   * such as those required for authenticating with Connect. The custom headers are added
   * to the request in addition to, and after, ksqlDB's default headers.
   *
   * @param userPrincipal principal associated with the user who submitted the connector
   *                      request to ksqlDB, if present (i.e., if user authentication
   *                      is enabled)
   * @return additional headers to be included with connector requests made by ksqlDB
   */
  default Map<String, String> getHeaders(Optional<KsqlPrincipal> userPrincipal) {
    return Collections.emptyMap();
  }

}
