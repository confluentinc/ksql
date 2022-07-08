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
import java.util.Map;
import java.util.Optional;

/**
 * Custom extension to allow for more fine-grained control of connector requests
 * made by ksqlDB.
 */
public interface ConnectRequestHeadersExtension {

  /**
   * Creates custom headers to be included with all connector requests made by ksqlDB.
   * Note that this is in addition to any headers already sent by ksqlDB by default,
   * such as those required for authenticating with Connect.
   *
   * @param userPrincipal principal associated with the user who submitted the connector
   *                      request to ksqlDB, if present (i.e., if user authentication
   *                      is enabled)
   * @return additional headers to be included with connector requests made by ksqlDB
   */
  Map<String, String> getHeaders(Optional<KsqlPrincipal> userPrincipal);

}
