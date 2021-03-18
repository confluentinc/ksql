/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest;

import io.confluent.ksql.util.KsqlHostInfo;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Wraps the incoming {@link io.confluent.ksql.rest.entity.KsqlRequest} streamsProperties
 * in a object withthe {@link KsqlHostInfo} and URL of the server that handles the request.
 * This should be created in the Rest Resource that receives the request.
 */
public class SessionProperties {
  
  private final Map<String, Object> mutableScopedProperties;
  private final KsqlHostInfo ksqlHostInfo;
  private final URL localUrl;
  private final boolean internalRequest;
  private final Map<String, String> sessionVariables;

  /**
   * @param mutableScopedProperties   The streamsProperties of the incoming request
   * @param ksqlHostInfo              The ksqlHostInfo of the server that handles the request
   * @param localUrl                  The url of the server that handles the request
   * @param internalRequest           Flag indicating if request is from within the KSQL cluster
   * @param sessionVariables          Initial session variables
   */
  public SessionProperties(
      final Map<String, Object> mutableScopedProperties,
      final KsqlHostInfo ksqlHostInfo,
      final URL localUrl,
      final boolean internalRequest,
      final Map<String, Object> sessionVariables
  ) {
    this(mutableScopedProperties, ksqlHostInfo, localUrl, internalRequest);
    if (sessionVariables != null) {
      this.sessionVariables.putAll(sessionVariables.entrySet().stream()
          .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().toString())));
    }
  }

  /**
   * @param mutableScopedProperties   The streamsProperties of the incoming request
   * @param ksqlHostInfo              The ksqlHostInfo of the server that handles the request 
   * @param localUrl                  The url of the server that handles the request
   * @param internalRequest           Flag indicating if request is from within the KSQL cluster
   */
  public SessionProperties(
      final Map<String, Object> mutableScopedProperties,
      final KsqlHostInfo ksqlHostInfo,
      final URL localUrl,
      final boolean internalRequest
  ) {
    this.mutableScopedProperties = 
        new HashMap<>(Objects.requireNonNull(mutableScopedProperties, "mutableScopedProperties"));
    this.ksqlHostInfo = Objects.requireNonNull(ksqlHostInfo, "ksqlHostInfo");
    this.localUrl = Objects.requireNonNull(localUrl, "localUrl");
    this.internalRequest = internalRequest;
    this.sessionVariables = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
  }

  public Map<String, Object> getMutableScopedProperties() {
    return mutableScopedProperties;
  }

  public KsqlHostInfo getKsqlHostInfo() {
    return ksqlHostInfo;
  }

  public URL getLocalUrl() {
    return localUrl;
  }

  public boolean getInternalRequest() {
    return internalRequest;
  }

  public Map<String, String> getSessionVariables() {
    return Collections.unmodifiableMap(sessionVariables);
  }

  public void setVariable(final String name, final String value) {
    sessionVariables.put(name, value);
  }

  public void unsetVariable(final String name) {
    sessionVariables.remove(name);
  }
}
