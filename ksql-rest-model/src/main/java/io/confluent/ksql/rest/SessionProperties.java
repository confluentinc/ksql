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
import java.util.Map;
import java.util.Objects;

public class SessionProperties {
  
  private final Map<String, Object> mutableScopedProperties;
  private final KsqlHostInfo ksqlHostInfo;
  private final URL localUrl;

  public SessionProperties(
          final Map<String, Object> mutableScopedProperties,
          final KsqlHostInfo ksqlHostInfo,
          final URL localUrl) {
    this.mutableScopedProperties = 
        Objects.requireNonNull(mutableScopedProperties, "mutableScopedProperties");
    this.ksqlHostInfo = Objects.requireNonNull(ksqlHostInfo, "ksqlHostInfo");
    this.localUrl = Objects.requireNonNull(localUrl, "localUrl");
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
}
