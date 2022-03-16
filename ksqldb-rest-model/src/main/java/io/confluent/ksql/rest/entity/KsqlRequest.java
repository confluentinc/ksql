/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.properties.PropertiesUtil;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KsqlRequest {
  private final String ksql;
  private final ImmutableMap<String, Object> configOverrides;
  private final ImmutableMap<String, Object> requestProperties;
  private final ImmutableMap<String, Object> sessionVariables;
  private final Optional<Long> commandSequenceNumber;

  public KsqlRequest(
      @JsonProperty("ksql") final String ksql,
      @JsonProperty("streamsProperties") final Map<String, ?> configOverrides,
      @JsonProperty("requestProperties") final Map<String, ?> requestProperties,
      @JsonProperty("commandSequenceNumber") final Long commandSequenceNumber
  ) {
    this(ksql, configOverrides, requestProperties, null, commandSequenceNumber);
  }

  @JsonCreator
  public KsqlRequest(
      @JsonProperty("ksql") final String ksql,
      @JsonProperty("streamsProperties") final Map<String, ?> configOverrides,
      @JsonProperty("requestProperties") final Map<String, ?> requestProperties,
      @JsonProperty("sessionVariables") final Map<String, ?> sessionVariables,
      @JsonProperty("commandSequenceNumber") final Long commandSequenceNumber
  ) {
    this.ksql = ksql == null ? "" : ksql;
    this.configOverrides = configOverrides == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(serializeClassValues(configOverrides));
    this.requestProperties = requestProperties == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(serializeClassValues(requestProperties));
    this.sessionVariables = sessionVariables == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(serializeClassValues(sessionVariables));
    this.commandSequenceNumber = Optional.ofNullable(commandSequenceNumber);
  }

  public String getKsql() {
    return ksql;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getConfigOverrides() {
    return PropertiesUtil.coerceTypes(configOverrides, false);
  }

  public Map<String, Object> getRequestProperties() {
    return PropertiesUtil.coerceTypes(requestProperties, false);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "sessionVariables is ImmutableMap")
  public Map<String, Object> getSessionVariables() {
    return sessionVariables;
  }

  public Optional<Long> getCommandSequenceNumber() {
    return commandSequenceNumber;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof KsqlRequest)) {
      return false;
    }

    final KsqlRequest that = (KsqlRequest) o;
    return Objects.equals(ksql, that.ksql)
        && Objects.equals(configOverrides, that.configOverrides)
        && Objects.equals(requestProperties, that.requestProperties)
        && Objects.equals(sessionVariables, that.sessionVariables)
        && Objects.equals(commandSequenceNumber, that.commandSequenceNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ksql, configOverrides, requestProperties,
        sessionVariables, commandSequenceNumber);
  }

  @Override
  public String toString() {
    return "KsqlRequest{"
        + "ksql='" + ksql + '\''
        + ", configOverrides=" + configOverrides
        + ", requestProperties=" + requestProperties
        + ", sessionVariables=" + sessionVariables
        + ", commandSequenceNumber=" + commandSequenceNumber
        + '}';
  }

  /**
   * Converts all Class references values to their canonical String value.
   * </p>
   * This conversion avoids the JsonMappingException error thrown by Jackson when attempting
   * to serialize the class properties prior to send this KsqlRequest object as part of the HTTP
   * request. The error thrown by Jackson is "Class ... not be found".
   */
  private static Map<String, ?> serializeClassValues(final Map<String, ?> properties) {
    return properties.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, kv -> {
          if (kv.getValue() instanceof Class) {
            return ((Class)kv.getValue()).getCanonicalName();
          }

          return kv.getValue();
        }));
  }
}