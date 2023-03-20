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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.config.PropertyParser;
import io.confluent.ksql.properties.LocalPropertyParser;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KsqlRequest {
  private static final PropertyParser PROPERTY_PARSER = new LocalPropertyParser();
  private static final Logger LOG = LoggerFactory.getLogger(KsqlRequest.class);

  private final String ksql;
  private final ImmutableMap<String, Object> configOverrides;
  private final ImmutableMap<String, Object> requestProperties;
  private final Optional<Long> commandSequenceNumber;
  @JsonIgnore
  private String maskedKsql;

  @JsonCreator
  public KsqlRequest(
      @JsonProperty("ksql") final String ksql,
      @JsonProperty("streamsProperties") final Map<String, ?> configOverrides,
      @JsonProperty("requestProperties") final Map<String, ?> requestProperties,
      @JsonProperty("commandSequenceNumber") final Long commandSequenceNumber
  ) {
    this.ksql = ksql == null ? "" : ksql;
    this.configOverrides = configOverrides == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(serializeClassValues(configOverrides));
    this.requestProperties = requestProperties == null
        ? ImmutableMap.of()
        : ImmutableMap.copyOf(serializeClassValues(requestProperties));
    this.commandSequenceNumber = Optional.ofNullable(commandSequenceNumber);
  }

  public String getMaskedKsql() {
    Objects.requireNonNull(maskedKsql);
    return maskedKsql;
  }

  @JsonProperty("ksql")
  public String getUnmaskedKsql() {
    return ksql;
  }

  public void setMaskedKsql(final String maskedKsql) {
    this.maskedKsql = Objects.requireNonNull(maskedKsql, "maskedKsql");
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getConfigOverrides() {
    return coerceTypes(configOverrides);
  }

  public Map<String, Object> getRequestProperties() {
    return coerceTypes(requestProperties);
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
        && Objects.equals(commandSequenceNumber, that.commandSequenceNumber);
  }

  @Override
  public int hashCode() {
    return Objects.hash(ksql, configOverrides, requestProperties, commandSequenceNumber);
  }

  @Override
  public String toString() {
    final String sql = Objects.isNull(maskedKsql) ? ksql : maskedKsql;
    if (Objects.isNull(maskedKsql)) {
      LOG.warn("maskedKsql is not set, default to unmasked one for toString which "
          + "may leak sensitive information, If this is seen in a test, it may be expected "
          + "depending on how the KsqlRequest was created. If seen in production, "
          + "this is not expected. Please file a Github issue.");
    }
    return "KsqlRequest{"
        + "ksql='" + sql + '\''
        + ", configOverrides=" + configOverrides
        + ", requestProperties=" + requestProperties
        + ", commandSequenceNumber=" + commandSequenceNumber
        + '}';
  }

  public String toStringWithoutQuery() {
    return "KsqlRequest{"
        + "configOverrides=" + configOverrides
        + ", requestProperties=" + requestProperties
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

  private static Map<String, Object> coerceTypes(final Map<String, Object> streamsProperties) {
    if (streamsProperties == null) {
      return Collections.emptyMap();
    }

    final Map<String, Object> validated = new HashMap<>(streamsProperties.size());
    streamsProperties.forEach((k, v) -> validated.put(k, coerceType(k, v)));
    return validated;
  }

  private static Object coerceType(final String key, final Object value) {
    try {
      final String stringValue = value == null
          ? null
          : value instanceof List
              ? listToString((List<?>) value)
              : String.valueOf(value);

      return PROPERTY_PARSER.parse(key, stringValue);
    } catch (final Exception e) {
      throw new KsqlException("Failed to set '" + key + "' to '" + value + "'", e);
    }
  }

  private static String listToString(final List<?> value) {
    return value.stream()
        .map(e -> e == null ? null : e.toString())
        .collect(Collectors.joining(","));
  }
}