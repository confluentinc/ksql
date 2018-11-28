/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.confluent.ksql.config.PropertyParser;
import io.confluent.ksql.rest.client.properties.LocalPropertyParser;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KsqlRequest {
  private static final PropertyParser PROPERTY_PARSER = new LocalPropertyParser();

  private final String ksql;
  private final Map<String, Object> streamsProperties;
  private final Optional<Long> commandSequenceNumber;

  @JsonCreator
  public KsqlRequest(
      @JsonProperty("ksql") final String ksql,
      @JsonProperty("streamsProperties") final Map<String, Object> streamsProperties,
      @JsonProperty("commandSequenceNumber") final Long commandSequenceNumber
  ) {
    this.ksql = ksql == null ? "" : ksql;
    this.streamsProperties = streamsProperties == null
        ? Collections.emptyMap()
        : Collections.unmodifiableMap(new HashMap<>(streamsProperties));
    this.commandSequenceNumber = Optional.ofNullable(commandSequenceNumber);
  }

  public String getKsql() {
    return ksql;
  }

  public Map<String, Object> getStreamsProperties() {
    return coerceTypes(streamsProperties);
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
    return Objects.equals(getKsql(), that.getKsql())
        && Objects.equals(getStreamsProperties(), that.getStreamsProperties())
        && Objects.equals(getCommandSequenceNumber(), that.getCommandSequenceNumber());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKsql(), getStreamsProperties(), getCommandSequenceNumber());
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
      final String stringValue = value == null ? null : String.valueOf(value);
      return PROPERTY_PARSER.parse(key, stringValue);
    } catch (final Exception e) {
      throw new KsqlException("Failed to set '" + key + "' to '" + value + "'", e);
    }
  }
}
