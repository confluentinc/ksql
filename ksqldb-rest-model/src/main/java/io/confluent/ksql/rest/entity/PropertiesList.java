/*
 * Copyright 2018 Confluent Inc.
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

import static io.confluent.ksql.util.KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_NESTED_ERROR_HANDLING_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TIMESTAMP_THROW_ON_INVALID;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.COMMIT_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_TASK_IDLE_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.POLL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.PROCESSING_GUARANTEE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TASK_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.WINDOW_SIZE_MS_CONFIG;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.KsqlConfigResolver;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {

  /**
   * The set of query-level properties that can be configured via the `SET` command. They can also
   * use the `ALTER SYSTEM` command to set a default value for queries without an explicit override.
   * NOTE: IF YOU ADD A NEW CONFIG AND WANT IT TO BE CONFIGURABLE PER-QUERY YOU MUST ADD IT HERE.
   */
  @SuppressWarnings("deprecation")
  public static final Set<String> QueryLevelProperties = ImmutableSet.of(
      AUTO_OFFSET_RESET_CONFIG,
      BUFFERED_RECORDS_PER_PARTITION_CONFIG,
      CACHE_MAX_BYTES_BUFFERING_CONFIG,
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG,
      KSQL_STRING_CASE_CONFIG_TOGGLE,
      KSQL_NESTED_ERROR_HANDLING_CONFIG,
      KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
      KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG,
      KSQL_QUERY_PULL_TABLE_SCAN_ENABLED,
      KSQL_TIMESTAMP_THROW_ON_INVALID,
      MAX_TASK_IDLE_MS_CONFIG,
      TASK_TIMEOUT_MS_CONFIG
  );

  /**
   * The set of system properties that can be changed via the `ALTER SYSTEM` command.
   * We use this "allow list" for security reasons.
   * (Independent of LD.)
   */
  public static final Set<String> MutableSystemProperties = ImmutableSet.of(
      MAX_POLL_RECORDS_CONFIG,
      MAX_POLL_INTERVAL_MS_CONFIG,
      SESSION_TIMEOUT_MS_CONFIG,
      HEARTBEAT_INTERVAL_MS_CONFIG,
      FETCH_MIN_BYTES_CONFIG,
      FETCH_MAX_BYTES_CONFIG,
      FETCH_MAX_WAIT_MS_CONFIG,
      METADATA_MAX_AGE_CONFIG,
      MAX_PARTITION_FETCH_BYTES_CONFIG,
      BATCH_SIZE_CONFIG,
      LINGER_MS_CONFIG,
      DELIVERY_TIMEOUT_MS_CONFIG,
      MAX_REQUEST_SIZE_CONFIG,
      MAX_BLOCK_MS_CONFIG,
      BUFFER_MEMORY_CONFIG,
      COMPRESSION_TYPE_CONFIG,
      ACCEPTABLE_RECOVERY_LAG_CONFIG,
      COMMIT_INTERVAL_MS_CONFIG,
      MAX_WARMUP_REPLICAS_CONFIG,
      NUM_STANDBY_REPLICAS_CONFIG,
      POLL_MS_CONFIG,
      PROBING_REBALANCE_INTERVAL_MS_CONFIG,
      PROCESSING_GUARANTEE_CONFIG,
      WINDOW_SIZE_MS_CONFIG
  );

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Property {
    private final String name;
    private final String scope;
    private final String value;

    @JsonProperty("editable")
    private final boolean editable;

    @JsonProperty("level")
    private final String level;

    @JsonCreator
    public Property(
        @JsonProperty("name") final String name,
        @JsonProperty("scope") final String scope,
        @JsonProperty("value") final String value
    ) {
      this.name = name;
      this.scope = scope;
      this.value = value;
      this.editable = Property.isEditable(name);
      this.level = PropertiesList.QueryLevelProperties.contains(name) ? "QUERY" : "SERVER";
    }

    @SuppressWarnings("checkstyle:MissingJavadocMethod")
    public static boolean isEditable(final String propertyName) {
      final KsqlConfigResolver resolver = new KsqlConfigResolver();
      final Optional<ConfigItem> resolvedItem = resolver.resolve(propertyName, false);

      return resolvedItem.isPresent()
          && (PropertiesList.MutableSystemProperties.contains(resolvedItem.get().getPropertyName())
          || PropertiesList.QueryLevelProperties.contains(resolvedItem.get().getPropertyName()));
    }

    public String getLevel() {
      return level;
    }

    public boolean getEditable() {
      return editable;
    }

    public String getName() {
      return name;
    }

    public String getScope() {
      return scope;
    }

    public String getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object object) {
      if (this == object) {
        return true;
      }
      if (object == null || getClass() != object.getClass()) {
        return false;
      }
      final Property that = (Property) object;
      return Objects.equals(name, that.name)
          && Objects.equals(scope, that.scope)
          && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, scope, value);
    }

    @Override
    public String toString() {
      return "Property{"
          + "name='" + name + '\''
          + ", scope='" + scope + '\''
          + ", value='" + value + '\''
          + '}';
    }
  }

  private final ImmutableList<Property> properties;
  private final ImmutableList<String> overwrittenProperties;
  private final ImmutableList<String> defaultProperties;

  @JsonCreator
  public PropertiesList(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("properties") final List<Property> properties,
      @JsonProperty("overwrittenProperties") final List<String> overwrittenProperties,
      @JsonProperty("defaultProperties") final List<String> defaultProperties
  ) {
    super(statementText);
    this.properties = ImmutableList.copyOf(properties);
    this.overwrittenProperties = ImmutableList.copyOf(overwrittenProperties);
    this.defaultProperties = ImmutableList.copyOf(defaultProperties);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "properties is ImmutableList")
  public List<Property> getProperties() {
    return properties;
  }

  @SuppressFBWarnings(
      value = "EI_EXPOSE_REP",
      justification = "overwrittenProperties is ImmutableList"
  )
  public List<String> getOverwrittenProperties() {
    return overwrittenProperties;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "defaultProperties is ImmutableList")
  public List<String> getDefaultProperties() {
    return defaultProperties;
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof PropertiesList
        && Objects.equals(properties, ((PropertiesList)o).properties)
        && Objects.equals(overwrittenProperties, ((PropertiesList)o).overwrittenProperties)
        && Objects.equals(defaultProperties, ((PropertiesList)o).defaultProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(properties, overwrittenProperties, defaultProperties);
  }
}
