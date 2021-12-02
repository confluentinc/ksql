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


import static io.confluent.ksql.util.KsqlConfig.CONNECT_URL_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.FAIL_ON_DESERIALIZATION_ERROR_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_AUTO;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_ON;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_COLLECT_UDF_METRICS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CUSTOM_METRICS_TAGS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_UDFS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ERROR_CLASSIFIER_REGEX_PREFIX;
import static io.confluent.ksql.util.KsqlConfig.KSQL_EXT_DIR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HIDDEN_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_LAMBDAS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_NESTED_ERROR_HANDLING_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_ERROR_MAX_QUEUE_SIZE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_METRICS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_RANGE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_MAX_MS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_READONLY_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STRING_CASE_CONFIG_TOGGLE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_TIMESTAMP_THROW_ON_INVALID;
import static io.confluent.ksql.util.KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE;
import static io.confluent.ksql.util.KsqlConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PropertiesList extends KsqlEntity {
  private static final List<String> InternalPropertiesList = ImmutableList.of(
      CONNECT_URL_PROPERTY,
      CONNECT_WORKER_CONFIG_FILE_PROPERTY,
      KSQL_ACCESS_VALIDATOR_AUTO,
      KSQL_ACCESS_VALIDATOR_OFF,
      KSQL_ACCESS_VALIDATOR_ON,
      KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
      KSQL_AUTH_CACHE_EXPIRY_TIME_SECS,
      KSQL_AUTH_CACHE_MAX_ENTRIES,
      KSQL_COLLECT_UDF_METRICS,
      KSQL_CREATE_OR_REPLACE_ENABLED,
      KSQL_CUSTOM_METRICS_TAGS,
      KSQL_ENABLE_ACCESS_VALIDATOR,
      KSQL_ENABLE_UDFS,
      KSQL_ERROR_CLASSIFIER_REGEX_PREFIX,
      KSQL_EXT_DIR,
      KSQL_HIDDEN_TOPICS_CONFIG,
      KSQL_INSERT_INTO_VALUES_ENABLED,
      KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY,
      KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY,
      KSQL_LAMBDAS_ENABLED,
      KSQL_METASTORE_BACKUP_LOCATION,
      KSQL_PROPERTIES_OVERRIDES_DENYLIST,
      KSQL_PULL_QUERIES_ENABLE_CONFIG,
      KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE,
      KSQL_QUERYANONYMIZER_ENABLED,
      KSQL_QUERY_PULL_ENABLE_STANDBY_READS,
      KSQL_QUERY_PULL_INTERPRETER_ENABLED,
      KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG,
      KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG,
      KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
      KSQL_QUERY_PULL_MAX_QPS_CONFIG,
      KSQL_QUERY_PULL_METRICS_ENABLED,
      KSQL_QUERY_PULL_RANGE_SCAN_ENABLED,
      KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG,
      KSQL_QUERY_PULL_TABLE_SCAN_ENABLED,
      KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG,
      KSQL_QUERY_PUSH_V2_ENABLED,
      KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED,
      KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
      KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY,
      KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED,
      KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS,
      KSQL_QUERY_STREAM_PULL_QUERY_ENABLED,
      KSQL_READONLY_TOPICS_CONFIG,
      KSQL_ROWPARTITION_ROWOFFSET_ENABLED,
      KSQL_SECURITY_EXTENSION_CLASS,
      KSQL_SERVICE_ID_CONFIG,
      KSQL_SHARED_RUNTIME_ENABLED,
      KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG,
      KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED,
      KSQL_SUPPRESS_BUFFER_SIZE_BYTES,
      KSQL_SUPPRESS_ENABLED,
      KSQL_UDF_SECURITY_MANAGER_ENABLED,
      KSQL_VARIABLE_SUBSTITUTION_ENABLE,
      METRIC_REPORTER_CLASSES_CONFIG,
      SCHEMA_REGISTRY_URL_PROPERTY,
      KSQL_HEADERS_COLUMNS_ENABLED
  );

  private static final List<String> QueryLevelPropertyList = ImmutableList.of(
      KSQL_STRING_CASE_CONFIG_TOGGLE,
      KSQL_NESTED_ERROR_HANDLING_CONFIG,
      KSQL_QUERY_ERROR_MAX_QUEUE_SIZE,
      KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
      KSQL_QUERY_RETRY_BACKOFF_MAX_MS,
      KSQL_TIMESTAMP_THROW_ON_INVALID,
      FAIL_ON_DESERIALIZATION_ERROR_CONFIG
  );

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Property {
    private final String name;
    private final String scope;
    private final String value;

    @JsonProperty("internal")
    private final boolean internal;

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
      this.internal = PropertiesList.InternalPropertiesList.contains(name);
      this.level = PropertiesList.QueryLevelPropertyList.contains(name) ? "QUERY" : "SERVER";
    }

    public String getLevel() {
      return level;
    }

    public boolean getInternal() {
      return internal;
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
