/**
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

package io.confluent.ksql.util;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class KsqlConfig extends AbstractConfig implements Cloneable {

  public static final String KSQL_CONFIG_PREPERTY_PREFIX = "ksql.";

  public static final String KSQL_TIMESTAMP_COLUMN_INDEX = "ksq.timestamp.column.index";
  public static final String SINK_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";

  public static final String SINK_NUMBER_OF_PARTITIONS = "PARTITIONS";
  public static final String SINK_NUMBER_OF_PARTITIONS_PROPERTY = "ksql.sink.partitions";
  public static final String DEFAULT_SINK_NUMBER_OF_PARTITIONS = "ksql.sink.partitions.default";

  public static final String SINK_NUMBER_OF_REPLICATIONS = "REPLICATIONS";
  public static final String SINK_NUMBER_OF_REPLICATIONS_PROPERTY = "ksql.sink.replications";
  public static final String DEFAULT_SINK_NUMBER_OF_REPLICATIONS = "ksql.sink.replications.default";

  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION";
  public static final String SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_PROPERTY =
      "ksql.sink.window.change.log.additional.retention";
  public static final String DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION =
      "ksql.sink.window.change.log.additional.retention.default";

  public static final String STREAM_INTERNAL_CHANGELOG_TOPIC_SUFFIX = "-changelog";

  public static final String STREAM_INTERNAL_REPARTITION_TOPIC_SUFFIX = "-repartition";

  public static final String FAIL_ON_DESERIALIZATION_ERROR_CONFIG = "fail.on.deserialization.error";

  public static final String
      KSQL_SERVICE_ID_CONFIG = "ksql.service.id";
  public static final ConfigDef.Type
      KSQL_SERVICE_ID_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_SERVICE_ID_DEFAULT = "ksql_";
  public static final ConfigDef.Importance
      KSQL_SERVICE_ID_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_SERVICE_ID_DOC =
      "Indicates the ID of the ksql service. It will be used as prefix for all KSQL queries in "
      + "this service.";

  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG = "ksql.persistent.prefix";
  public static final ConfigDef.Type
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT = "query_";
  public static final ConfigDef.Importance
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_PERSISTENT_QUERY_NAME_PREFIX_DOC =
      "Second part of the prefix for persitent queries.";

  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG = "ksql.transient.prefix";
  public static final ConfigDef.Type
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT = "transient_";
  public static final ConfigDef.Importance
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_TRANSIENT_QUERY_NAME_PREFIX_DOC =
      "Second part of the prefix for transient queries.";

  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG = "ksql.statestore.suffix";
  public static final ConfigDef.Type
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_TYPE = ConfigDef.Type.STRING;
  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT = "transient_";
  public static final ConfigDef.Importance
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  public static final String
      KSQL_TABLE_STATESTORE_NAME_SUFFIX_DOC =
      "Suffix for state store names in Tables.";

  public int defaultSinkNumberOfPartitions = 4;
  public short defaultSinkNumberOfReplications = 1;
  // TODO: Find out the best default value.
  public long defaultSinkWindowChangeLogAdditionalRetention = 1000000;

  public String defaultAutoOffsetRestConfig = "latest";
  public long defaultCommitIntervalMsConfig = 2000;
  public long defaultCacheMaxBytesBufferingConfig = 10000000;
  public int defaultNumberOfStreamsThreads = 4;

  Map<String, Object> ksqlConfigProps;
  Map<String, Object> ksqlStreamConfigProps;

  private static final ConfigDef CONFIG_DEF = new ConfigDef();

  public KsqlConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);

    ksqlConfigProps = new HashMap<>();
    ksqlStreamConfigProps = new HashMap<>();
    ksqlConfigProps.put(KSQL_SERVICE_ID_CONFIG, KSQL_SERVICE_ID_DEFAULT);
    ksqlConfigProps.put(KSQL_PERSISTENT_QUERY_NAME_PREFIX_CONFIG, KSQL_PERSISTENT_QUERY_NAME_PREFIX_DEFAULT);
    ksqlConfigProps.put(KSQL_TRANSIENT_QUERY_NAME_PREFIX_CONFIG, KSQL_TRANSIENT_QUERY_NAME_PREFIX_DEFAULT);
    ksqlConfigProps.put(KSQL_TABLE_STATESTORE_NAME_SUFFIX_CONFIG, KSQL_TABLE_STATESTORE_NAME_SUFFIX_DEFAULT);

    if (props.containsKey(DEFAULT_SINK_NUMBER_OF_PARTITIONS)) {
      ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS_PROPERTY,
                          Integer.parseInt(props.get(DEFAULT_SINK_NUMBER_OF_PARTITIONS).toString()));
    } else {
      ksqlConfigProps.put(SINK_NUMBER_OF_PARTITIONS_PROPERTY, defaultSinkNumberOfPartitions);
    }

    if (props.containsKey(DEFAULT_SINK_NUMBER_OF_REPLICATIONS)) {
      ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS_PROPERTY,
                          Short.parseShort(props.get(DEFAULT_SINK_NUMBER_OF_REPLICATIONS).toString()));
    } else {
      ksqlConfigProps.put(SINK_NUMBER_OF_REPLICATIONS_PROPERTY, defaultSinkNumberOfReplications);
    }

    if (props.containsKey(DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION)) {
      ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_PROPERTY,
                          Long.parseLong(props.get(DEFAULT_SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION).toString()));
    } else {
      ksqlConfigProps.put(SINK_WINDOW_CHANGE_LOG_ADDITIONAL_RETENTION_PROPERTY,
                          defaultSinkWindowChangeLogAdditionalRetention);
    }

    ksqlStreamConfigProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, defaultAutoOffsetRestConfig);
    ksqlStreamConfigProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, defaultCommitIntervalMsConfig);
    ksqlStreamConfigProps.put(
        StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, defaultCacheMaxBytesBufferingConfig);
    ksqlStreamConfigProps.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, defaultNumberOfStreamsThreads);

    for (Map.Entry<?, ?> entry : props.entrySet()) {
      final String key = entry.getKey().toString();
      if (key.toLowerCase().startsWith(KSQL_CONFIG_PREPERTY_PREFIX)) {
        ksqlConfigProps.put(key, entry.getValue());
      } else {
        ksqlStreamConfigProps.put(key, entry.getValue());
      }
    }

    final Object fail = props.get(FAIL_ON_DESERIALIZATION_ERROR_CONFIG);
    if (fail == null || !Boolean.parseBoolean(fail.toString())) {
      ksqlStreamConfigProps.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
    }
  }

  public Map<String, Object> getKsqlConfigProps() {
    return ksqlConfigProps;
  }

  public Map<String, Object> getKsqlStreamConfigProps() {
    return ksqlStreamConfigProps;
  }

  public Map<String, Object> getKsqlAdminClientConfigProps() {
    Set<String> adminClientConfigProperties = AdminClientConfig.configNames();
    Map<String, Object> adminClientConfigs = new HashMap<>();
    for (Map.Entry<String, Object> entry: ksqlStreamConfigProps.entrySet()) {
      if (adminClientConfigProperties.contains(entry.getKey())) {
        adminClientConfigs.put(entry.getKey(), entry.getValue());
      }
    }
    return adminClientConfigs;
  }

  public Object get(String propertyName) {
    if (propertyName.toLowerCase().startsWith(KSQL_CONFIG_PREPERTY_PREFIX)) {
      return ksqlConfigProps.get(propertyName);
    } else {
      return ksqlStreamConfigProps.get(propertyName);
    }
  }

  public void put(String propertyName, Object propertyValue) {
    if (propertyName.toLowerCase().startsWith(KSQL_CONFIG_PREPERTY_PREFIX)) {
      ksqlConfigProps.put(propertyName, propertyValue);
    } else {
      ksqlStreamConfigProps.put(propertyName, propertyValue);
    }
  }

  public KsqlConfig clone() {
    Map<String, Object> clonedProperties = new HashMap<>();
    clonedProperties.putAll(ksqlConfigProps);
    clonedProperties.putAll(ksqlStreamConfigProps);
    return new KsqlConfig(clonedProperties);
  }

}
