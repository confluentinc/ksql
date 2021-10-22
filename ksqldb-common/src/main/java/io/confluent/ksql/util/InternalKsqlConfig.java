/*
 * Copyright 2021 Confluent Inc.
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
package io.confluent.ksql.util;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;

import io.confluent.ksql.configdef.ConfigValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import static io.confluent.ksql.configdef.ConfigValidators.zeroOrPositive;
import static io.confluent.ksql.util.KsqlConfig.CONNECT_URL_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.DEFAULT_CONNECT_URL;
import static io.confluent.ksql.util.KsqlConfig.DEFAULT_EXT_DIR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_AUTO;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_OFF;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACCESS_VALIDATOR_ON;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_AUTH_CACHE_MAX_ENTRIES_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_COLLECT_UDF_METRICS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CREATE_OR_REPLACE_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CUSTOM_METRICS_TAGS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_CUSTOM_METRICS_TAGS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_ACCESS_VALIDATOR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ENABLE_UDFS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ERROR_CLASSIFIER_REGEX_PREFIX;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ERROR_CLASSIFIER_REGEX_PREFIX_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_EXT_DIR;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HIDDEN_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HIDDEN_TOPICS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_HIDDEN_TOPICS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INSERT_INTO_VALUES_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_LAMBDAS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_LAMBDAS_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_LAMBDAS_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_METASTORE_BACKUP_LOCATION_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_PULL_QUERIES_ENABLE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERYANONYMIZER_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_INTERPRETER_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_MAX_QPS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_METRICS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_METRICS_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_RANGE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PULL_THREAD_POOL_SIZE_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_READONLY_TOPICS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_READONLY_TOPICS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_READONLY_TOPICS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_ROWPARTITION_ROWOFFSET_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SECURITY_EXTENSION_CLASS;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SECURITY_EXTENSION_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SECURITY_EXTENSION_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SERVICE_ID_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_ENABLED_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_SUPPRESS_ENABLED_DOC;
import static io.confluent.ksql.util.KsqlConfig.KSQL_UDF_SECURITY_MANAGER_ENABLED;
import static io.confluent.ksql.util.KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE;
import static io.confluent.ksql.util.KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE_DEFAULT;
import static io.confluent.ksql.util.KsqlConfig.KSQL_VARIABLE_SUBSTITUTION_ENABLE_DOC;
import static io.confluent.ksql.util.KsqlConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static io.confluent.ksql.util.KsqlConfig.METRIC_REPORTER_CLASSES_DOC;
import static io.confluent.ksql.util.KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY;

public class InternalKsqlConfig extends AbstractConfig {
    private static final Logger LOG = LoggerFactory.getLogger(InternalKsqlConfig.class);

    public static final ConfigDef CONFIG_DEF;

    static {
        CONFIG_DEF = new ConfigDef()
            .define(
                KSQL_SERVICE_ID_CONFIG,
                ConfigDef.Type.STRING,
                KSQL_SERVICE_ID_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                "Indicates the ID of the ksql service. It will be used as prefix for "
                    + "all implicitly named resources created by this instance in Kafka. "
                    + "By convention, the id should end in a seperator character of some form, e.g. "
                    + "a dash or underscore, as this makes identifiers easier to read."
            )
            .define(
                SCHEMA_REGISTRY_URL_PROPERTY,
                ConfigDef.Type.STRING,
                "",
                new ConfigDef.NonNullValidator(),
                ConfigDef.Importance.MEDIUM,
                "The URL for the schema registry"
            )
            .define(
                CONNECT_URL_PROPERTY,
                ConfigDef.Type.STRING,
                DEFAULT_CONNECT_URL,
                Importance.MEDIUM,
                "The URL for the connect deployment, defaults to http://localhost:8083"
            )
            .define(
                CONNECT_WORKER_CONFIG_FILE_PROPERTY,
                ConfigDef.Type.STRING,
                "",
                Importance.LOW,
                "The path to a connect worker configuration file. An empty value for this configuration"
                    + "will prevent connect from starting up embedded within KSQL. For more information"
                    + " on configuring connect, see "
                    + "https://docs.confluent.io/current/connect/userguide.html#configuring-workers."
            )
            .define(
                KSQL_ENABLE_UDFS,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.MEDIUM,
                "Whether or not custom UDF jars found in the ext dir should be loaded. Default is true "
            )
            .define(
                KSQL_COLLECT_UDF_METRICS,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                "Whether or not metrics should be collected for custom udfs. Default is false. Note: "
                    + "this will add some overhead to udf invocation. It is recommended that this "
                    + " be set to false in production."
            )
            .define(
                KSQL_EXT_DIR,
                ConfigDef.Type.STRING,
                DEFAULT_EXT_DIR,
                ConfigDef.Importance.LOW,
                "The path to look for and load extensions such as UDFs from."
            )
            .define(
                KSQL_INTERNAL_TOPIC_REPLICAS_PROPERTY,
                Type.SHORT,
                (short) 1,
                ConfigDef.Importance.MEDIUM,
                "The replication factor for the internal topics of KSQL server."
            )
            .define(
                KSQL_INTERNAL_TOPIC_MIN_INSYNC_REPLICAS_PROPERTY,
                Type.SHORT,
                (short) 1,
                ConfigDef.Importance.MEDIUM,
                "The minimum number of insync replicas for the internal topics of KSQL server."
            )
            .define(
                KSQL_UDF_SECURITY_MANAGER_ENABLED,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                "Enable the security manager for UDFs. Default is true and will stop UDFs from"
                    + " calling System.exit or executing processes"
            )
            .define(
                KSQL_INSERT_INTO_VALUES_ENABLED,
                Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                "Enable the INSERT INTO ... VALUES functionality."
            )
            .define(
                KSQL_SECURITY_EXTENSION_CLASS,
                Type.CLASS,
                KSQL_SECURITY_EXTENSION_DEFAULT,
                ConfigDef.Importance.LOW,
                KSQL_SECURITY_EXTENSION_DOC
            )
            .define(
                KSQL_CUSTOM_METRICS_TAGS,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.LOW,
                KSQL_CUSTOM_METRICS_TAGS_DOC
            )
            .define(
                KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.LOW,
                KSQL_QUERYANONYMIZER_CLUSTER_NAMESPACE_DOC
            )
            .define(
                KSQL_QUERYANONYMIZER_ENABLED,
                ConfigDef.Type.BOOLEAN,
                true,
                ConfigDef.Importance.LOW,
                KSQL_QUERYANONYMIZER_ENABLED_DOC
            )
            .define(
                KSQL_ENABLE_ACCESS_VALIDATOR,
                Type.STRING,
                KSQL_ACCESS_VALIDATOR_AUTO,
                ValidString.in(
                    KSQL_ACCESS_VALIDATOR_ON,
                    KSQL_ACCESS_VALIDATOR_OFF,
                    KSQL_ACCESS_VALIDATOR_AUTO
                ),
                ConfigDef.Importance.LOW,
                KSQL_ACCESS_VALIDATOR_DOC
            )
            .define(METRIC_REPORTER_CLASSES_CONFIG,
                     Type.LIST,
                     "",
                     Importance.LOW,
                     METRIC_REPORTER_CLASSES_DOC
            )
            .define(
                KSQL_PULL_QUERIES_ENABLE_CONFIG,
                Type.BOOLEAN,
                KSQL_QUERY_PULL_ENABLE_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_ENABLE_DOC
            )
            .define(
                KSQL_QUERY_PULL_ENABLE_STANDBY_READS,
                Type.BOOLEAN,
                KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DEFAULT,
                Importance.MEDIUM,
                KSQL_QUERY_PULL_ENABLE_STANDBY_READS_DOC
            )
            .define(
                KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_CONFIG,
                Type.LONG,
                KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DEFAULT,
                zeroOrPositive(),
                Importance.MEDIUM,
                KSQL_QUERY_PULL_MAX_ALLOWED_OFFSET_LAG_DOC
            )
            .define(
                KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_CONFIG,
                Type.INT,
                KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DEFAULT,
                Importance.MEDIUM,
                KSQL_ACTIVE_PERSISTENT_QUERY_LIMIT_DOC
            )
            .define(
                KSQL_SHUTDOWN_TIMEOUT_MS_CONFIG,
                Type.LONG,
                KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT,
                Importance.MEDIUM,
                KSQL_SHUTDOWN_TIMEOUT_MS_DOC
            )
            .define(
                KSQL_AUTH_CACHE_EXPIRY_TIME_SECS,
                Type.LONG,
                KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DEFAULT,
                Importance.LOW,
                KSQL_AUTH_CACHE_EXPIRY_TIME_SECS_DOC
            )
            .define(
                KSQL_AUTH_CACHE_MAX_ENTRIES,
                Type.LONG,
                KSQL_AUTH_CACHE_MAX_ENTRIES_DEFAULT,
                Importance.LOW,
                KSQL_AUTH_CACHE_MAX_ENTRIES_DOC
            )
            .define(
                KSQL_HIDDEN_TOPICS_CONFIG,
                Type.LIST,
                KSQL_HIDDEN_TOPICS_DEFAULT,
                ConfigValidators.validRegex(),
                Importance.LOW,
                KSQL_HIDDEN_TOPICS_DOC
            )
            .define(
                KSQL_READONLY_TOPICS_CONFIG,
                Type.LIST,
                KSQL_READONLY_TOPICS_DEFAULT,
                ConfigValidators.validRegex(),
                Importance.LOW,
                KSQL_READONLY_TOPICS_DOC
            )
            .define(
                KSQL_QUERY_PULL_METRICS_ENABLED,
                Type.BOOLEAN,
                true,
                Importance.LOW,
                KSQL_QUERY_PULL_METRICS_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_PULL_MAX_QPS_CONFIG,
                Type.INT,
                KSQL_QUERY_PULL_MAX_QPS_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_MAX_QPS_DOC
            )
            .define(
                KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_CONFIG,
                Type.INT,
                KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_MAX_CONCURRENT_REQUESTS_DOC
            )
            .define(
                KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
                Type.INT,
                KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT,
                Importance.HIGH,
                KSQL_QUERY_PULL_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
            )
            .define(
                KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_CONFIG,
                Type.INT,
                KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DEFAULT,
                Importance.HIGH,
                KSQL_QUERY_PUSH_V2_MAX_HOURLY_BANDWIDTH_MEGABYTES_DOC
            )
            .define(
                KSQL_QUERY_PULL_THREAD_POOL_SIZE_CONFIG,
                Type.INT,
                KSQL_QUERY_PULL_THREAD_POOL_SIZE_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_THREAD_POOL_SIZE_DOC
            )
            .define(
                KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_CONFIG,
                Type.INT,
                KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_ROUTER_THREAD_POOL_SIZE_DOC
            )
            .define(
                KSQL_QUERY_PULL_TABLE_SCAN_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_TABLE_SCAN_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_STREAM_PULL_QUERY_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_STREAM_PULL_QUERY_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_PULL_RANGE_SCAN_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_RANGE_SCAN_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_PULL_INTERPRETER_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_PULL_INTERPRETER_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PULL_INTERPRETER_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_PUSH_V2_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_PUSH_V2_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PUSH_V2_ENABLED_DOC
            )
            .define(
                KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED,
                Type.BOOLEAN,
                KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PUSH_V2_REGISTRY_INSTALLED_DOC
            )
            .define(
                KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY,
                Type.BOOLEAN,
                KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PUSH_V2_NEW_NODE_CONTINUITY_DOC
            )
            .define(
                KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED,
                Type.BOOLEAN,
                KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_PUSH_V2_INTERPRETER_ENABLED_DOC
            )
            .define(
                KSQL_ERROR_CLASSIFIER_REGEX_PREFIX,
                Type.STRING,
                "",
                Importance.LOW,
                KSQL_ERROR_CLASSIFIER_REGEX_PREFIX_DOC
            )
            .define(
                KSQL_CREATE_OR_REPLACE_ENABLED,
                Type.BOOLEAN,
                KSQL_CREATE_OR_REPLACE_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_CREATE_OR_REPLACE_ENABLED_DOC
            )
            .define(
                KSQL_METASTORE_BACKUP_LOCATION,
                Type.STRING,
                KSQL_METASTORE_BACKUP_LOCATION_DEFAULT,
                Importance.LOW,
                KSQL_METASTORE_BACKUP_LOCATION_DOC
            )
            .define(
                KSQL_SUPPRESS_ENABLED,
                Type.BOOLEAN,
                KSQL_SUPPRESS_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_SUPPRESS_ENABLED_DOC
            )
            // TODO if/when suppress is officially supported, move this back to KsqlConfig
            .define(
                KSQL_SUPPRESS_BUFFER_SIZE_BYTES,
                Type.LONG,
                KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DEFAULT,
                Importance.LOW,
                KSQL_SUPPRESS_BUFFER_SIZE_BYTES_DOC
            )
            .define(
                KSQL_PROPERTIES_OVERRIDES_DENYLIST,
                Type.LIST,
                "",
                Importance.LOW,
                KSQL_PROPERTIES_OVERRIDES_DENYLIST_DOC
            )
            .define(
                KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS,
                Type.INT,
                KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DEFAULT,
                Importance.LOW,
                KSQL_QUERY_STATUS_RUNNING_THRESHOLD_SECS_DOC
            )
            .define(
                KSQL_VARIABLE_SUBSTITUTION_ENABLE,
                Type.BOOLEAN,
                KSQL_VARIABLE_SUBSTITUTION_ENABLE_DEFAULT,
                Importance.LOW,
                KSQL_VARIABLE_SUBSTITUTION_ENABLE_DOC
            )
            .define(
                KSQL_LAMBDAS_ENABLED,
                Type.BOOLEAN,
                KSQL_LAMBDAS_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_LAMBDAS_ENABLED_DOC
            )
            .define(
                KSQL_ROWPARTITION_ROWOFFSET_ENABLED,
                Type.BOOLEAN,
                KSQL_ROWPARTITION_ROWOFFSET_DEFAULT,
                Importance.LOW,
                KSQL_ROWPARTITION_ROWOFFSET_DOC
            )
            .define(
                KSQL_SHARED_RUNTIME_ENABLED,
                Type.BOOLEAN,
                KSQL_SHARED_RUNTIME_ENABLED_DEFAULT,
                Importance.MEDIUM,
                KSQL_SHARED_RUNTIME_ENABLED_DOC
            )
            .define(
                KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED,
                Type.BOOLEAN,
                KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DEFAULT,
                Importance.LOW,
                KSQL_SOURCE_TABLE_MATERIALIZATION_ENABLED_DOC
            )
            .withClientSslSupport();
    }

    public InternalKsqlConfig(final Map<?, ?> props) {
        super(CONFIG_DEF, props);
    }

}
