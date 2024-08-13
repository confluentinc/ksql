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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.parser.tree.ListProperties;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.rest.entity.PropertiesList.Property;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.runtime.distributed.DistributedConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ListPropertiesExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ListPropertiesExecutor.class);

  private ListPropertiesExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListProperties> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KsqlConfigResolver resolver = new KsqlConfigResolver();

    final Map<String, String> engineProperties = statement
        .getSessionConfig()
        .getConfig(false)
        .getAllConfigPropsWithSecretsObfuscated();

    final List<Property> mergedProperties = mergedProperties(statement);

    final List<String> overwritten = mergedProperties
        .stream()
        .filter(property -> !Objects.equals(
            engineProperties.get(property.getName()), property.getValue()))
        .map(Property::getName)
        .collect(Collectors.toList());

    final List<String> defaultProps = mergedProperties.stream()
        .filter(property -> resolver.resolve(property.getName(), false)
            .map(resolved -> resolved.isDefaultValue(property.getValue()))
            .orElse(false))
        .map(Property::getName)
        .collect(Collectors.toList());

    return StatementExecutorResponse.handled(Optional.of(new PropertiesList(
        statement.getMaskedStatementText(), mergedProperties, overwritten, defaultProps)));
  }

  private static List<Property> mergedProperties(
      final ConfiguredStatement<ListProperties> statement) {
    final List<Property> mergedProperties = new ArrayList<>();

    statement.getSessionConfig().getConfig(true)
        .getAllConfigPropsWithSecretsObfuscated()
        .forEach((key, value) -> mergedProperties.add(new Property(key, "KSQL", value)));

    embeddedConnectWorkerProperties(statement)
        .forEach((key, value) ->
                     mergedProperties.add(new Property(key, "EMBEDDED CONNECT WORKER", value)));

    return mergedProperties;
  }

  private static Map<String, String> embeddedConnectWorkerProperties(
      final ConfiguredStatement<ListProperties> statement
  ) {
    final String configFile = statement
        .getSessionConfig()
        .getConfig(false)
        .getString(KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY);

    if (configFile.isEmpty()) {
      return Collections.emptyMap();
    }

    final Map<String, String> workerProps = Utils.propsToStringMap(getWorkerProps(configFile));
    // only list known connect worker properties to avoid showing potentially sensitive data
    // in other configs
    final Set<String> allowList = embeddedConnectWorkerPropertiesAllowList(workerProps);
    return workerProps.keySet().stream()
        .filter(allowList::contains)
        .collect(Collectors.toMap(k -> k, workerProps::get));
  }

  private static Set<String> embeddedConnectWorkerPropertiesAllowList(
      final Map<String, String> workerProps
  ) {
    final DistributedConfig config;
    try {
      config = new DistributedConfig(workerProps);
    } catch (ConfigException e) {
      LOGGER.warn("Could not create Connect worker config to validate properties; "
          + "not displaying Connect worker properties as a result. Note that "
          + "this should not happen if ksqlDB was able to start with embedded Connect. "
          + "Error: {}", e.getMessage());
      return Collections.emptySet();
    }
    
    return config.values().keySet().stream()
        .filter(k -> config.typeOf(k) != Type.PASSWORD)
        .collect(Collectors.toSet());
  }

  private static Properties getWorkerProps(final String configFile) {
    try {
      return Utils.loadProps(configFile);
    } catch (final IOException e) {
      return new Properties();
    }
  }
}
