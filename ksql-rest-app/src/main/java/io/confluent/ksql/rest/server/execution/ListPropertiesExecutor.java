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
import io.confluent.ksql.rest.entity.KsqlEntity;
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
import java.util.stream.Collectors;
import org.apache.kafka.common.utils.Utils;

public final class ListPropertiesExecutor {

  private ListPropertiesExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<ListProperties> statement,
      final Map<String, ?> sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final KsqlConfigResolver resolver = new KsqlConfigResolver();

    final Map<String, String> engineProperties
        = statement.getConfig().getAllConfigPropsWithSecretsObfuscated();

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

    return Optional.of(new PropertiesList(
        statement.getStatementText(), mergedProperties, overwritten, defaultProps));
  }

  private static List<Property> mergedProperties(
      ConfiguredStatement<ListProperties> statement) {
    final List<Property> mergedProperties = new ArrayList<>();

    statement.getConfig()
        .cloneWithPropertyOverwrite(statement.getOverrides())
        .getAllConfigPropsWithSecretsObfuscated()
        .forEach((key, value) -> mergedProperties.add(new Property(key, "KSQL", value)));

    embeddedConnectWorkerProperties(statement)
        .forEach((key, value) ->
                     mergedProperties.add(new Property(key, "EMBEDDED CONNECT WORKER", value)));

    return mergedProperties;
  }

  private static Map<String, String> embeddedConnectWorkerProperties(
      ConfiguredStatement<ListProperties> statement) {
    String configFile = statement.getConfig()
        .getString(KsqlConfig.CONNECT_WORKER_CONFIG_FILE_PROPERTY);
    return !configFile.isEmpty()
        ? Utils.propsToStringMap(getWorkerProps(configFile))
        : Collections.emptyMap();
  }

  private static Properties getWorkerProps(String configFile) {
    try {
      return Utils.loadProps(configFile);
    } catch (IOException e) {
      return new Properties();
    }
  }
}
