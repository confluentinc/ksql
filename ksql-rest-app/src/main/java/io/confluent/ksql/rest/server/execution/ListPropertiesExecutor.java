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
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.PropertiesList;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ListPropertiesExecutor {

  private ListPropertiesExecutor() { }

  public static Optional<KsqlEntity> execute(
      final PreparedStatement statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext,
      final KsqlConfig ksqlConfig,
      final Map<String, Object> propertyOverrides
  ) {
    final KsqlConfigResolver resolver = new KsqlConfigResolver();

    final Map<String, String> engineProperties
        = ksqlConfig.getAllConfigPropsWithSecretsObfuscated();

    final Map<String, String> mergedProperties = ksqlConfig
        .cloneWithPropertyOverwrite(propertyOverrides)
        .getAllConfigPropsWithSecretsObfuscated();

    final List<String> overwritten = mergedProperties.entrySet()
        .stream()
        .filter(e -> !Objects.equals(engineProperties.get(e.getKey()), e.getValue()))
        .map(Entry::getKey)
        .collect(Collectors.toList());

    final List<String> defaultProps = mergedProperties.entrySet().stream()
        .filter(e -> resolver.resolve(e.getKey(), false)
            .map(resolved -> resolved.isDefaultValue(e.getValue()))
            .orElse(false))
        .map(Entry::getKey)
        .collect(Collectors.toList());

    return Optional.of(new PropertiesList(
        statement.getStatementText(), mergedProperties, overwritten, defaultProps));
  }

}
