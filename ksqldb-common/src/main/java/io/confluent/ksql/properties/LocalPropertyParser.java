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

package io.confluent.ksql.properties;

import io.confluent.ksql.config.ConfigItem;
import io.confluent.ksql.config.ConfigResolver;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.config.PropertyParser;
import io.confluent.ksql.config.PropertyValidator;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Objects;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LocalPropertyParser implements PropertyParser {

  private static final Logger LOG = LogManager.getLogger(LocalPropertyParser.class);

  private final ConfigResolver resolver;
  private final PropertyValidator validator;

  public LocalPropertyParser() {
    this(new KsqlConfigResolver(), new LocalPropertyValidator());
  }

  LocalPropertyParser(final ConfigResolver resolver, final PropertyValidator validator) {
    this.resolver = Objects.requireNonNull(resolver, "resolver");
    this.validator = Objects.requireNonNull(validator, "validator");
  }

  @Override
  public Object parse(final String property, final Object value) {
    if (property.equalsIgnoreCase(KsqlConstants.LEGACY_RUN_SCRIPT_STATEMENTS_CONTENT)) {
      validator.validate(property, value);
      return value;
    }

    final ConfigItem configItem = resolver.resolve(property, true)
        .orElseThrow(() -> new PropertyNotFoundException(property));

    // Migrate legacy processing guarantee values before validation
    final Object migratedValue = migrateProcessingGuaranteeIfNeeded(
        configItem.getPropertyName(), value);

    final Object parsedValue = configItem.parseValue(migratedValue);

    validator.validate(configItem.getPropertyName(), parsedValue);
    return parsedValue;
  }

  /**
   * Migrates legacy processing guarantee values for backward compatibility.
   *
   * @param propertyName the property name
   * @param value the property value
   * @return the migrated value if applicable, otherwise the original value
   */
  private static Object migrateProcessingGuaranteeIfNeeded(
      final String propertyName,
      final Object value) {
    if (propertyName.equals(StreamsConfig.PROCESSING_GUARANTEE_CONFIG) && value != null) {
      final String guarantee = value.toString();
      if (guarantee.equals("exactly_once")) {
        LOG.info("Migrating processing.guarantee "
            + "from deprecated 'exactly_once' to 'exactly_once_v2'");
        return StreamsConfig.EXACTLY_ONCE_V2;
      }
    }
    return value;
  }
}
