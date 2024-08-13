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

public class LocalPropertyParser implements PropertyParser {

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

    final Object parsedValue = configItem.parseValue(value);

    validator.validate(configItem.getPropertyName(), parsedValue);
    return parsedValue;
  }
}
