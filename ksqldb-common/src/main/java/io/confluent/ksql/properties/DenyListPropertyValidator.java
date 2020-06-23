/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.ksql.util.KsqlException;

import java.util.Collection;
import java.util.Map;

/**
 * Class that validates if a property, or list of properties, is part of a list of denied
 * properties.
 */
public class DenyListPropertyValidator extends LocalPropertyValidator {
  public DenyListPropertyValidator(final Collection<String> immutableProps) {
    super(immutableProps);
  }

  /**
   * Validates if a list of properties are part of the list of denied properties.
   * @throws if a property is part of the denied list.
   */
  public void validateAll(final Map<String, Object> properties) {
    properties.forEach((k ,v) -> {
      try {
        validate(k, v);
      } catch (final Exception e) {
        throw new KsqlException(e.getMessage());
      }
    });
  }
}
