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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Class that validates if a property, or list of properties, is part of a list of denied
 * properties.
 */
public class DenyListPropertyValidator {
  private final Set<String> immutableProps;

  public DenyListPropertyValidator(final Collection<String> immutableProps) {
    this.immutableProps = ImmutableSet.copyOf(
        Objects.requireNonNull(immutableProps, "immutableProps"));
  }

  /**
   * Validates if a list of properties are part of the list of denied properties.
   * @throws if a property is part of the denied list.
   */
  public void validateAll(final Map<String, Object> properties) {
    properties.forEach((name ,v) -> {
      if (immutableProps.contains(name)) {
        throw new KsqlException(String.format("A property override was set locally for a "
            + "property that the server prohibits overrides for: '%s'", name));
      }
    });
  }
}
