/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.config;

import java.util.Optional;

/**
 * A resolver of configuration names.
 */
public interface ConfigResolver {

  /**
   * Attempt to resolve the supplied {@code propertyName}.
   *
   * <p>The return value is tri-state.
   * <ul>
   * <li><b>{@code Optional.empty()}</b> - indicating the property is know to be
   * invalid/unknown.</li>
   * <li><b>{@code ConfigItem.Resolved}</b></li> - a known valid property. Such items can parse
   * and validate potential values for the property and can obfuscate sensitive data.
   * <li><b>{@code ConfigItem.Unresolved}</b></li> - a potentially valid property. Such items
   * perform no parsing, validation or obfuscation.
   * </ul>
   *
   * <p>For {@code strict} resolution, only known configuration, i.e. those that map directly to an
   * item in a {@code  ConfigDef} instance, or UDF properties, i.e. those prefixed with
   * {@link io.confluent.ksql.util.KsqlConfig#KSQL_FUNCTIONS_PROPERTY_PREFIX}, will be resolved.
   * All others will result in {@code empty}.
   *
   * <p>For non-{@code strict} resolution, only configuration <i>known</i> to be invalid, e.g.
   * properties that are prefixed with {@code ksql.}, but which are not known KSQL config, will
   * result in {@code empty}.
   *
   * @param propertyName the name of the property to resolve.
   * @param strict if resolution should use strict mode or not.
   */
  Optional<ConfigItem> resolve(String propertyName, boolean strict);

  Optional<ConfigItem> resolveKsqlConfig(String propertyName);

  Optional<ConfigItem> resolveStreamsConfig(String propertyName, boolean strict);
}
