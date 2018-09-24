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
   */
  Optional<ConfigItem> resolve(String propertyName);
}
