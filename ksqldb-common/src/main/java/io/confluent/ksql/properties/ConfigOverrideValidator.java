/*
 * Copyright 2026 Confluent Inc.
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

import java.util.Map;

/**
 * Validates the property overrides a user supplies on a request against server policy.
 *
 * <p>Two implementations exist and exactly one is active per server, selected at startup by
 * {@link ConfigOverrideValidatorFactory} from
 * {@code ksql.properties.overrides.validation.mode}:
 * <ul>
 *   <li>{@link DenyListPropertyValidator} &mdash; rejects a fixed set of prohibited names
 *       (default, today's behavior).</li>
 *   <li>{@link AllowListPropertyValidator} &mdash; rejects anything not explicitly permitted
 *       (deny-by-default).</li>
 * </ul>
 */
public interface ConfigOverrideValidator {

  /**
   * Validates every property override in {@code properties}.
   *
   * @param properties the user-supplied overrides (keys are property names)
   * @throws io.confluent.ksql.util.KsqlException if at least one property is not permitted
   */
  void validateAll(Map<String, Object> properties);
}
