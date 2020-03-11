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

package io.confluent.ksql.model;

import org.apache.commons.lang3.StringUtils;

/**
 * Enum of the windowing types KSQL supports.
 */
public enum WindowType {

  SESSION(false),
  HOPPING(true),
  TUMBLING(true);

  private final boolean requiresWindowSize;

  WindowType(final boolean requiresWindowSize) {
    this.requiresWindowSize = requiresWindowSize;
  }

  public boolean requiresWindowSize() {
    return requiresWindowSize;
  }

  public static WindowType of(final String text) {
    try {
      return WindowType.valueOf(text.toUpperCase());
    } catch (final Exception e) {
      throw new IllegalArgumentException("Unknown window type: '" + text + "' "
          + System.lineSeparator()
          + "Valid values are: " + StringUtils.join(WindowType.values(), ", "),
          e);
    }
  }
}
