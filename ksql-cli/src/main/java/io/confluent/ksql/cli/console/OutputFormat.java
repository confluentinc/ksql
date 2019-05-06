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

package io.confluent.ksql.cli.console;

import java.util.Arrays;
import java.util.stream.Collectors;

public enum OutputFormat {
  JSON,
  TABULAR;

  public static final String VALID_FORMATS = Arrays.stream(OutputFormat.values())
      .map(Object::toString)
      .collect(Collectors.joining("', '", "'", "'"));

  public static OutputFormat get(final String format) {
    try {
      return OutputFormat.valueOf(format);
    } catch (final IllegalArgumentException e) {
      throw new IllegalArgumentException("Unknown Output format: " + format
          + ". Valid values are: " + VALID_FORMATS);
    }
  }
}
