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

package io.confluent.ksql.parser.properties.with;

import io.confluent.ksql.serde.FormatInfo;

public final class SourcePropertiesUtil {

  private SourcePropertiesUtil() {
  }

  public static FormatInfo getKeyFormat(final CreateSourceProperties properties) {
    return properties.getKeyFormat()
        .orElseThrow(() -> new IllegalStateException("Key format not present"));
  }

  public static FormatInfo getValueFormat(final CreateSourceProperties properties) {
    return properties.getValueFormat()
        .orElseThrow(() -> new IllegalStateException("Value format not present"));
  }

}
