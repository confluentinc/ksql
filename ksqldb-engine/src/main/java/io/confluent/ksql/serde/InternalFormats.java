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

package io.confluent.ksql.serde;

import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.serde.connect.ConnectProperties;
import io.confluent.ksql.serde.none.NoneFormat;

/**
 * Util class for creating internal formats.
 */
public final class InternalFormats {

  private InternalFormats() {
  }

  /**
   * Build formats for internal topics.
   *
   * <p>Internal topics don't normally need any serde features set, as they use the format
   * defaults. However, until ksqlDB supports wrapped single keys, any internal topic with a key
   * format that supports both wrapping and unwrapping needs to have an explicit {@link
   * SerdeFeature#UNWRAP_SINGLES} set to ensure backwards compatibility is easily achievable once
   * wrapped keys are supported.
   * 
   * <p>Note: The unwrap feature should only be set when there is only a single key column. As
   * ksql does not yet support multiple key columns, the only time there is not a single key column
   * is when there is no key column, i.e. key-less streams. Internal topics, i.e. changelog and 
   * repartition topics, are never key-less. Hence, this method can safely set the unwrap feature
   * without checking the schema.
   *
   * <p>The code that sets the option can be removed once wrapped keys are supported. Issue 6296
   * tracks the removal.
   *
   * @param keyFormat key format.
   * @param valueFormatInfo value format info.
   * @return Formats instance.
   * @see <a href=https://github.com/confluentinc/ksql/issues/6296>Issue 6296</a>
   * @see SerdeFeaturesFactory#buildInternal
   */
  public static Formats of(final KeyFormat keyFormat, final FormatInfo valueFormatInfo) {
    // Do not use NONE format for internal topics:
    if (keyFormat.getFormatInfo().getFormat().equals(NoneFormat.NAME)) {
      throw new IllegalArgumentException(NoneFormat.NAME + " can not be used for internal topics");
    }

    // Internal formats should not use user-specified schema ids
    return Formats.of(
        keyFormat.getFormatInfo().copyWithoutProperty(ConnectProperties.SCHEMA_ID),
        valueFormatInfo.copyWithoutProperty(ConnectProperties.SCHEMA_ID),
        keyFormat.getFeatures(),
        SerdeFeatures.of()
    );
  }

}