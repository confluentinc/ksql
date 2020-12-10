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
import io.confluent.ksql.schema.ksql.LogicalSchema;
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
   * defaults. However, until ksqlDB supports wrapped single keys, any internal topic with a
   * single key field and a key format that supports both wrapping and unwrapping needs to
   * have an explicit {@link SerdeFeature#UNWRAP_SINGLES} set to ensure backwards
   * compatibility is easily achievable once wrapped keys are supported.
   *
   * <p>The code that sets the feature can be removed once wrapped single keys are supported.
   * Issue 6296 tracks the removal.
   *
   * @param schema logical schema.
   * @param keyFormat key format.
   * @param valueFormat value format.
   * @return Formats instance.
   * @see <a href=https://github.com/confluentinc/ksql/issues/6296>Issue 6296</a>
   * @see SerdeFeaturesFactory#buildInternalKeyFeatures
   */
  public static Formats of(
      final LogicalSchema schema,
      final FormatInfo keyFormat,
      final FormatInfo valueFormat
  ) {
    // Do not use NONE format for internal topics:
    if (keyFormat.getFormat().equals(NoneFormat.NAME)) {
      throw new IllegalArgumentException(NoneFormat.NAME + " can not be used for internal topics");
    }

    return Formats.of(
        keyFormat,
        valueFormat,
        SerdeFeaturesFactory.buildInternalKeyFeatures(
            schema,
            FormatFactory.fromName(keyFormat.getFormat())),
        SerdeFeatures.of()
    );
  }

  public static final class PlaceholderFormats extends Formats {

    public static PlaceholderFormats of() {
      return new PlaceholderFormats();
    }

    private PlaceholderFormats() {
      super(
          FormatInfo.of("placeholder"),
          FormatInfo.of("placeholder"),
          SerdeFeatures.of(),
          SerdeFeatures.of()
      );
    }

    @Override
    public FormatInfo getKeyFormat() {
      throw new UnsupportedOperationException(
          "getKeyFormat should not be called on a placeholder");
    }

    @Override
    public FormatInfo getValueFormat() {
      throw new UnsupportedOperationException(
          "getValueFormat should not be called on a placeholder");
    }

    @Override
    public SerdeFeatures getKeyFeatures() {
      throw new UnsupportedOperationException(
          "getKeyFeatures should not be called on a placeholder");
    }

    @Override
    public SerdeFeatures getValueFeatures() {
      throw new UnsupportedOperationException(
          "getValueFeatures should not be called on a placeholder");
    }
  }
}
