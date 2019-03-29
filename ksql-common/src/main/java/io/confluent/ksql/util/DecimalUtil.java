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

package io.confluent.ksql.util;

import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Utility functions to handle Decimal types.
 */
public final class DecimalUtil {
  public static final String PRECISION_FIELD = "precision";
  public static final String SCALE_FIELD = Decimal.SCALE_FIELD;

  private DecimalUtil() {

  }

  /**
   * Returns true if the specified {@link Schema} is a valid Decimal schema.
   * </p>
   * A valid Decimal schema is based on the Connect {@link Decimal} logical name to keep a standard
   * across Connect and KSQL decimals.
   * @param schema The schema to check for Decimal name
   * @return True if the schema is a valid Decimal type; False otherwise
   */
  public static boolean isDecimalSchema(final Schema schema) {
    if (schema == null) {
      return false;
    }

    // Use the Connect Decimal name because Schema does not have a type for decimals
    return (schema.name() != null) ? schema.name().equalsIgnoreCase(Decimal.LOGICAL_NAME) : false;
  }

  /**
   * Returns the precision of a Decimal schema.
   * </p>
   * The precision is obtained from the 'precision' key found on the {@link Schema}
   * parameters list.
   *
   * @param schema The schema to get the precision from
   * @return A numeric precision value
   */
  public static Integer getPrecision(final Schema schema) {
    Preconditions.checkArgument(isDecimalSchema(schema),
        "Schema is not a valid Decimal schema: " + schema);

    return Integer.parseInt(schema.parameters().get(PRECISION_FIELD));
  }

  /**
   * Returns the scale of a Decimal schema.
   * </p>
   * The scale is obtained from the 'scale' key found on the {@link Schema}
   * parameters list.
   *
   * @param schema The schema to get the scale from
   * @return A numeric scale value
   */
  public static Integer getScale(final Schema schema) {
    Preconditions.checkArgument(isDecimalSchema(schema),
        "Schema is not a valid Decimal schema: " + schema);

    return Integer.parseInt(schema.parameters().get(SCALE_FIELD));
  }

  /**
   * Builds a Decimal {@link Schema} type using the precision and scale values specified.
   * </p>
   * The schema is created by setting the {@link Decimal} logical name, and adding the
   * precision and scale values as a {@link Schema} parameters.
   *
   * @param precision The precision for the Decimal schema
   * @param scale The scale for the Decimal schema
   * @return A valid Decimal schema
   */
  public static Schema schema(final int precision, final int scale) {
    return builder(precision, scale).build();
  }

  private static SchemaBuilder builder(final int precision, final int scale) {
    // Do not use Decimal.schema(scale). Decimal does not set the precision, the precision
    // parameter must be set first (before scale) so that EntityUtil can obtain the list
    // of parameters in the right order, i.e. DECIMAL(precision, scale)
    return SchemaBuilder.bytes()
        .name(Decimal.LOGICAL_NAME)
        .parameter(PRECISION_FIELD, Integer.toString(precision))
        .parameter(SCALE_FIELD, Integer.toString(scale))
        .optional(); // KSQL only uses optional types
  }
}
