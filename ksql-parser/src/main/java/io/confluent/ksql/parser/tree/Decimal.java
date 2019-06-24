/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser.tree;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

@Immutable
public final class Decimal extends Type {

  private final int precision;
  private final int scale;

  public static Decimal of(final int precision, final int scale) {
    return new Decimal(precision, scale);
  }

  public static Decimal of(final Schema schema) {
    return new Decimal(DecimalUtil.precision(schema), DecimalUtil.scale(schema));
  }

  private Decimal(final int precision, final int scale) {
    super(Optional.empty(), SqlType.DECIMAL);
    this.precision = precision;
    this.scale = scale;
  }

  public int getPrecision() {
    return precision;
  }

  public int getScale() {
    return scale;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitDecimal(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final Decimal that = (Decimal) o;
    return precision == that.precision
        && scale == that.scale;
  }

  @Override
  public int hashCode() {
    return Objects.hash(precision, scale);
  }

  @Override
  public boolean supportsCast() {
    return true;
  }
}
