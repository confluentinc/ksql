/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.expression.tree;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.lang3.ArrayUtils;

@Immutable
public class BytesLiteral extends Literal {

  private final ImmutableList<Byte> value;

  public BytesLiteral(final ByteBuffer value) {
    this(Optional.empty(), value);
  }

  public BytesLiteral(final Optional<NodeLocation> location, final ByteBuffer value) {
    super(location);
    final byte[] bytes = new byte[requireNonNull(value, "value").capacity()];
    value.get(bytes);
    this.value = ImmutableList.copyOf(ArrayUtils.toObject(bytes));
  }

  @Override
  public ByteBuffer getValue() {
    return ByteBuffer.wrap(ArrayUtils.toPrimitive(value.toArray(new Byte[0])));
  }

  public Byte[] getByteArray() {
    return value.toArray(new Byte[0]);
  }

  @Override
  public <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitBytesLiteral(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final BytesLiteral that = (BytesLiteral) o;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }
}
