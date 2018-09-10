/**
 * Copyright 2017 Confluent Inc.
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
 **/

package io.confluent.ksql.parser.tree;

import java.util.Optional;


public class LongLiteral extends Literal {

  private final long value;

  public LongLiteral(final long value) {
    this(Optional.empty(), value);
  }

  public LongLiteral(final NodeLocation location, final long value) {
    this(Optional.of(location), value);
  }

  private LongLiteral(final Optional<NodeLocation> location, final long value) {
    super(location);
    this.value = value;
  }

  public long getValue() {
    return value;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitLongLiteral(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LongLiteral that = (LongLiteral) o;

    if (value != that.value) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return (int) (value ^ (value >>> 32));
  }
}
