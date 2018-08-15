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

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class InListExpression
    extends Expression {

  private final List<Expression> values;

  public InListExpression(final List<Expression> values) {
    this(Optional.empty(), values);
  }

  public InListExpression(final NodeLocation location, final List<Expression> values) {
    this(Optional.of(location), values);
  }

  private InListExpression(final Optional<NodeLocation> location, final List<Expression> values) {
    super(location);
    this.values = values;
  }

  public List<Expression> getValues() {
    return values;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitInListExpression(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final InListExpression that = (InListExpression) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return values.hashCode();
  }
}
