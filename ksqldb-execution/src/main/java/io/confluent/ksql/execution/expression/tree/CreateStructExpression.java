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

package io.confluent.ksql.execution.expression.tree;

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class CreateStructExpression extends Expression {

  private final ImmutableList<Field> fields;

  public CreateStructExpression(
      final List<Field> fields
  ) {
    this(Optional.empty(), fields);
  }

  public CreateStructExpression(
      final Optional<NodeLocation> location,
      final List<Field> fields
  ) {
    super(location);
    this.fields = ImmutableList.copyOf(fields);
  }

  @Override
  protected <R, C> R accept(final ExpressionVisitor<R, C> visitor, final C context) {
    return visitor.visitStructExpression(this, context);
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "fields is ImmutableList")
  public ImmutableList<Field> getFields() {
    return fields;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateStructExpression that = (CreateStructExpression) o;
    return Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fields);
  }

  @Immutable
  public static class Field {
    private final String name;
    private final Expression value;

    public Field(final String name, final Expression value) {
      this.name = Objects.requireNonNull(name, "name");
      this.value = Objects.requireNonNull(value, "value");
    }

    public String getName() {
      return name;
    }

    public Expression getValue() {
      return value;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Field field = (Field) o;
      return Objects.equals(name, field.name)
          && Objects.equals(value, field.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }

    @Override
    public String toString() {
      return "Field{"
          + "name='" + name + '\''
          + ", value=" + value
          + '}';
    }
  }

}
