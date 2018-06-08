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

import java.util.Objects;
import java.util.Optional;

import io.confluent.ksql.util.KsqlException;

import static java.util.Objects.requireNonNull;

public class PrimitiveType extends Type {

  final KsqlType ksqlType;

  public PrimitiveType(KsqlType ksqlType) {
    this(Optional.empty(), ksqlType);
  }

  public PrimitiveType(NodeLocation location, KsqlType ksqlType) {
    this(Optional.of(location), ksqlType);
  }

  private PrimitiveType(Optional<NodeLocation> location, KsqlType ksqlType) {
    super(location, ksqlType);
    requireNonNull(ksqlType, "ksqlType is null");
    this.ksqlType = ksqlType;
  }

  public static PrimitiveType getPrimitiveType(String typeName) {
    switch (typeName) {
      case "BOOLEAN":
        return new PrimitiveType(Type.KsqlType.BOOLEAN);
      case "INT":
      case "INTEGER":
        return new PrimitiveType(Type.KsqlType.INTEGER);
      case "BIGINT":
        return new PrimitiveType(Type.KsqlType.BIGINT);
      case "DOUBLE":
        return new PrimitiveType(Type.KsqlType.DOUBLE);
      case "VARCHAR":
      case "STRING":
        return new PrimitiveType(Type.KsqlType.STRING);
      default:
        throw new KsqlException("Invalid primitive column type: " + typeName);
    }
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitPrimitiveType(this, context);
  }

  @Override
  public KsqlType getKsqlType() {
    return ksqlType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(ksqlType);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PrimitiveType) {
      return ((PrimitiveType) obj).getKsqlType() == ksqlType;
    }
    return false;
  }
}
