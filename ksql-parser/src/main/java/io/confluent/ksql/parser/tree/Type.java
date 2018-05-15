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

import static java.util.Objects.requireNonNull;

public abstract class Type extends Expression {


  public enum KsqlType {
    BOOLEAN, INTEGER, BIGINT, DOUBLE, STRING, ARRAY, MAP, STRUCT
  }

  final KsqlType ksqlType;

  public Type(KsqlType ksqlType) {
    this(Optional.empty(), ksqlType);
  }

  public Type(NodeLocation location, KsqlType ksqlType) {
    this(Optional.of(location), ksqlType);
  }

  protected Type(Optional<NodeLocation> location, KsqlType ksqlType) {
    super(location);
    requireNonNull(ksqlType, "ksqlType is null");
    this.ksqlType = ksqlType;
  }

  public KsqlType getKsqlType() {
    return ksqlType;
  }
}
