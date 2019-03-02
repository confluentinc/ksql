/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

public abstract class Type extends Expression {

  public enum KsqlType {
    BOOLEAN, INTEGER, BIGINT, DOUBLE, STRING, ARRAY, MAP, STRUCT
  }

  private final KsqlType ksqlType;

  protected Type(final Optional<NodeLocation> location, final KsqlType ksqlType) {
    super(location);
    this.ksqlType = requireNonNull(ksqlType, "ksqlType");
  }

  public KsqlType getKsqlType() {
    return ksqlType;
  }
}
