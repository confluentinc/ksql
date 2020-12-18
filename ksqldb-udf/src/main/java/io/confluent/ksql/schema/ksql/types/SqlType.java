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

package io.confluent.ksql.schema.ksql.types;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.utils.FormatOptions;
import java.util.Objects;

/**
 * Base for all SQL types in KSQL.
 */
@Immutable
public abstract class SqlType {

  private final SqlBaseType baseType;
  private final boolean optional;

  SqlType(final SqlBaseType baseType, final Boolean optional) {
    this.baseType = Objects.requireNonNull(baseType, "baseType");
    this.optional = Objects.requireNonNull(optional, "optional");

  }

  public SqlBaseType baseType() {
    return baseType;
  }

  public boolean isOptional() {
    return optional;
  }

  public abstract SqlType required();

  public abstract String toString(FormatOptions formatOptions);
}
