/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.types;

import com.google.errorprone.annotations.Immutable;

/**
 * A {@code ArgType} represents the types that can be used as part
 * of a {@link io.confluent.ksql.function.FunctionSignature} - this is
 * an analog to {@link java.lang.reflect.Type}, but does not support the
 * entire Java type system and is also flexible enough to map to other
 * programming languages (namely, the SQL type language).
 *
 * <p>This does not leverage the SQL type system directly to support a
 * wider range of types that are useful for defining functions (e.g.
 * generics and variable arguments).</p>
 */
@Immutable
public abstract class ParamType {

  @Override
  public abstract int hashCode();

  @Override
  public abstract boolean equals(Object obj);

  @Override
  public abstract String toString();
}
