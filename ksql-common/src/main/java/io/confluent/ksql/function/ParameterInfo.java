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

package io.confluent.ksql.function;

import io.confluent.ksql.function.types.ParamType;

public final class ParameterInfo {

  private final String name;
  private final ParamType type;
  private final String description;
  private final boolean isVariadic;

  public ParameterInfo(String name, ParamType type, String description, boolean isVariadic) {
    this.name = name;
    this.type = type;
    this.description = description;
    this.isVariadic = isVariadic;
  }

  public String name() {
    return name;
  }

  public ParamType type() {
    return type;
  }

  public String description() {
    return description;
  }

  public boolean isVariadic() {
    return isVariadic;
  }
}
