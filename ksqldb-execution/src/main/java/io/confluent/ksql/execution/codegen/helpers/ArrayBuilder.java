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

package io.confluent.ksql.execution.codegen.helpers;

import java.util.ArrayList;
import java.util.List;

/**
 * Used to construct arrays using the builder pattern. Note that we
 * cannot use {@link com.google.common.collect.ImmutableList} because
 * it does not accept null values.
 */
public class ArrayBuilder {

  private final ArrayList<Object> list;

  public ArrayBuilder(final int size) {
    list = new ArrayList<>(size);
  }

  public ArrayBuilder add(final Object value) {
    list.add(value);
    return this;
  }

  public List<Object> build() {
    return new ArrayList<>(list);
  }

}
