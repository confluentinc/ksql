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

package io.confluent.ksql.function.udf.list;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Arrays;
import java.util.List;

@UdfDescription(name = "AS_ARRAY", description = "Construct a list based on some inputs")
public class AsArray {

  @SuppressWarnings("varargs")
  @SafeVarargs
  @Udf
  public final <T> List<T> asArray(@UdfParameter final T... elements) {
    return Arrays.asList(elements);
  }

}