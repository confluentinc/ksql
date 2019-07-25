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
import java.util.List;

@UdfDescription(name = "slice", description = "slice of an ARRAY")
public class Slice {

  @Udf
  public <T> List<T> slice(
      @UdfParameter(description = "the input array")  final List<T> in,
      @UdfParameter(description = "start index")      final Integer from,
      @UdfParameter(description = "end index")        final Integer to) {
    if (in == null) {
      return null;
    }

    try {
      // SQL systems are usually 1-indexed and are inclusive of end index
      final int start = from == null ? 0 : from - 1;
      final int end = to == null ? in.size() : to;
      return in.subList(start, end);
    } catch (final IndexOutOfBoundsException e) {
      return null;
    }
  }

}