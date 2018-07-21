/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udf.array;

import java.util.List;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "array_slice", author = "Confluent", description = "Returns a subsection of"
    + "an array, beginning at the specified offset (counted from 0), and of requested length. If"
    + " either the requested length or starting offset are negative then the entire array is"
    + " returned.")
public class ArraySliceKudf {

  @SuppressWarnings("rawtypes")
  @Udf(description = "As above.")
  public List slice(final List input, final int start, final int length) {
    if (input == null) {
      return null;
    }
    if (start < 0 || length < 0) {
      return input;
    }
    return input.subList(start, start + length);
  }
}
