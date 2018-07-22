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
import java.util.Set;
import java.util.stream.Collectors;
import com.beust.jcommander.internal.Lists;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "array_except", author = "Confluent",
    description = "Returns an array of all the elements in an array except for those also present"
        + " in a second array. Any duplicates are removed. Returns NULL if either input is NULL")
public class ArrayExceptKudf {

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Udf(description = "Return an array of all distinct elements from a first array minus those"
      + " also present in a second array.")
  public List except(final List lhs, final List rhs) {
    if (lhs == null || rhs == null) {
      return null;
    }
    final Set distinct = (Set) lhs.stream()
        .filter(e -> !rhs.contains(e))
        .collect(Collectors.toSet());
    final List result = Lists.newArrayList(distinct);
    return result;
  }
}
