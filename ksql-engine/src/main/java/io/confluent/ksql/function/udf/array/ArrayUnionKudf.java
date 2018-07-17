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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "array_union", author = "Confluent",
    description = "Returns an array of all the distinct elements from both input arrays.")
public class ArrayUnionKudf {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Udf(description = "Returns an array of all the distinct elements from both input arrays.")
  public List union(final List input1, final List input2) {
    if (input1 == null || input2 == null) {
      return null;
    }
    Set combined = Sets.newHashSet(input1);
    combined.addAll(input2);
    return Lists.newArrayList(combined);
  }

}
