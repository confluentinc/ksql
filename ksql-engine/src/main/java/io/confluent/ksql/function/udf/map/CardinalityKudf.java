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

package io.confluent.ksql.function.udf.map;

import java.util.List;
import java.util.Map;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;

@UdfDescription(name = "cardinality", author = "Confluent",
    description = "Returns the number of keys in the specified map or entries in an array.")
public class CardinalityKudf {

  @Udf(description = "Returns the number of keys in the specified map.")
  public Integer cardinality(final Map<String, ?> input) {
    if (input == null) {
      return null;
    }
    return input.keySet().size();
  }

  @Udf(description = "Returns the number of keys in the specified array.")
  public Integer cardinality(@SuppressWarnings("rawtypes") final List input) {
    if (input == null) {
      return null;
    }
    return input.size();
  }
}
