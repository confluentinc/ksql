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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;

@UdfDescription(
    name = "ARRAY_CONTAINS",
    category = FunctionCategory.ARRAY,
    description = ArrayContains.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class ArrayContains {

  static final String DESCRIPTION = "Returns true if the array is non-null and contains the "
      + "supplied value.";

  @Udf
  public <T> boolean contains(
      @UdfParameter final List<T> array,
      @UdfParameter final T val
  ) {
    return array != null && array.contains(val);
  }

}
