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

@UdfDescription(name = "sublist", description = "sublist of a generic list")
public class Sublist {

  @Udf()
  public <T> List<T> sublist(
      @UdfParameter(description = "the input list")         final List<T> in,
      @UdfParameter(description = "start index, inclusive") final int from,
      @UdfParameter(description = "end index, exclusive")   final int to) {
    return in.subList(from, to);
  }

}