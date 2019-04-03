/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package ${package};

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
    name = "reverse",
    description = "Example UDF that reverses an object",
    version = "${version}",
    author = "${author}"
)
public class ReverseUdf {

  @Udf(description = "Reverse a string")
  public String reverseString(
      @UdfParameter(value = "source", description = "the value to reverse")
      final String source
  ) {
    return new StringBuilder(source).reverse().toString();
  }

  @Udf(description = "Reverse an integer")
  public String reverseInt(
      @UdfParameter(value = "source", description = "the value to reverse")
      final Integer source
  ) {
    return new StringBuilder(source.toString()).reverse().toString();
  }

  @Udf(description = "Reverse a long")
  public String reverseLong(
      @UdfParameter(value = "source", description = "the value to reverse")
      final Long source
  ) {
    return new StringBuilder(source.toString()).reverse().toString();
  }

  @Udf(description = "Reverse a double")
  public String reverseDouble(
      @UdfParameter(value = "source", description = "the value to reverse")
      final Double source
  ) {
    return new StringBuilder(source.toString()).reverse().toString();
  }
}
