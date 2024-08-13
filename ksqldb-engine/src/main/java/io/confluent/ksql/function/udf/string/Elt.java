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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "elt",
    category = FunctionCategory.STRING,
    description = Elt.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Elt {

  static final String DESCRIPTION = "ELT() returns the Nth element of the list of strings. Returns"
      + " NULL if N is less than 1 or greater than the number of arguments. Note that this method"
      + " is 1-indexed. This is the complement to FIELD.";

  @Udf
  public String elt(
      @UdfParameter(description = "the nth element to extract") final int n,
      @UdfParameter(description = "the strings of which to extract the nth") final String... args
  ) {
    if (args == null) {
      return null;
    }
    if (n < 1 || n > args.length) {
      return null;
    }

    return args[n - 1];
  }
}
