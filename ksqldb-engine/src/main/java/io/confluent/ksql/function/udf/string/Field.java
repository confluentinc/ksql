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
    name = "field",
    category = FunctionCategory.STRING,
    description = Field.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Field {

  static final String DESCRIPTION = "Returns the position (1-indexed) of str in args, or 0 if"
      + " not found. If str is NULL, the return value is 0 because NULL is not considered"
      + " equal to any value. This is the compliment to ELT.";

  @Udf
  public int field(
      @UdfParameter final String str,
      @UdfParameter final String... args
  ) {
    if (str == null || args == null) {
      return 0;
    }

    for (int i = 0; i < args.length; i++) {
      if (str.equals(args[i])) {
        return i + 1;
      }
    }

    return 0;
  }

}
