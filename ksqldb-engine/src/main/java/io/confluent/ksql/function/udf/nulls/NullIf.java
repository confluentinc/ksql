/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf.nulls;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = NullIf.NAME_TEXT,
    category = FunctionCategory.CONDITIONAL,
    description = "Returns NULL if value1 and value2 are equal. Otherwise it returns value1.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class NullIf {

  public static final String NAME_TEXT = "NULLIF";
  public static final FunctionName NAME = FunctionName.of(NAME_TEXT);

  @Udf
  public final <T> T nullIf(
          @UdfParameter(description = "expression 1") final T expr1,
          @UdfParameter(description = "expression 2") final T expr2
  ) {
    if (expr1 == null) {
      return null;
    }
    if (expr1.equals(expr2)) {
      return null;
    } else {
      return expr1;
    }
  }
}
