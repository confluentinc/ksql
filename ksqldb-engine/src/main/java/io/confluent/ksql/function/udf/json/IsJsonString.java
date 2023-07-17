/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udf.json;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "IS_JSON_STRING",
    category = FunctionCategory.JSON,
    description = "Given a string, returns true if it can be parsed as a valid JSON value, false"
        + " otherwise.",
    author = KsqlConstants.CONFLUENT_AUTHOR)
public class IsJsonString {

  @Udf
  public boolean check(@UdfParameter(description = "The input JSON string") final String input) {
    if (input == null) {
      return false;
    }

    try {
      return !UdfJsonMapper.parseJson(input).isMissingNode();
    } catch (KsqlFunctionException e) {
      return false;
    }
  }
}
