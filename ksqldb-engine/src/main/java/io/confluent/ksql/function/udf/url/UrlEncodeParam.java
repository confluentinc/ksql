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

package io.confluent.ksql.function.udf.url;

import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "url_encode_param",
    category = FunctionCategory.URL,
    description = "Returns a version of the input with all URL sensitive characters encoded "
        + "using the application/x-www-form-urlencoded standard.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class UrlEncodeParam {

  @Udf
  public String encodeParam(
      @UdfParameter(description = "the value to encode") final String input) {
    if (input == null) {
      return null;
    }
    final Escaper escaper = UrlEscapers.urlFormParameterEscaper();
    return escaper.escape(input);
  }
}
