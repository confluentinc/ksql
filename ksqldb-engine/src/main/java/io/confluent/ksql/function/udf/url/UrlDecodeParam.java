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

import static java.nio.charset.StandardCharsets.UTF_8;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


@UdfDescription(
    name = "url_decode_param",
    category = FunctionCategory.URL,
    description = "Decodes a previously encoded application/x-www-form-urlencoded String",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class UrlDecodeParam {

  @Udf
  public String decodeParam(
      @UdfParameter(description = "the value to decode") final String input) {
    if (input == null) {
      return null;
    }
    try {
      return URLDecoder.decode(input, UTF_8.name());
    } catch (final UnsupportedEncodingException e) {
      throw new KsqlFunctionException(
          "url_decode udf encountered an encoding exception while decoding: " + input, e);
    }
  }
}
