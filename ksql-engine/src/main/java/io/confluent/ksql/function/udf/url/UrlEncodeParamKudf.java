/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
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
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = UrlEncodeParamKudf.NAME,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Returns a version of the input with all URL sensitive characters encoded "
                      + "using the application/x-www-form-urlencoded standard.")
public class UrlEncodeParamKudf {

  static final String NAME = "url_encode_param";

  @Udf(description = "Returns a version of 'input' with all URL sensitive characters encoded using"
                     + "the aplpication/x-www-form-urlencoded standard.")
  public String encodeParam(final String input) {
    final Escaper escaper = UrlEscapers.urlFormParameterEscaper();
    return escaper.escape(input);
  }
}
