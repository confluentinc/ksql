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

import com.google.common.base.Splitter;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.net.URI;
import java.util.List;

@UdfDescription(
        name = UrlExtractParameterKudf.NAME,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Extracts a parameter with a specified name encoded inside an "
                      + "application/x-www-form-urlencoded String.")
public class UrlExtractParameterKudf {

  static final String NAME = "url_extract_parameter";

  private static final Splitter PARAM_SPLITTER = Splitter.on('&');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  @Udf(description = "Extracts a parameter with a specified name encoded inside an "
          + "application/x-www-form-urlencoded String.")
  public String extractParam(
      @UdfParameter(value = "input", description = "a vald URL")
      final String input,
      @UdfParameter(
          value = "paramToFind",
          description = "the query parameter whose value will be extracted")
      final String paramToFind) {
    final String query = UrlParser.extract(input, URI::getQuery);
    if (query == null) {
      return null;
    }

    for (final String param : PARAM_SPLITTER.split(query)) {
      final List<String> kvParam = KV_SPLITTER.splitToList(param);
      if (kvParam.size() == 2 && kvParam.get(0).equals(paramToFind)) {
        return kvParam.get(1);
      }
    }
    return null;
  }
}
