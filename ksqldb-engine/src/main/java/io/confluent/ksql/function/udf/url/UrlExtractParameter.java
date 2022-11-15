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

import com.google.common.base.Splitter;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.net.URI;
import java.util.List;

@UdfDescription(
    name = "url_extract_parameter",
    category = FunctionCategory.URL,
    description = "Extracts a parameter with a specified name encoded inside an "
        + "application/x-www-form-urlencoded String.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class UrlExtractParameter {

  private static final Splitter PARAM_SPLITTER = Splitter.on('&');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  @Udf
  public String extractParam(
      @UdfParameter(description = "a valid URL") final String input,
      @UdfParameter(description = "the parameter key") final String paramToFind) {
    final String query = UrlParser.extract(input, URI::getQuery);
    if (query == null) {
      return null;
    }

    for (final String param : PARAM_SPLITTER.split(query)) {
      final List<String> kvParam = KV_SPLITTER.splitToList(param);
      if (kvParam.size() == 1 && kvParam.get(0).equals(paramToFind)) {
        return "";
      } else if (kvParam.size() == 2 && kvParam.get(0).equals(paramToFind)) {
        return kvParam.get(1);
      }
    }
    return null;
  }
}
