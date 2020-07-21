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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.net.URI;

@UdfDescription(
    name = "url_extract_port",
    category = FunctionCategory.URL,
    description = "Extracts the port from an application/x-www-form-urlencoded encoded String."
        + " If there is no port or the string is invalid, this will return null.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class UrlExtractPort {

  @Udf
  public Integer extractPort(
      @UdfParameter(description = "a valid URL to extract a port from") final String input) {
    final Integer port = UrlParser.extract(input, URI::getPort);

    // check for LT 0 because URI::getPort returns -1 if the port
    // does not exist, but UrlParser#extract will return null if
    // the URI is invalid
    return (port == null || port < 0) ? null : port;
  }
}
