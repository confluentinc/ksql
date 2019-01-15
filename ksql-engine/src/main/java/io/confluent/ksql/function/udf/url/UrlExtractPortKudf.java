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

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.util.KsqlConstants;
import java.net.URI;

@UdfDescription(
        name = UrlExtractPortKudf.NAME,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Extracts the port from an application/x-www-form-urlencoded encoded String."
                      + " If there is no port or the string is invalid, this will return null.")
public class UrlExtractPortKudf {

  static final String NAME = "url_extract_port";

  @Udf(description = "Extracts the port from an application/x-www-form-urlencoded encoded String"
                     + " If there is no port or the string is invalid, this will return null.")
  public Integer extractPort(final String input) {
    final Integer port = UrlParser.extract(input, URI::getPort);
    return (port == null || port < 0) ? null : port;
  }
}
