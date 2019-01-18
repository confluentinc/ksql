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
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

import java.net.URI;

@UdfDescription(
        name = UrlExtractPathKudf.NAME,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Extracts the path of an application/x-www-form-urlencoded "
                      + "encoded String input")
public class UrlExtractPathKudf {

  static final String NAME = "url_extract_path";

  @Udf(description = "Extracts the path of an application/x-www-form-urlencoded "
                             + "encoded String input")
  public String extractPath(
      @UdfParameter(value = "input", description = "a valid URL to extract the path from")
      final String input) {
    // A path component, consisting of a sequence of path segments separated by a slash (/). A path
    // is always defined for a URI, though the defined path may be empty (zero length).
    // source: https://en.wikipedia.org/wiki/Uniform_Resource_Identifier#Definition
    return UrlParser.extract(input, URI::getPath);
  }
}
