/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udf.url;

import java.net.URI;
import java.util.Iterator;
import com.google.common.base.Splitter;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class UrlExtractParameterKudf extends UrlParser implements Kudf {

  private static final Splitter PARAM_SPLITTER = Splitter.on('&');
  private static final Splitter KV_SPLITTER = Splitter.on('=').limit(2);

  @Override
  public Object evaluate(Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("url_extract_parameter udf requires two input arguments.");
    }

    URI uri = parseUrl(args[0].toString());
    if (uri == null) {
      return null;
    }

    String query = uri.getQuery();
    if (query == null) {
      return null;
    }

    String paramToFind = args[1].toString();
    for (String thisParam : PARAM_SPLITTER.split(query)) {
      Iterator<String> arg = KV_SPLITTER.split(thisParam).iterator();
      if (arg.next().equalsIgnoreCase(paramToFind)) {
        if (arg.hasNext()) {
          return arg.next();
        }
        // key was found but value is empty
        return null;
      }
    }
    return null;
  }
}
