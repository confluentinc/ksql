/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import java.util.List;

public class StringUtil {

  public static String cleanQuotes(final String stringWithQuotes) {
    // TODO: move check to grammar
    if (stringWithQuotes.startsWith("'") && stringWithQuotes.endsWith("'")) {
      return stringWithQuotes.substring(1, stringWithQuotes.length() - 1);
    }
    return stringWithQuotes;
  }

  public static String join(final String delimiter, final List<? extends Object> objs) {
    final StringBuilder sb = new StringBuilder();
    int cnt = 0;
    for (final Object obj : objs) {
      if (cnt > 0) {
        sb.append(delimiter);
      }
      sb.append(obj);
      cnt += 1;
    }
    return sb.toString();
  }

}
