/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.parser;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class VariableParser {
  private VariableParser() {
  }

  /**
   * Parses a list of Strings of the form, var=val into a map of variables and values.
   */
  public static Map<String, String> getVariables(final List<String> definedVars) {
    if (definedVars == null) {
      return Collections.emptyMap();
    }

    final ImmutableMap.Builder<String, String> variables = ImmutableMap.builder();
    for (String pair : definedVars) {
      final String[] parts = pair.split("=");
      if (parts.length != 2) {
        throw new IllegalArgumentException("Failed to parse argument " + pair
            + ": variables must be defined using '=' (i.e. var=val).");
      }

      variables.put(parts[0], parts[1]);
    }

    return variables.build();
  }
}
