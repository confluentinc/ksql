package io.confluent.ksql.parser;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class VariableParser {

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
        throw new IllegalArgumentException("Variables must be defined using '=' (i.e. var=val).");
      }

      variables.put(parts[0], parts[1]);
    }

    return variables.build();
  }
}
