/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import io.confluent.ksql.parser.SqlBaseLexer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public final class GrammarTokenExporter {
  private GrammarTokenExporter() {
  }

  public static void main(final String[] args) throws IOException {
    final List<String> tokens = GrammarTokenExporter.getTokens();
    final List<String> tokensWithQuotes = new ArrayList<>();
    for (String token:tokens) {
      tokensWithQuotes.add("'" + token + "'");
    }
    System.out.println(tokensWithQuotes);
  }

  private static final Pattern pattern = Pattern.compile("(T__[0-9]|\\w+_\\w+|UNRECOGNIZED)");

  public static List<String> getTokens() {
    final List<String> tokens = new ArrayList<>(Arrays.asList(SqlBaseLexer.ruleNames));
    return tokens.stream().filter(pattern.asPredicate().negate()).collect(Collectors.toList());
  }
}
