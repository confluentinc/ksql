/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.parser;

import com.google.common.collect.ImmutableSet;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Vocabulary;

public final class KsqlParserUtil {

  private KsqlParserUtil() {
  }

  public static boolean isReserved(final String token) {

    final SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(
            new CaseInsensitiveStream(CharStreams.fromString(token)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    final SqlBaseParser.NonReservedContext nonReservedContext = sqlBaseParser.nonReserved();
    if (nonReservedContext.exception == null) {
      // if the token does not match "nonReserved", then we expect the above
      // method call to "throw" an exception
      return false;
    }

    // this part was taken directly from FunctionNameValidator
    final Vocabulary vocabulary = SqlBaseParser.VOCABULARY;
    final int tokens = vocabulary.getMaxTokenType();
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();

    for (int i = 0; i < tokens; i++) {
      final String symbolicName = vocabulary.getSymbolicName(i);
      if (symbolicName != null) {
        final String keyWord = symbolicName.toLowerCase();
        builder.add(keyWord);
      }
    }

    final ImmutableSet<String> allVocab = builder.build();

    return allVocab.contains(token.toLowerCase());
  }
}
