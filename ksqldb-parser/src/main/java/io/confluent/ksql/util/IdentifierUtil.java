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

import io.confluent.ksql.parser.CaseInsensitiveStream;
import io.confluent.ksql.parser.SqlBaseLexer;
import io.confluent.ksql.parser.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

public final class IdentifierUtil {

  private IdentifierUtil() {

  }

  /**
   * @param identifier  the identifier
   * @return whether or not {@code identifier} is a valid identifier without quotes
   */
  public static boolean isValid(final String identifier) {
    final SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(
        new CaseInsensitiveStream(CharStreams.fromString(identifier)));
    final CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
    final SqlBaseParser sqlBaseParser = new SqlBaseParser(tokenStream);

    // don't log or print anything in the case of error since this is expected
    // for this method
    sqlBaseLexer.removeErrorListeners();
    sqlBaseParser.removeErrorListeners();

    sqlBaseParser.identifier();

    // needs quotes if the `identifier` was not able to read the entire line
    return sqlBaseParser.getNumberOfSyntaxErrors() == 0
        && sqlBaseParser.getCurrentToken().getCharPositionInLine() == identifier.length();
  }

  /**
   * @param identifier the identifier
   * @return whether or not {@code identifier} needs quotes to be parsed as the same identifier
   */
  public static boolean needsQuotes(final String identifier) {
    return !(isValid(identifier) && upperCase(identifier));
  }

  private static boolean upperCase(final String identifier) {
    return identifier.toUpperCase().equals(identifier);
  }
}
