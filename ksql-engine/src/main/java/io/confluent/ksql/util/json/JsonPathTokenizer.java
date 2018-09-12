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

package io.confluent.ksql.util.json;

import static com.google.common.base.Verify.verify;
import static java.lang.Character.isLetterOrDigit;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.google.common.base.VerifyException;
import com.google.common.collect.AbstractIterator;
import io.confluent.ksql.util.KsqlException;

public class JsonPathTokenizer
    extends AbstractIterator<String> {
  private static final char QUOTE = '\"';
  private static final char BACKSLASH = '\\';
  private static final char DOT = '.';
  private static final char OPEN_BRACKET = '[';
  private static final char CLOSE_BRACKET = ']';
  // CHECKSTYLE_RULES.OFF: AvoidEscapedUnicodeCharacters
  private static final char UNICODE_CARET = '\u2038';
  // CHECKSTYLE_RULES.ON: AvoidEscapedUnicodeCharacters

  private final String path;
  private int index;

  public JsonPathTokenizer(final String path) {
    this.path = requireNonNull(path, "path is null");

    if (path.isEmpty()) {
      throw invalidJsonPath();
    }

    // skip the start token
    match('$');
  }

  @Override
  protected String computeNext() {
    if (!hasNextCharacter()) {
      return endOfData();
    }

    if (tryMatch(DOT)) {
      return matchPathSegment();
    }

    if (tryMatch(OPEN_BRACKET)) {
      final String token = tryMatch(QUOTE) ? matchQuotedSubscript() : matchUnquotedSubscript();

      match(CLOSE_BRACKET);
      return token;
    }

    throw invalidJsonPath();
  }

  private String matchPathSegment() {
    // seek until we see a special character or whitespace
    final int start = index;
    while (hasNextCharacter() && isUnquotedPathCharacter(peekCharacter())) {
      nextCharacter();
    }
    final int end = index;

    final String token = path.substring(start, end);

    // an empty unquoted token is not allowed
    if (token.isEmpty()) {
      throw invalidJsonPath();
    }

    return token;
  }

  private static boolean isUnquotedPathCharacter(final char c) {
    return c == ':' || isUnquotedSubscriptCharacter(c);
  }

  private String matchUnquotedSubscript() {
    // seek until we see a special character or whitespace
    final int start = index;
    while (hasNextCharacter() && isUnquotedSubscriptCharacter(peekCharacter())) {
      nextCharacter();
    }
    final int end = index;

    final String token = path.substring(start, end);

    // an empty unquoted token is not allowed
    if (token.isEmpty()) {
      throw invalidJsonPath();
    }

    return token;
  }

  private static boolean isUnquotedSubscriptCharacter(final char c) {
    return c == '_' || isLetterOrDigit(c);
  }

  private String matchQuotedSubscript() {
    // quote has already been matched

    // seek until we see the close quote
    final StringBuilder token = new StringBuilder();
    boolean escaped = false;

    while (hasNextCharacter() && (escaped || peekCharacter() != QUOTE)) {
      if (escaped) {
        switch (peekCharacter()) {
          case QUOTE:
          case BACKSLASH:
            token.append(peekCharacter());
            break;
          default:
            throw invalidJsonPath();
        }
        escaped = false;
      } else {
        switch (peekCharacter()) {
          case BACKSLASH:
            escaped = true;
            break;
          case QUOTE:
            throw new VerifyException("Should be handled by loop condition");
          default:
            token.append(peekCharacter());
        }
      }
      nextCharacter();
    }
    if (escaped) {
      verify(!hasNextCharacter(), "Loop terminated after escape while there is still input");
      throw invalidJsonPath();
    }

    match(QUOTE);

    return token.toString();
  }

  private boolean hasNextCharacter() {
    return index < path.length();
  }

  private void match(final char expected) {
    if (!tryMatch(expected)) {
      throw invalidJsonPath();
    }
  }

  private boolean tryMatch(final char expected) {
    if (!hasNextCharacter() || peekCharacter() != expected) {
      return false;
    }
    index++;
    return true;
  }

  private void nextCharacter() {
    index++;
  }

  private char peekCharacter() {
    return path.charAt(index);
  }

  private KsqlException invalidJsonPath() {
    return new KsqlException(format("Invalid JSON path: '%s'", path));
  }

  @Override
  public String toString() {
    return path.substring(0, index) + UNICODE_CARET + path.substring(index);
  }
}

