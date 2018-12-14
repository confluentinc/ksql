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

package io.confluent.ksql.cli.console;

import java.util.Objects;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;

/**
 * Trims lines of white space before delegating to the supplied parser.
 *
 * <p>If a user puts spaces after the end of the line continuation char e.g.
 *
 * <pre>
 * {@code show \ <--extra space after '\'.
 * tables;}
 * </pre>
 *
 * <p>then jline won't recognise the '\' as a line continuation char, but will treat it as an
 * escape char.
 *
 * <p>This class trims each line first, before passing on. Thereby ensuring the continuation char
 * is respected.
 */
final class TrimmingParser implements Parser {

  private final Parser delegate;

  TrimmingParser(final Parser delegate) {
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public ParsedLine parse(final String line, final int cursor, final ParseContext context) {
    final String trimmed = line.trim();
    final int adjCursor = adjustCursor(line, trimmed, cursor);
    return delegate.parse(trimmed, adjCursor, context);
  }

  private int adjustCursor(final String origLine, final String trimmedLine, final int origCursor) {
    final int prefixLen = origLine.indexOf(trimmedLine);
    if (origCursor < prefixLen) {
      return 0; // Before first char trimmed line
    }

    if (origCursor < (prefixLen + trimmedLine.length())) {
      return origCursor - prefixLen;  // Within trimmed line
    }

    return trimmedLine.length();  // Past end of trimmed line
  }
}
