/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.cli.console;

import java.util.Objects;
import java.util.function.Predicate;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;

/**
 * Ensures complete lines are either terminated with a semi-colon or are cli commands.
 */
final class KsqlLineParser implements Parser {

  private static final String TERMINATION_CHAR = ";";

  private final Parser delegate;
  private final Predicate<String> cliCmdPredicate;

  KsqlLineParser(
      final Parser delegate,
      final Predicate<String> cliCmdPredicate
  ) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.cliCmdPredicate = Objects.requireNonNull(cliCmdPredicate, "cliCmdPredicate");
  }

  @Override
  public ParsedLine parse(final String line, final int cursor, final ParseContext context) {
    final ParsedLine parsed = delegate.parse(line, cursor, context);

    if (cliCmdPredicate.test(line)) {
      return parsed;
    }

    if (context != ParseContext.ACCEPT_LINE) {
      return parsed;
    }

    final String bare = CommentStripper.strip(parsed.line());
    if (bare.isEmpty()) {
      return parsed;
    }

    if (!bare.endsWith(TERMINATION_CHAR)) {
      throw new EOFError(-1, -1, "Missing termination char", "termination char");
    }

    return parsed;
  }
}
