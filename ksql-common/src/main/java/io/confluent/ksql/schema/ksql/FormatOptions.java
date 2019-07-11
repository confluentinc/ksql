/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import static java.util.Objects.requireNonNull;

import java.util.function.Predicate;

public final class FormatOptions {

  private final Predicate<String> reservedWordPredicate;

  public static FormatOptions none() {
    return new FormatOptions(word -> true);
  }

  /**
   * Construct instance.
   *
   * <p>The {@code reservedWordPredicate} allows code that lives in the common module
   * to be wired up to the set of reserved words defined in the parser module. Wire this up to
   * {@code ParserUtil::isReservedIdentifier}.
   *
   * <p>If using this type in a module that does <i>not</i> have access to the parser, then the
   * <i>safest</i> option is to pass in predicate that always returns true, which will always
   * escape field names by wrapping them in back quotes.
   *
   * <p>Where the predicate returns {@code true} a field name will be escaped by enclosing in
   * quotes. NB: this also makes the field name case-sensitive. So care must be taken to ensure
   * field names have the correct case.
   *
   * @param reservedWordPredicate predicate to test if a word is a reserved in SQL syntax.
   * @return instance of {@code FormatOptions}.
   */
  public static FormatOptions of(final Predicate<String> reservedWordPredicate) {
    return new FormatOptions(reservedWordPredicate);
  }

  private FormatOptions(final Predicate<String> fieldNameEscaper) {
    this.reservedWordPredicate = requireNonNull(fieldNameEscaper, "reservedWordPredicate");
  }

  public boolean isReservedWord(final String word) {
    return reservedWordPredicate.test(word);
  }
}
