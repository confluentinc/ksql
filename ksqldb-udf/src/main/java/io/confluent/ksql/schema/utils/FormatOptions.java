/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.schema.utils;

import static java.util.Objects.requireNonNull;

import java.util.function.Predicate;

public final class FormatOptions {

  private static final String ESCAPE = "`";
  private final Predicate<String> addQuotesPredicate;

  public static FormatOptions none() {
    return new FormatOptions(word -> true);
  }

  /**
   * @return options that escape nothing
   * @apiNote this is <i>dangerous</i> and could cause reserved identifiers
   *          to be mangled. Use this API sparingly (e.g. in logging error
   *          messages)
   */
  public static FormatOptions noEscape() {
    return new FormatOptions(word -> false);
  }

  /**
   * Construct instance.
   *
   * <p>The {@code addQuotesPredicate} allows code that lives in the common module
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
   * @param addQuotesPredicate predicate to test if a word should be quoted.
   * @return instance of {@code FormatOptions}.
   */
  public static FormatOptions of(final Predicate<String> addQuotesPredicate) {
    return new FormatOptions(addQuotesPredicate);
  }

  private FormatOptions(final Predicate<String> fieldNameEscaper) {
    this.addQuotesPredicate = requireNonNull(fieldNameEscaper, "addQuotesPredicate");
  }

  private boolean shouldQuote(final String word) {
    return addQuotesPredicate.test(word);
  }

  /**
   * Escapes {@code word} if it is a reserved word, determined by {@link #shouldQuote(String)}.
   *
   * @param word the word to escape
   * @return {@code word}, if it is not a reserved word, otherwise {@code word} wrapped in
   *         back quotes
   */
  public String escape(final String word) {
    return shouldQuote(word) ? ESCAPE + word + ESCAPE : word;
  }
}
