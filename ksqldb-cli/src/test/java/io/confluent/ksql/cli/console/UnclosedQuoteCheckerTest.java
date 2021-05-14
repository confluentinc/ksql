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

package io.confluent.ksql.cli.console;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class UnclosedQuoteCheckerTest {

  @Test
  public void shouldFindUnclosedQuote() {
    // Given:
    final String line = "some line 'this is in a quote";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldFindUnclosedQuote_commentCharsInside() {
    // Given:
    final String line = "some line 'this is in a quote -- not a comment";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldNotFindUnclosedQuote_commentCharsInside() {
    // Given:
    final String line = "some line 'this is in a quote -- not a comment'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldFindUnclosedQuote_escaped() {
    // Given:
    final String line = "some line 'this is in a quote\\'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldFindUnclosedQuote_escapedEscape() {
    // Given:
    final String line = "some line 'this is in a quote\\\\'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldFindUnclosedQuote_escapedThree() {
    // Given:
    final String line = "some line 'this is in a quote\\\'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldFindUnclosedQuote_twoQuote() {
    // Given:
    final String line = "some line 'this is in a quote''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldNotFindUnclosedQuote_endsQuote() {
    // Given:
    final String line = "some line 'this is in a quote'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_endsNonQuote() {
    // Given:
    final String line = "some line 'this is in a quote' more";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_containsDoubleQuote() {
    // Given:
    final String line = "some line 'this is \"in\" a quote'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_containsUnclosedDoubleQuote() {
    // Given:
    final String line = "some line 'this is \"in a quote'";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_escaped() {
    // Given:
    final String line = "some line 'this is in a quote\\''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_twoQuote() {
    // Given:
    final String line = "some line 'this is in a quote'''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldFindUnclosedQuote_manyQuote() {
    // Given:
    final String line = "some line 'this is in a quote''''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(true));
  }

  @Test
  public void shouldNotFindUnclosedQuote_manyQuote() {
    // Given:
    final String line = "some line 'this is in a quote'''''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_escapedAndMultipleQuotes() {
    // Given:
    final String line = "some line 'this is in a quote\\''''";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_inComment() {
    // Given:
    final String line = "some line -- 'this is in a comment";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_onlyComment() {
    // Given:
    final String line = "some line -- this is a comment";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }

  @Test
  public void shouldNotFindUnclosedQuote_commentAfterQuote() {
    // Given:
    final String line = "some line 'quoted text' -- this is a comment";

    // Then:
    assertThat(UnclosedQuoteChecker.isUnclosedQuote(line), is(false));
  }
}
