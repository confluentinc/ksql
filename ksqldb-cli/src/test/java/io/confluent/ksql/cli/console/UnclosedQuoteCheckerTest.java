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

package io.confluent.ksql.cli.console;

import org.junit.Assert;
import org.junit.Test;

public class UnclosedQuoteCheckerTest {

  @Test
  public void shouldFindUnclosedQuote() {
    // Given:
    final String line = "some line 'this is in a quote";

    // Then:
    Assert.assertTrue(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldFindUnclosedQuote_escaped() {
    // Given:
    final String line = "some line 'this is in a quote\\'";

    // Then:
    Assert.assertTrue(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldNotFindUnclosedQuote_endsQuote() {
    // Given:
    final String line = "some line 'this is in a quote'";

    // Then:
    Assert.assertFalse(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldNotFindUnclosedQuote_endsNonQuote() {
    // Given:
    final String line = "some line 'this is in a quote' more";

    // Then:
    Assert.assertFalse(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldNotFindUnclosedQuote_inComment() {
    // Given:
    final String line = "some line -- 'this is in a comment";

    // Then:
    Assert.assertFalse(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldNotFindUnclosedQuote_onlyComment() {
    // Given:
    final String line = "some line -- this is a comment";

    // Then:
    Assert.assertFalse(UnclosedQuoteChecker.isUnclosedQuote(line));
  }
}
