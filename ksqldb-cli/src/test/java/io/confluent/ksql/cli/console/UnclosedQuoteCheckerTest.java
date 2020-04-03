package io.confluent.ksql.cli.console;


import org.junit.Assert;
import org.junit.Test;

public class UnclosedQuoteCheckerTest {

  @Test
  public void shouldFindUnclosedQuote() {
    // Given:
    final String line = "some line 'this is in a comment";

    // Then:
    Assert.assertTrue(UnclosedQuoteChecker.isUnclosedQuote(line));
  }

  @Test
  public void shouldFindUnclosedQuote_escaped() {
    // Given:
    final String line = "some line 'this is in a comment\\'";

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
    final String line = "some line -- 'this is in a quote";

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
