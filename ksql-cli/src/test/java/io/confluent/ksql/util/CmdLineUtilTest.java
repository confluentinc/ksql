/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.List;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

public class CmdLineUtilTest {

  @Test
  public void shouldSplitAndRemoveQuotesOnEmptyString() {
    assertSplit("");
  }

  @Test
  public void shouldSplitAndRemoveQuotesOnSimpleArgs() {
    assertSplit("arg0 arg1", "arg0", "arg1");
  }

  @Test
  public void shouldSplitSimpleQuotedArgs() {
    assertSplit("'arg0' 'arg1'", "'arg0'", "'arg1'");
  }

  @Test
  public void shouldSplitArgsWithTabs() {
    assertSplit("Arg \t Arg0 \t Arg1 \t ", "Arg", "Arg0", "Arg1");
  }

  @Test
  public void shouldSplitQuotedArgsWithLeadingTrailingWhiteSpace() {
    assertSplit("' \t arg0 \t ' ' arg1 '", "' \t arg0 \t '", "' arg1 '");
  }

  @Test
  public void shouldNotSplitUnterminatedQuotes() {
    assertSplit("'some unterminated string", "'some unterminated string");
  }

  @Test
  public void shouldNotSplitJustUnterminatedQuote() {
    assertSplit("'", "'");
  }

  @Test
  public void shouldSplitEmptyQuotedString() {
    assertSplit("''", "''");
  }

  @Test
  public void shouldSplitThreeSingleQuotes() {
    assertSplit("'''", "''");
  }

  @Test
  public void shouldSplitSingleQuotedEscapedQuote() {
    assertSplit("''''", "'''");
  }

  @Test
  public void shouldSplitStringWithEmbeddedQuoted() {
    assertSplit("'string''with''embedded''quotes'", "'string'with'embedded'quotes'");
  }

  @Test
  public void shouldSplitStringWithEmbeddedDoubleQuoted() {
    assertSplit("string\"with\"double\"quotes", "string\"with\"double\"quotes");
  }

  @Test
  public void shouldSplitQuotedStringWithEmbeddedDoubleQuoted() {
    assertSplit("'string\"with\"double\"quotes'", "'string\"with\"double\"quotes'");
  }

  @Test
  public void shouldIgnoreLeadingWhiteSpace() {
    assertSplit(" some thing", "some", "thing");
  }

  @Test
  public void shouldHandleCharactersTrailingAfterQuoteEnds() {
    assertSplit("'thing'one 'thing'two", "'thing'one", "'thing'two");
  }

  @Test
  public void shouldRemoveQuotesFromUnQuotedString() {
    assertMatchedQuotesRemoved(" some input ", " some input ");
  }

  @Test
  public void shouldRemoveSingleQuotes() {
    assertMatchedQuotesRemoved("' some input '", " some input ");
  }

  @Test
  public void shouldNotRemoveUnterminatedQuote() {
    assertMatchedQuotesRemoved("' some input ", "' some input ");
  }

  @Test
  public void shouldNotRemoveIfOnlyTrailingQuote() {
    assertMatchedQuotesRemoved("something'", "something'");
  }

  @Test
  public void shouldRemoveMultipleMatchingQuotes() {
    assertMatchedQuotesRemoved("a'quoted1'connnected'quoted2'a", "aquoted1connnectedquoted2a");
  }

  @Test
  public void shouldCorrectlyHandleEscapedQuotesWhenRemovingMatchedQuotes() {
    assertMatchedQuotesRemoved("'a '''' b ''' c '''' d '' e '' f '''", "a '' b ' c ' d  e  f '''");
  }

  private static void assertSplit(final String input, final String... expected) {
    final List<String> result = CmdLineUtil.splitByUnquotedWhitespace(input);
    assertThat(result, CoreMatchers.is(Arrays.asList(expected)));
  }

  private static void assertMatchedQuotesRemoved(final String input, final String expected) {
    final String result = CmdLineUtil.removeMatchedSingleQuotes(input);
    assertThat(result, is(expected));
  }
}