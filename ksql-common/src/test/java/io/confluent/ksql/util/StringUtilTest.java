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
 **/

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class StringUtilTest {

  @Test
  public void testNotCleanIfMissingQuotePrefix() {
    assertCleaned("COLUMN_NAME'", "COLUMN_NAME'");
  }

  @Test
  public void testNotCleanIfMissingQuotePostfix() {
    assertCleaned("'COLUMN_NAME", "'COLUMN_NAME");
  }

  @Test
  public void testCleanQuotesIfQuoted() {
    assertCleaned("'COLUMN_NAME'", "COLUMN_NAME");
  }

  @Test
  public void testDoesNotReduceAnyQuotesIfNotQuotedString() {
    assertCleaned("prefix ''Something in quotes'' postfix",
        "prefix ''Something in quotes'' postfix");
  }

  @Test
  public void testReducesDoubleSingleQuotesInQuotedStringToSingleSingleQuotes() {
    assertCleaned("'prefix ''Something in quotes'' postfix'",
        "prefix 'Something in quotes' postfix");

    assertCleaned("'a '''' b ''' c '''' d '' e '' f '''",
        "a '' b '' c '' d ' e ' f '");
  }

  @Test
  public void testCleanDateFormat() {
    assertCleaned("'yyyy-MM-dd''T''HH:mm:ssX'", "yyyy-MM-dd'T'HH:mm:ssX");
    assertCleaned("'yyyy.MM.dd G ''at'' HH:mm:ss z'", "yyyy.MM.dd G 'at' HH:mm:ss z");
    assertCleaned("'EEE, MMM d, ''''yy'", "EEE, MMM d, ''yy");
    assertCleaned("'hh ''o''clock'' a, zzzz'", "hh 'o'clock' a, zzzz");
    assertCleaned("'YYYY-''W''ww-u'", "YYYY-'W'ww-u");
  }

  private static void assertCleaned(final String input, final String expected){
    final String result = StringUtil.cleanQuotes(input);
    assertThat(result, is(expected));
  }
}