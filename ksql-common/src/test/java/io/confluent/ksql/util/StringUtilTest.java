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
  public void testCleanQuotes() {
    assertThat("COLUMN_NAME",
        is(StringUtil.cleanQuotes("'COLUMN_NAME'")));
  }

  @Test
  public void testCleanDateFormat() {
    assertDateFormat("yyyy-MM-dd''T''HH:mm:ssX", "yyyy-MM-dd'T'HH:mm:ssX");
    assertDateFormat("'yyyy-MM-dd''T''HH:mm:ssX'", "yyyy-MM-dd'T'HH:mm:ssX");
    assertDateFormat("yyyy.MM.dd G ''at'' HH:mm:ss z", "yyyy.MM.dd G 'at' HH:mm:ss z");
    assertDateFormat("'yyyy.MM.dd G ''at'' HH:mm:ss z'", "yyyy.MM.dd G 'at' HH:mm:ss z");
    assertDateFormat("EEE, MMM d, ''''yy","EEE, MMM d, ''yy");
    assertDateFormat("'EEE, MMM d, ''''yy'", "EEE, MMM d, ''yy");
    assertDateFormat("hh ''o''clock'' a, zzzz", "hh 'o''clock' a, zzzz");
    assertDateFormat("'hh ''o''clock'' a, zzzz'", "hh 'o''clock' a, zzzz");
    assertDateFormat("YYYY-''W''ww-u", "YYYY-'W'ww-u");
    assertDateFormat("'YYYY-''W''ww-u'", "YYYY-'W'ww-u");
  }

  private void assertDateFormat(final String input, final String expected){
    final String result = StringUtil.cleanQuotes(input);
    assertThat(result, is(expected));
  }

}