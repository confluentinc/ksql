/*
 * Copyright 2018 Confluent Inc.
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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.jline.reader.Parser.ParseContext.ACCEPT_LINE;
import static org.jline.reader.Parser.ParseContext.UNSPECIFIED;

import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.internal.LastControl;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class TrimmingParserTest {

  @Mock
  private Parser delegate;
  @Mock
  private ParsedLine parsedLine;
  private Parser parser;

  @Before
  public void setUp() {
    parser = new TrimmingParser(delegate);

    LastControl.pullMatchers();
  }

  @After
  public void after(){
    LastControl.pullMatchers();
  }

  @Test
  public void shouldTrimWhiteSpaceAndReturnLine() {
    expect(delegate.parse("line \t containing \t space", 0, UNSPECIFIED))
        .andReturn(parsedLine);
    replay(delegate);

    final ParsedLine line = parser.parse(" \t line \t containing \t space \t ", 0, UNSPECIFIED);

    assertThat(line, is(parsedLine));
  }

  @Test
  public void shouldAdjustCursorIfInLeftWhiteSpace() {
    expect(delegate.parse(anyString(), eq(0), anyObject()))
        .andReturn(parsedLine).anyTimes();
    replay(delegate);

    parser.parse("  line  ", 0, UNSPECIFIED);
    parser.parse("  line  ", 1, UNSPECIFIED);
    parser.parse("  line  ", 2, UNSPECIFIED);
  }

  @Test
  public void shouldAdjustCursorIfInRightWhiteSpace() {
    expect(delegate.parse(anyString(), eq(4), anyObject()))
        .andReturn(parsedLine).anyTimes();
    replay(delegate);

    parser.parse("  line  ", 6, UNSPECIFIED);
    parser.parse("  line  ", 7, UNSPECIFIED);
    parser.parse("  line  ", 8, UNSPECIFIED);
  }

  @Test
  public void shouldAdjustCursorWhenInTrimmedResult() {
    expect(delegate.parse(anyString(), eq(2), anyObject()))
        .andReturn(parsedLine);
    replay(delegate);

    parser.parse("  line  ", 4, UNSPECIFIED);
  }

  @Test
  public void shouldPassThroughAnyLineThatDoesNotNeedTrimming() {
    expect(delegate.parse("line without spaces at ends", 4, ACCEPT_LINE))
        .andReturn(parsedLine);
    replay(delegate);

    final ParsedLine line = parser.parse("line without spaces at ends", 4, ACCEPT_LINE);

    assertThat(line, is(parsedLine));
  }
}