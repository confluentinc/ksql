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

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.util.function.Predicate;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.internal.LastControl;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.Parser.ParseContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class KsqlLineParserTest {

  private static final String UNTERMINATED_LINE = "an unterminated line";
  private static final String TERMINATED_LINE = "a terminated line;";

  @Mock
  private Parser delegate;
  @Mock
  private ParsedLine parsedLine;
  @Mock
  private Predicate<String> cliLinePredicate;
  private KsqlLineParser parser;

  @Before
  public void setUp() {
    EasyMock.expect(cliLinePredicate.test(anyString())).andReturn(false);
    EasyMock.replay(cliLinePredicate);

    parser = new KsqlLineParser(delegate, cliLinePredicate);

    LastControl.pullMatchers();
  }

  @After
  public void a() {
    LastControl.pullMatchers();
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullParam() {
    new KsqlLineParser(null, cliLinePredicate);
  }

  @Test
  public void shouldCallDelegateWithCorrectParams() {
    // Given:
    EasyMock.expect(parsedLine.line()).andReturn(TERMINATED_LINE).anyTimes();
    EasyMock.expect(delegate.parse("some-string", 55, ParseContext.ACCEPT_LINE))
        .andReturn(parsedLine);

    EasyMock.replay(delegate, parsedLine);

    // When:
    parser.parse("some-string", 55, ParseContext.ACCEPT_LINE);

    // Then:
    EasyMock.verify(delegate);
  }

  @Test
  public void shouldReturnResultIfNotAcceptLine() {
    // Given:
    givenDelegateWillReturn(UNTERMINATED_LINE);

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.COMPLETE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldAcceptIfEmptyLine() {
    // Given:
    givenDelegateWillReturn("");

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldAcceptIfLineTerminated() {
    // Given:
    givenDelegateWillReturn(TERMINATED_LINE);

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldAcceptIfPredicateReturnsTrue() {
    // Given:
    givenPredicateWillReturnTrue();
    givenDelegateWillReturn(UNTERMINATED_LINE);

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldNotAcceptUnterminatedAcceptLine() {
    // Given:
    givenDelegateWillReturn(UNTERMINATED_LINE);

    // When:
    final EOFError e = assertThrows(
        EOFError.class,
        () -> parser.parse("what ever", 0, ParseContext.ACCEPT_LINE)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Missing termination char"));
  }

  @Test
  public void shouldAlwaysAcceptCommentLines() {
    // Given:
    givenDelegateWillReturn(" -- this is a comment");

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldAcceptTerminatedLineEndingInComment() {
    // Given:
    givenDelegateWillReturn(TERMINATED_LINE + " -- this is a comment");

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldNotAcceptUnterminatedLineEndingInComment() {
    // Given:
    givenDelegateWillReturn(UNTERMINATED_LINE + " -- this is a comment");

    // When:
    final EOFError e = assertThrows(
        EOFError.class,
        () -> parser.parse("what ever", 0, ParseContext.ACCEPT_LINE)
    );

    //Then:
    assertThat(e.getMessage(), containsString("Missing termination char"));
  }

  private void givenDelegateWillReturn(final String line) {
    EasyMock.expect(parsedLine.line()).andReturn(line).anyTimes();
    EasyMock.expect(delegate.parse(anyObject(), anyInt(), anyObject())).andReturn(parsedLine);
    EasyMock.replay(delegate, parsedLine);
  }

  private void givenPredicateWillReturnTrue() {
    EasyMock.reset(cliLinePredicate);
    EasyMock.expect(cliLinePredicate.test(anyString())).andReturn(true);
    EasyMock.replay(cliLinePredicate);
  }
}