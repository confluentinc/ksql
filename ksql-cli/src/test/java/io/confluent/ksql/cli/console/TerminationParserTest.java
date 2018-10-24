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
 */

package io.confluent.ksql.cli.console;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.Parser.ParseContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class TerminationParserTest {

  private static final String UNTERMINATED_LINE = "an unterminated line";
  private static final String TERMINATED_LINE = "a terminated line;";

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private Parser delegate;
  @Mock
  private ParsedLine parsedLine;
  private TerminationParser parser;

  @Before
  public void setUp() {
    parser = new TerminationParser(delegate);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullParam() {
    new TerminationParser(null);
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
  public void shouldReturnResultIfEmptyLine() {
    // Given:
    givenDelegateWillReturn("");

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldReturnResultIfLineTerminated() {
    // Given:
    givenDelegateWillReturn(TERMINATED_LINE);

    // When:
    final ParsedLine result = parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);

    // Then:
    assertThat(result, is(parsedLine));
  }

  @Test
  public void shouldThrowIfUnterminatedAcceptLine() {
    // Given:
    expectedException.expect(EOFError.class);
    expectedException.expectMessage("Missing termination char");

    givenDelegateWillReturn(UNTERMINATED_LINE);

    // When:
    parser.parse("what ever", 0, ParseContext.ACCEPT_LINE);
  }

  private void givenDelegateWillReturn(final String line) {
    EasyMock.expect(parsedLine.line()).andReturn(line).anyTimes();
    EasyMock.expect(delegate.parse(anyObject(), anyInt(), anyObject())).andReturn(parsedLine);
    EasyMock.replay(delegate, parsedLine);
  }
}