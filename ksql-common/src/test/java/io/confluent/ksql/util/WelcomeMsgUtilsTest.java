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

package io.confluent.ksql.util;

import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.easymock.EasyMock.anyString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(EasyMockRunner.class)
public class WelcomeMsgUtilsTest {

  private StringWriter stringWriter;
  private PrintWriter realPrintWriter;
  @Mock
  private PrintWriter mockPrintWriter;

  @Before
  public void setUp() throws Exception {
    stringWriter = new StringWriter();
    realPrintWriter = new PrintWriter(stringWriter);
  }

  @Test
  public void shouldOutputLongWelcomeMessageIfConsoleIsWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is(
        "                     ======================================\n"
        + "                     =      _  __ _____  ____  _          =\n"
        + "                     =     | |/ // ____|/ __ \\| |         =\n"
        + "                     =     | ' /| (___ | |  | | |         =\n"
        + "                     =     |  <  \\___ \\| |  | | |         =\n"
        + "                     =     | . \\ ____) | |__| | |____     =\n"
        + "                     =     |_|\\_\\_____/ \\___\\_\\______|    =\n"
        + "                     =                                    =\n"
        + "                     =   Streaming SQL Engine for Kafka   =\n"
        + "                     ======================================\n"
        + "\n"
        + "Copyright 2018 Confluent Inc.\n"
        + "\n")
    );
  }

  @Test
  public void shouldOutputShortWelcomeMessageIfConsoleNotWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(35, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is("KSQL, Copyright 2018 Confluent Inc.\n\n"));
  }

  @Test
  public void shouldFlushWriterWhenOutputingLongMessage() {
    // Given:
    mockPrintWriter.println(anyString());
    EasyMock.expectLastCall().anyTimes();
    mockPrintWriter.println();
    EasyMock.expectLastCall().anyTimes();
    mockPrintWriter.flush();
    EasyMock.expectLastCall();
    EasyMock.replay(mockPrintWriter);

    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, mockPrintWriter);

    // Then:
    EasyMock.verify(mockPrintWriter);
  }

  @Test
  public void shouldFlushWriterWhenOutputingShortMessage() {
    // Given:
    mockPrintWriter.println(anyString());
    EasyMock.expectLastCall().anyTimes();
    mockPrintWriter.println();
    EasyMock.expectLastCall().anyTimes();
    mockPrintWriter.flush();
    EasyMock.expectLastCall();
    EasyMock.replay(mockPrintWriter);

    // When:
    WelcomeMsgUtils.displayWelcomeMessage(10, mockPrintWriter);

    // Then:
    EasyMock.verify(mockPrintWriter);
  }
}