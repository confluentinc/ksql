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

package io.confluent.ksql.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WelcomeMsgUtilsTest {

  private StringWriter stringWriter;
  private PrintWriter realPrintWriter;
  @Mock
  private PrintWriter mockPrintWriter;

  @Before
  public void setUp() {
    stringWriter = new StringWriter();
    realPrintWriter = new PrintWriter(stringWriter);
  }

  @Test
  public void shouldOutputLongWelcomeMessageIfConsoleIsWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is(
        "                  \n"
        + "                  ===========================================\n"
        + "                  =       _              _ ____  ____       =\n"
        + "                  =      | | _____  __ _| |  _ \\| __ )      =\n"
        + "                  =      | |/ / __|/ _` | | | | |  _ \\      =\n"
        + "                  =      |   <\\__ \\ (_| | | |_| | |_) |     =\n"
        + "                  =      |_|\\_\\___/\\__, |_|____/|____/      =\n"
        + "                  =                   |_|                   =\n"
        + "                  =        The Database purpose-built       =\n"
        + "                  =        for stream processing apps       =\n"
        + "                  ===========================================\n"
        + "\n"
        + "Copyright 2017-2022 Confluent Inc.\n"
        + "\n")
    );
  }

  @Test
  public void shouldOutputShortWelcomeMessageIfConsoleNotWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(35, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is("ksqlDB, Copyright 2017-2022 Confluent Inc.\n\n"));
  }

  @Test
  public void shouldFlushWriterWhenOutputtingLongMessage() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, mockPrintWriter);

    // Then:
    Mockito.verify(mockPrintWriter).flush();
  }

  @Test
  public void shouldFlushWriterWhenOutputtingShortMessage() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(10, mockPrintWriter);

    // Then:
    Mockito.verify(mockPrintWriter).flush();
  }
}
