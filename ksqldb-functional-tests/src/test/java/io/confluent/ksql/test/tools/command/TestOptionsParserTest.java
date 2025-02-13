/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.test.tools.command;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import io.confluent.ksql.tools.test.command.TestOptionsParser;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.Test;

public class TestOptionsParserTest {

  @Test
  public void shouldParseCommandWithAllFiles() throws IOException {
    // When:
    final TestOptions testOptions = TestOptionsParser.parse(new String[]{"--sql-file", "foo", "--input-file", "bar", "--output-file", "tab"}, TestOptions.class);

    // Then:
    assert testOptions != null;
    assertThat(testOptions.getStatementsFile(), equalTo("foo"));
    assertThat(testOptions.getInputFile(), equalTo("bar"));
    assertThat(testOptions.getOutputFile(), equalTo("tab"));
  }

  @Test
  public void shouldPrintHelp() throws IOException {
    // Given:
    final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent, true, "UTF-8"));

    // When:
    TestOptionsParser.parse(new String[]{"-h"}, TestOptions.class);

    // Then:
    System.setOut(System.out);
    assertThat(outContent.toString("UTF-8"), containsString("ksql-test-runner - The KSQL testing tool"));
    assertThat(outContent.toString("UTF-8"), containsString("--input-file <inputFile>"));
    assertThat(outContent.toString("UTF-8"), containsString("--output-file <outputFile>"));
    assertThat(outContent.toString("UTF-8"), containsString("--sql-file <statementsFile>"));
  }

  @Test
  public void shouldFailWithMissingTestFile() throws IOException {
    // Given:
    final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    final PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent, true, "UTF-8"));

    // When:
    TestOptionsParser.parse(new String[]{}, TestOptions.class);

    // Then:
    assertTrue(errContent.toString("UTF-8").startsWith("Required option '--sql-file' is missing"));
    System.setErr(originalErr);
  }

}