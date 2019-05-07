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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestOptionsParserTest {

  @Rule
  public final ExpectedException expectedException = org.junit.rules.ExpectedException.none();

  @Test
  public void shouldParseCommandWithTestFile() throws IOException {
    // When:
    final TestOptions testOptions = TestOptionsParser.parse(new String[]{"foo"}, TestOptions.class);

    // Then:
    assertThat(testOptions.getTestFile(), CoreMatchers.equalTo("foo"));
  }

  @Test
  public void shouldFailWithMissingTestFile() throws IOException {
    // Given:
    final ByteArrayOutputStream errContent = new ByteArrayOutputStream();
    final PrintStream originalErr = System.err;
    System.setErr(new PrintStream(errContent));

    // When:
    final TestOptions testOptions = TestOptionsParser.parse(new String[]{}, TestOptions.class);

    // Then:
    assertTrue(errContent.toString().startsWith("Required arguments are missing: 'test-file'"));
    System.setErr(originalErr);
  }

}