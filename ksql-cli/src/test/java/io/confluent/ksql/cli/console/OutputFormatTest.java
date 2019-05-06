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

import static io.confluent.ksql.cli.console.OutputFormat.VALID_FORMATS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class OutputFormatTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldReturnAllValues() {
    assertThat(VALID_FORMATS, is("'JSON', 'TABULAR'"));
  }

  @Test
  public void shouldResolve() {
    assertThat(OutputFormat.valueOf("JSON"), is(OutputFormat.JSON));
    assertThat(OutputFormat.valueOf("TABULAR"), is(OutputFormat.TABULAR));
  }

  @Test
  public void shouldThrowOnUnknownFormat() {
    // Expect:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unknown Output format: This-is-unknown. "
        + "Valid values are: " + VALID_FORMATS);

    // When:
    OutputFormat.get("This-is-unknown");
  }
}