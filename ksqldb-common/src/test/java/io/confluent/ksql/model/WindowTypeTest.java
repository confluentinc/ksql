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

package io.confluent.ksql.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WindowTypeTest {

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void shouldParseSession() {
    assertThat(WindowType.of("SESSION"), is(WindowType.SESSION));
  }

  @Test
  public void shouldParseHopping() {
    assertThat(WindowType.of("HOPPING"), is(WindowType.HOPPING));
  }

  @Test
  public void shouldParseTumbling() {
    assertThat(WindowType.of("TUMBLING"), is(WindowType.TUMBLING));
  }

  @Test
  public void shouldParseAnyCase() {
    assertThat(WindowType.of("SeSsIoN"), is(WindowType.SESSION));
  }

  @Test
  public void shouldIncludeOriginalTextInExceptionMessage() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Unknown window type: 'something'");

    // When:
    WindowType.of("something");
  }

  @Test
  public void shouldIncludeValidValuesInExceptionMessage() {
    // Then:
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("Valid values are: SESSION, HOPPING, TUMBLING");

    // When:
    WindowType.of("meh");
  }
}