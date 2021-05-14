/*
 * Copyright 2021 Confluent Inc.
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

import static io.confluent.ksql.model.WindowType.of;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class WindowTypeTest {

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
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> of("something")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Unknown window type: 'something'"));
  }

  @Test
  public void shouldIncludeValidValuesInExceptionMessage() {
    // Then:
    // When:
    final Exception e = assertThrows(
        IllegalArgumentException.class,
        () -> of("meh")
    );

    // Then:
    assertThat(e.getMessage(), containsString("Valid values are: SESSION, HOPPING, TUMBLING"));
  }
}