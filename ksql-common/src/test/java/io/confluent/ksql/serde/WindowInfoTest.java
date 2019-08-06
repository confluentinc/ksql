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

package io.confluent.ksql.serde;

import static io.confluent.ksql.model.WindowType.HOPPING;
import static io.confluent.ksql.model.WindowType.SESSION;
import static io.confluent.ksql.model.WindowType.TUMBLING;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import io.confluent.ksql.model.WindowType;
import java.time.Duration;
import java.util.Optional;
import org.junit.Test;

public class WindowInfoTest {
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testAllPublicStaticMethods(WindowInfo.class);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            WindowInfo.of(SESSION, Optional.empty()),
            WindowInfo.of(SESSION, Optional.empty())
        )
        .addEqualityGroup(
            WindowInfo.of(TUMBLING, Optional.of(Duration.ofMillis(19))),
            WindowInfo.of(TUMBLING, Optional.of(Duration.ofMillis(19)))
        )
        .addEqualityGroup(
            WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(19))),
            WindowInfo.of(HOPPING, Optional.of(Duration.ofMillis(19)))
        )
        .addEqualityGroup(
            WindowInfo.of(TUMBLING, Optional.of(Duration.ofMillis(1010)))
        )
        .testEquals();
  }

  @Test
  public void shouldImplementToString() {
    // Given:
    final WindowInfo windowInfo = WindowInfo.of(TUMBLING, Optional.of(Duration.ofMillis(19)));

    // When:
    final String result = windowInfo.toString();

    // Then:
    assertThat(result, containsString("TUMBLING"));
    assertThat(result, containsString("19"));
  }

  @Test
  public void shouldGetType() {
    // Given:
    final WindowInfo windowInfo = WindowInfo.of(SESSION, Optional.empty());

    // When:
    final WindowType result = windowInfo.getType();

    // Then:
    assertThat(result, is(SESSION));
  }

  @Test
  public void shouldGetFormatInfo() {
    // Given:
    final WindowInfo windowInfo = WindowInfo.of(HOPPING, Optional.of(Duration.ofSeconds(10)));

    // When:
    final Optional<Duration> result = windowInfo.getSize();

    // Then:
    assertThat(result, is(Optional.of(Duration.ofSeconds(10))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSizeProvidedButNotRequired() {
    WindowInfo.of(SESSION, Optional.of(Duration.ofSeconds(10)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSizeRequiredButNotProvided() {
    WindowInfo.of(TUMBLING, Optional.empty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSizeZero() {
    WindowInfo.of(TUMBLING, Optional.of(Duration.ZERO));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfSizeNegative() {
    WindowInfo.of(TUMBLING, Optional.of(Duration.ofSeconds(-1)));
  }
}