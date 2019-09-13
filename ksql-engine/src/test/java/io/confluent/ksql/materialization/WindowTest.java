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

package io.confluent.ksql.materialization;

import com.google.common.testing.EqualsTester;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import java.time.Instant;
import java.util.Optional;
import org.junit.Test;

public class WindowTest {

  private static final Instant INSTANT = Instant.now();
  private static final Instant LATER_INSTANCE = INSTANT.plusMillis(1);

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .testStaticMethods(Window.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldImplementEquals() {
    new EqualsTester()
        .addEqualityGroup(
            Window.of(INSTANT, Optional.of(LATER_INSTANCE)),
            Window.of(INSTANT, Optional.of(LATER_INSTANCE))
        )
        .addEqualityGroup(
            Window.of(INSTANT.minusMillis(1), Optional.of(LATER_INSTANCE))
        )
        .addEqualityGroup(
            Window.of(INSTANT, Optional.empty())
        )
        .testEquals();
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowIfEndBeforeStart() {
    Window.of(LATER_INSTANCE, Optional.of(INSTANT));
  }
}
