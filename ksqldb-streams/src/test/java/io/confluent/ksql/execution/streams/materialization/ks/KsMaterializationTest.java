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

package io.confluent.ksql.execution.streams.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.execution.streams.materialization.Locator;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedTable;
import io.confluent.ksql.execution.streams.materialization.StreamsMaterializedWindowedTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.serde.WindowInfo;
import java.time.Duration;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializationTest {

  @Mock
  private Locator locator;
  @Mock
  private KsStateStore stateStore;
  private KsMaterialization materialization;

  @Before
  public void setUp() {
    givenWindowType(Optional.empty());
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterialization.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldReturnLocator() {
    assertThat(materialization.locator(), is(sameInstance(locator)));
  }

  @Test
  public void shouldReturnWindowType() {
    // Given:
    givenWindowType(Optional.of(WindowType.TUMBLING));

    // Then:
    assertThat(materialization.windowType(), is(Optional.of(WindowType.TUMBLING)));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnWindowedIfNotWindowed() {
    // Given:
    givenWindowType(Optional.empty());

    // When:
    materialization.windowed();
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowOnNonWindowedIfWindowed() {
    // Given:
    givenWindowType(Optional.of(WindowType.SESSION));

    // When:
    materialization.nonWindowed();
  }

  @Test
  public void shouldReturnNonWindowed() {
    // Given:
    givenWindowType(Optional.empty());

    // When:
    final StreamsMaterializedTable table = materialization.nonWindowed();

    // Then:
    assertThat(table, is(instanceOf(KsMaterializedTable.class)));
  }

  @Test
  public void shouldReturnWindowedForSession() {
    // Given:
    givenWindowType(Optional.of(WindowType.SESSION));

    // When:
    final StreamsMaterializedWindowedTable table = materialization.windowed();

    // Then:
    assertThat(table, is(instanceOf(KsMaterializedSessionTable.class)));
  }

  @Test
  public void shouldReturnWindowedForTumbling() {
    // Given:
    givenWindowType(Optional.of(WindowType.TUMBLING));

    // When:
    final StreamsMaterializedWindowedTable table = materialization.windowed();

    // Then:
    assertThat(table, is(instanceOf(KsMaterializedWindowTable.class)));
  }

  @Test
  public void shouldReturnWindowedForHopping() {
    // Given:
    givenWindowType(Optional.of(WindowType.HOPPING));

    // When:
    final StreamsMaterializedWindowedTable table = materialization.windowed();

    // Then:
    assertThat(table, is(instanceOf(KsMaterializedWindowTable.class)));
  }

  private void givenWindowType(final Optional<WindowType> windowType) {
    final Optional<WindowInfo> windowInfo = windowType
        .map(wt -> WindowInfo.of(wt, wt.requiresWindowSize()
            ? Optional.of(Duration.ofSeconds(1))
            : Optional.empty(),
            Optional.empty())
        );

    materialization = new KsMaterialization(windowInfo, locator, stateStore);
  }
}