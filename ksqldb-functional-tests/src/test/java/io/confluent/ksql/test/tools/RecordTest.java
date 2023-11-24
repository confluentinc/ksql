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

package io.confluent.ksql.test.tools;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.test.model.TestHeader;
import io.confluent.ksql.test.model.WindowData;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;

public class RecordTest {

  private static final String TOPIC_NAME = "bob";

  @Test
  public void shouldGetKey() {
    // Given:
    final Record record = new Record(
        TOPIC_NAME,
        10,
        null,
        "bar",
        null,
        Optional.of(1000L),
        null,
        Optional.empty()
    );

    // When:
    final Object key = record.key();

    // Then:
    assertThat(key, equalTo(10));
  }

  @Test
  public void shouldGetTimeWindowKey() {
    // Given:
    final Record record = new Record(
        TOPIC_NAME,
        "foo",
        null,
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "TIME"),
        Optional.empty()
    );

    // When:
    final Object key = record.key();

    // Then:
    assertThat(key, instanceOf(Windowed.class));
    final Windowed<?> windowed = (Windowed<?>) key;
    assertThat(windowed.window(), instanceOf(TimeWindow.class));
    assertThat(windowed.window().start(), equalTo(100L));
    assertThat(windowed.window().end(), equalTo(1000L));
  }

  @Test
  public void shouldGetSessionWindowKey() {
    // Given:
    final Record record = new Record(
        TOPIC_NAME,
        "foo",
        null,
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "SESSION"),
        Optional.empty()
    );

    // When:
    final Object key = record.key();

    // Then:
    assertThat(key, instanceOf(Windowed.class));
    final Windowed<?> windowed = (Windowed<?>) key;
    assertThat(windowed.window(), instanceOf(SessionWindow.class));
    assertThat(windowed.window().start(), equalTo(100L));
    assertThat(windowed.window().end(), equalTo(1000L));
  }

  @Test
  public void shouldGetHeaders() {
    // Given:
    final Record record = new Record(
        TOPIC_NAME,
        "foo",
        null,
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "SESSION"),
        Optional.of(ImmutableList.of(new TestHeader("a", new byte[] {12})))
    );

    // When:
    final List<TestHeader> headers = record.headers().get();

    // Then:
    assertThat(headers.size(), equalTo(1));
    assertThat(headers.get(0).key(), equalTo("a"));
    assertThat(headers.get(0).value(), equalTo(new byte[] {12}));
  }
}