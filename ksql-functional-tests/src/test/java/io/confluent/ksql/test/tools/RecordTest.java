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

import io.confluent.ksql.test.model.WindowData;
import java.util.Optional;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.SessionWindowedDeserializer;
import org.apache.kafka.streams.kstream.SessionWindowedSerializer;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindowedSerializer;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("rawtypes")
@RunWith(MockitoJUnitRunner.class)
public class RecordTest {

  @Mock
  private Topic topic;

  @Test
  public void shouldGetCorrectStringKeySerializer() {
    // Given:
    final Record record = new Record(
        topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        null
    );

    // When:
    final Serializer<?> serializer = record.keySerializer();

    // Then:
    assertThat(serializer, instanceOf(Serdes.String().serializer().getClass()));
  }

  @Test
  public void shouldGetCorrectTimeWondowedKeySerializer() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "TIME"));

    // When:
    final Serializer<?> serializer = record.keySerializer();

    // Then:
    assertThat(serializer, instanceOf(TimeWindowedSerializer.class));
  }

  @Test
  public void shouldGetCorrectSessionWindowedKeySerializer() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "SESSION"));

    // When:
    final Serializer<?> serializer = record.keySerializer();

    // Then:
    assertThat(serializer, instanceOf(SessionWindowedSerializer.class));
  }

  @Test
  public void shouldGetCorrectStringKeyDeserializer() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        null);

    // When:
    final Deserializer deserializer = record.keyDeserializer();

    // Then:
    assertThat(deserializer, instanceOf(Serdes.String().deserializer().getClass()));

  }

  @Test
  public void shouldGetCorrectTimedWindowKeyDeserializer() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "TIME"));

    // When:
    final Deserializer deserializer = record.keyDeserializer();

    // Then:
    assertThat(deserializer, instanceOf(TimeWindowedDeserializer.class));

  }

  @Test
  public void shouldGetCorrectSessionedWindowKeyDeserializer() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "SESSION"));

    // When:
    final Deserializer deserializer = record.keyDeserializer();

    // Then:
    assertThat(deserializer, instanceOf(SessionWindowedDeserializer.class));

  }

  @Test
  public void shouldGetStringKey() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        null);

    // When:
    final String key = record.key();

    // Then:
    assertThat(key, equalTo("foo"));
  }


  @Test
  public void shouldGetTimeWindowKey() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "TIME"));

    // When:
    final Object key = record.key();

    // Then:
    assertThat(key, instanceOf(Windowed.class));
    final Windowed windowed = (Windowed) key;
    assertThat(windowed.window(), instanceOf(TimeWindow.class));
    assertThat(windowed.window().start(), equalTo(100L));
    assertThat(windowed.window().end(), equalTo(1000L));
  }

  @Test
  public void shouldGetSessionWindowKey() {
    // Given:
    final Record record = new Record(topic,
        "foo",
        "bar",
        null,
        Optional.of(1000L),
        new WindowData(100L, 1000L, "SESSION"));

    // When:
    final Object key = record.key();

    // Then:
    assertThat(key, instanceOf(Windowed.class));
    final Windowed windowed = (Windowed) key;
    assertThat(windowed.window(), instanceOf(SessionWindow.class));
    assertThat(windowed.window().start(), equalTo(100L));
    assertThat(windowed.window().end(), equalTo(1000L));
  }

}