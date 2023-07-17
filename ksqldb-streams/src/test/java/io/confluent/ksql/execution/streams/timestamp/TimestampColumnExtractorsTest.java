/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams.timestamp;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.Column.Namespace;
import org.apache.kafka.streams.kstream.Windowed;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TimestampColumnExtractorsTest {

  @Mock
  private Column column;
  @Mock
  private GenericKey key;
  @Mock
  private Windowed<GenericKey> windowedKey;
  @Mock
  private GenericRow value;

  @Before
  public void setUp() {
    when(windowedKey.key()).thenReturn(key);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowOnNegativeIndex() {
    // Given:
    when(column.index()).thenReturn(-1);

    // When:
    TimestampColumnExtractors.create(column);
  }

  @Test
  public void shouldExtractKeyColumn() {
    // Given:
    when(column.namespace()).thenReturn(Namespace.KEY);
    when(column.index()).thenReturn(0);

    when(key.get(0)).thenReturn("some value");

    final ColumnExtractor extractor = TimestampColumnExtractors.create(column);

    // When:
    final Object result = extractor.extract(key, value);

    // Then:
    assertThat(result, is("some value"));
  }

  @Test
  public void shouldExtractWindowedKeyColumn() {
    // Given:
    when(column.namespace()).thenReturn(Namespace.KEY);
    when(column.index()).thenReturn(0);

    when(key.get(0)).thenReturn("some value");

    final ColumnExtractor extractor = TimestampColumnExtractors.create(column);

    // When:
    final Object result = extractor.extract(windowedKey, value);

    // Then:
    assertThat(result, is("some value"));
  }

  @Test
  public void shouldExtractValueColumn() {
    // Given:
    when(column.namespace()).thenReturn(Namespace.VALUE);
    when(column.index()).thenReturn(1);

    when(value.get(1)).thenReturn("some other value");

    final ColumnExtractor extractor = TimestampColumnExtractors.create(column);

    // When:
    final Object result = extractor.extract(key, value);

    // Then:
    assertThat(result, is("some other value"));
  }
}