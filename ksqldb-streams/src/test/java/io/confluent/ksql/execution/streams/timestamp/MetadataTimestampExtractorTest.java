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

package io.confluent.ksql.execution.streams.timestamp;

import static org.mockito.Mockito.mock;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetadataTimestampExtractorTest {

  @Mock
  private TimestampExtractor timestampExtractor;
  @Mock
  private ConsumerRecord<Object, Object> record;

  @Test
  public void shouldCallInternalTimestampExtractorOnExtract() {
    // Given
    final MetadataTimestampExtractor metadataTimestampExtractor =
        new MetadataTimestampExtractor(timestampExtractor);

    // When
    metadataTimestampExtractor.extract(record, 1);

    // Then
    Mockito.verify(timestampExtractor, Mockito.times(1))
        .extract(record, 1);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowOnNullTimestampExtractor() {
    // When/Then
    new MetadataTimestampExtractor(null);
  }

  @Test(expected = UnsupportedOperationException.class)
  public void shouldThrowUnsupportedExceptionOnExtractGenericRow() {
    // when/Then
    new MetadataTimestampExtractor(timestampExtractor)
        .extract(mock(Struct.class), mock(GenericRow.class));
  }
}
