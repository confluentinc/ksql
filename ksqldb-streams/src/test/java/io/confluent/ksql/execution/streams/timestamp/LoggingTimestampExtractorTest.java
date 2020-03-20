/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingTimestampExtractorTest {

  private static final long PREVIOUS_TS = 1001L;

  private static final GenericRow ROW = GenericRow.fromList(ImmutableList.of(1, 2, 3));

  @Mock
  private ProcessingLogger logger;

  @Mock
  private ConsumerRecord<Object, Object> record;

  @Before
  public void setUp() {
    when(record.value()).thenReturn(ROW);
  }

  @Test
  public void shouldLogExceptionsAndNotFailOnExtractFromRow() {
    // Given:
    final KsqlException e = new KsqlException("foo");
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw e;
        },
        logger,
        false
    );

    // When:
    final long result = extractor.extract(ROW);

    // Then (did not throw):
    verify(logger).error(RecordProcessingError
        .recordProcessingError("Failed to extract timestamp from row", e, ROW::toString));

    assertThat(result, is(-1L));
  }

  @Test
  public void shouldLogExceptionsAndFailOnExtractFromRow() {
    // Given:
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw new KsqlException("foo");
        },
        logger,
        true
    );

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> extractor.extract(ROW)
    );

    verify(logger).error(any());
  }

  @Test
  public void shouldLogExceptionsAndNotFailOnExtractFromRecord() {
    // Given:
    final KsqlException e = new KsqlException("foo");
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw e;
        },
        logger,
        false
    );

    // When:
    final long result = extractor.extract(record, PREVIOUS_TS);

    // Then (did not throw):
    verify(logger).error(RecordProcessingError
        .recordProcessingError("Failed to extract timestamp from row", e, ROW::toString));

    assertThat(result, is(-1L));
  }

  @Test
  public void shouldLogExceptionsAndFailOnExtractFromRecord() {
    // Given:
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw new KsqlException("foo");
        },
        logger,
        true
    );

    // When/Then:
    assertThrows(
        KsqlException.class,
        () -> extractor.extract(record, PREVIOUS_TS)
    );

    verify(logger).error(any());
  }

  @Test
  public void shouldLogExceptionsAndNotFailOnExtractFromRecordWithNullValue() {
    // Given:
    when(record.value()).thenReturn(null);

    final KsqlException e = new KsqlException("foo");
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw e;
        },
        logger,
        false
    );

    // When:
    final long result = extractor.extract(record, PREVIOUS_TS);

    // Then (did not throw):
    verify(logger).error(RecordProcessingError
        .recordProcessingError("Failed to extract timestamp from row", e, () -> "null"));

    assertThat(result, is(-1L));
  }
}