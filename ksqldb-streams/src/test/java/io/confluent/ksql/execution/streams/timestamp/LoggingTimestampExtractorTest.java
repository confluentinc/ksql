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

import static org.mockito.ArgumentMatchers.any;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.util.KsqlException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LoggingTimestampExtractorTest {

  @Mock
  private ProcessingLogger logger;

  @Mock
  private GenericRow row;

  @Test
  public void shouldLogExceptionsAndNotFail() {
    // Given:
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw new KsqlException("foo");
        },
        logger,
        false
    );

    // When:
    extractor.extract(row);

    // Then (did not throw):
    Mockito.verify(logger).error(any());
  }

  @Test
  public void shouldLogExceptionsAndFail() {
    // Given:
    final LoggingTimestampExtractor extractor = new LoggingTimestampExtractor(
        (rec) -> {
          throw new KsqlException("foo");
        },
        logger,
        true
    );

    // When/Then:
    try {
      extractor.extract(row);
    } catch (final Exception e) {
      Mockito.verify(logger).error(any());
      return;
    }

    Assert.fail("Expected error!");
  }

}