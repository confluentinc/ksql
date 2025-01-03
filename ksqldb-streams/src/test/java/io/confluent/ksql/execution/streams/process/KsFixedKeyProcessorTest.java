/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.streams.process;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsFixedKeyProcessorTest {
  private static final long KEY = 10L;
  private static final GenericRow VALUE = GenericRow.genericRow(12);
  private static final String RESULT = "the result";
  private static final long ROWTIME = 123456L;

  @Mock
  private KsqlTransformer<Long, String> ksqlTransformer;
  @Mock
  private FixedKeyProcessorContext<Long, String> processorContext;
  @Captor
  private ArgumentCaptor<KsqlProcessingContext> ksqlProcessingContextCaptor;

  private KsFixedKeyProcessor<Long, String> ksFixedKeyProcessor;

  @Before
  public void setUp() {
    ksFixedKeyProcessor = new KsFixedKeyProcessor<>(ksqlTransformer);
    ksFixedKeyProcessor.init(processorContext);

    when(ksqlTransformer.transform(any(), any(), any())).thenReturn(RESULT);

    when(processorContext.currentStreamTimeMs()).thenReturn(ROWTIME);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnProcessIfContextNotInitialized() {
    // Given:
    ksFixedKeyProcessor = new KsFixedKeyProcessor<>(ksqlTransformer);

    // When:
    ksFixedKeyProcessor.process(getMockRecord());
  }

  @Test
  public void shouldInvokeInnerTransformer() {
    // When:
    ksFixedKeyProcessor.process(getMockRecord());

    // Then:
    verify(ksqlTransformer).transform(
        eq(KEY),
        eq(VALUE),
        any()
    );
  }

  @Test
  public void shouldReturnValueFromInnerTransformer() {
    // When:
    ksFixedKeyProcessor.process(getMockRecord());

    // Then:
    verify(processorContext).forward(
        argThat(record -> record.value().equals(RESULT))
    );
  }

  @Test
  public void shouldExposeRowTime() {
    // Given:
    ksFixedKeyProcessor.process(getMockRecord());
    final KsqlProcessingContext ksqlCtx = getKsqlProcessingContext();

    // When:
    final long rowTime = ksqlCtx.getRowTime();

    // Then:
    assertThat(rowTime, is(ROWTIME));
    verify(ksqlTransformer).transform(
        eq(KEY),
        eq(VALUE),
        argThat(ctx -> ctx.getRowTime() == ROWTIME)
    );
  }

  private KsqlProcessingContext getKsqlProcessingContext() {
    verify(ksqlTransformer).transform(
        any(),
        any(),
        ksqlProcessingContextCaptor.capture()
    );

    return ksqlProcessingContextCaptor.getValue();
  }

  @SuppressWarnings("unchecked")
  private static FixedKeyRecord<Long, GenericRow> getMockRecord() {
    final FixedKeyRecord<Long, GenericRow> mockRecord = mock(FixedKeyRecord.class);
    when(mockRecord.key()).thenReturn(KEY);
    when(mockRecord.value()).thenReturn(VALUE);
    // withValue should return new record with the same key and new value
    when(mockRecord.withValue(any())).thenAnswer(invocation -> {
      // It should match with RESULT type
      final String newValue = invocation.getArgument(0);
      final FixedKeyRecord<Long, String> newRecord = mock(FixedKeyRecord.class);
      when(newRecord.value()).thenReturn(newValue);
      return newRecord;
    });
    return mockRecord;
  }
}
