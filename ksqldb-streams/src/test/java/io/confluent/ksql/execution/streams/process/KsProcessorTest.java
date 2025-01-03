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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsProcessorTest {
  private static final long KEY = 10L;
  private static final GenericRow VALUE = GenericRow.genericRow(12);
  private static final String RESULT_KEY = "the result key";
  private static final GenericRow RESULT_VALUE = GenericRow.genericRow("the result value");
  private static final long ROWTIME = 123456L;

  @Mock
  private KsqlTransformer<Long, String> ksqlKeyTransformer;
  @Mock
  private KsqlTransformer<Long, GenericRow> ksqlValueTransformer;
  @Mock
  private ProcessorContext<String, GenericRow> ctx;
  @Captor
  private ArgumentCaptor<KsqlProcessingContext> ctxCaptor;

  private KsProcessor<Long, String> ksProcessor;

  @Before
  public void setUp() {
    ksProcessor = new KsProcessor<>(ksqlKeyTransformer, ksqlValueTransformer);
    ksProcessor.init(ctx);

    when(ksqlKeyTransformer.transform(any(), any(), any())).thenReturn(RESULT_KEY);
    when(ksqlValueTransformer.transform(any(), any(), any())).thenReturn(RESULT_VALUE);

    when(ctx.currentStreamTimeMs()).thenReturn(ROWTIME);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnProcessIfNotInitialized() {
    // Given:
    ksProcessor = new KsProcessor<>(ksqlKeyTransformer, ksqlValueTransformer);

    // When:
    ksProcessor.process(new Record<>(KEY, VALUE, ROWTIME));
  }

  @Test
  public void shouldInvokeInnerTransformers() {
    // When:
    final Record<Long, GenericRow> record = new Record<>(KEY, VALUE, ROWTIME);
    ksProcessor.process(record);

    // Then:
    verify(ksqlKeyTransformer).transform(
        eq(KEY),
        eq(VALUE),
        any()
    );
    verify(ksqlValueTransformer).transform(
        eq(KEY),
        eq(VALUE),
        any()
    );
  }

  @Test
  public void shouldReturnValueFromInnerTransformer() {
    // When:
    final Record<Long, GenericRow> record = new Record<>(KEY, VALUE, ROWTIME);
    ksProcessor.process(record);

    // Then:
    final Record<String, GenericRow> result = new Record<>(RESULT_KEY, RESULT_VALUE, ROWTIME);
    verify(ctx).forward(result);
  }

  @Test
  public void shouldExposeRowTime() {
    // Given:
    final Record<Long, GenericRow> inputRecord = new Record<>(KEY, VALUE, 123L);
    ksProcessor.process(inputRecord);

    final KsqlProcessingContext ksqlCtx = getKsqlProcessingContext();

    // When:
    final long rowTime = ksqlCtx.getRowTime();

    // Then:
    assertThat(rowTime, is(ROWTIME));
    verify(ksqlKeyTransformer).transform(
        eq(KEY),
        eq(VALUE),
        argThat(ctx -> ctx.getRowTime() == ROWTIME)
    );
    verify(ksqlValueTransformer).transform(
        eq(KEY),
        eq(VALUE),
        argThat(ctx -> ctx.getRowTime() == ROWTIME)
    );
  }

  private KsqlProcessingContext getKsqlProcessingContext() {
    verify(ksqlKeyTransformer).transform(
        any(),
        any(),
        ctxCaptor.capture()
    );

    return ctxCaptor.getValue();
  }
}
