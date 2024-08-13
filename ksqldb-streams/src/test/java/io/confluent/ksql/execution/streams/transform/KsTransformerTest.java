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

package io.confluent.ksql.execution.streams.transform;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsTransformerTest {

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
  private ProcessorContext ctx;
  @Captor
  private ArgumentCaptor<KsqlProcessingContext> ctxCaptor;

  private KsTransformer<Long, String> ksTransformer;

  @Before
  public void setUp() {
    ksTransformer = new KsTransformer<>(ksqlKeyTransformer, ksqlValueTransformer);
    ksTransformer.init(ctx);

    when(ksqlKeyTransformer.transform(any(), any(), any())).thenReturn(RESULT_KEY);
    when(ksqlValueTransformer.transform(any(), any(), any())).thenReturn(RESULT_VALUE);

    when(ctx.timestamp()).thenReturn(ROWTIME);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowOnTransformIfNotInitialized() {
    // Given:
    ksTransformer = new KsTransformer<>(ksqlKeyTransformer, ksqlValueTransformer);

    // When:
    ksTransformer.transform(KEY, VALUE);
  }

  @Test
  public void shouldInvokeInnerTransformers() {
    // When:
    ksTransformer.transform(KEY, VALUE);

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
    final KeyValue<String, GenericRow> result = ksTransformer.transform(KEY, VALUE);

    // Then:
    assertThat(result, is(KeyValue.pair(RESULT_KEY, RESULT_VALUE)));
  }

  @Test
  public void shouldExposeRowTime() {
    // Given:
    ksTransformer.transform(KEY, VALUE);

    final KsqlProcessingContext ksqlCtx = getKsqlProcessingContext();

    // When:
    final long rowTime = ksqlCtx.getRowTime();

    // Then:
    assertThat(rowTime, is(ROWTIME));
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