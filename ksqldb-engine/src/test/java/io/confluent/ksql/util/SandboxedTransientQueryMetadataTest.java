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

package io.confluent.ksql.util;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.PushQueryMetadata.ResultType;
import io.confluent.ksql.util.QueryMetadata.Listener;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SandboxedTransientQueryMetadataTest {
  private static final String STATEMENT = "stmt";
  private static final ImmutableMap<String, Object> PROPERTIES = ImmutableMap.of("foo", "bar");
  private static final ImmutableMap<String, Object> OVERRIDES = ImmutableMap.of("biz", "baz");
  private static final ImmutableSet<SourceName> SOURCE_NAMES = ImmutableSet.of(SourceName.of("one"));
  private static final String PLAN = "plan";
  private static final String APP_ID = "appid";
  private static final ResultType RESULT_TYPE = ResultType.TABLE;

  @Mock
  private TransientQueryMetadata original;
  @Mock
  private LogicalSchema schema;
  @Mock
  private Topology topology;
  @Mock
  private Listener listener;

  SandboxedTransientQueryMetadata sandboxed;

  @Before
  public void setup() {
    when(original.getStatementString()).thenReturn(STATEMENT);
    when(original.getStreamsProperties()).thenReturn(PROPERTIES);
    when(original.getOverriddenProperties()).thenReturn(OVERRIDES);
    when(original.getSourceNames()).thenReturn(SOURCE_NAMES);
    when(original.getExecutionPlan()).thenReturn(PLAN);
    when(original.getQueryApplicationId()).thenReturn(APP_ID);
    when(original.getLogicalSchema()).thenReturn(schema);
    when(original.getTopology()).thenReturn(topology);
    sandboxed = SandboxedTransientQueryMetadata.of(original, listener);
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfRowQueueUsed() {
    sandboxed.getRowQueue().poll();
  }

  @Test(expected = IllegalStateException.class)
  public void shouldThrowIfStarted() {
    sandboxed.start();
  }

  @Test
  public void shouldCallbackOnClose() {
    // when:
    sandboxed.close();

    // then:
    verify(listener).onClose(sandboxed);
  }
}