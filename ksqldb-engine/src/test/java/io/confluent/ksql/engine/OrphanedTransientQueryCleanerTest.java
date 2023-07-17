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

package io.confluent.ksql.engine;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.QueryCleanupService.QueryCleanupTask;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConfig;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class OrphanedTransientQueryCleanerTest {
  private static final String TOPIC1
      = "_confluent-ksql-default_transient_932097300573686369_1606940079718"
      + "-Aggregate-GroupBy-repartition";
  private static final String TOPIC2
      = "_confluent-ksql-default_transient_932097300573686369_1606940079718"
      + "-Aggregate-Aggregate-Materialize-changelog";
  private static final String TOPIC3
      = "_confluent-ksql-default_transient_123497300573686369_1606940012345"
      + "-Aggregate-Aggregate-Materialize-changelog";

  private static final String BAD_TOPIC_NAME
      = "_confluent-ksql-default_node0_transient_bad";

  private static final String APP_ID_1
      = "_confluent-ksql-default_transient_932097300573686369_1606940079718";
  private static final String APP_ID_2
      = "_confluent-ksql-default_transient_123497300573686369_1606940012345";

  @Mock
  private QueryCleanupService queryCleanupService;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient topicClient;
  @Mock
  private KsqlConfig ksqlConfig;
  @Captor
  private ArgumentCaptor<QueryCleanupTask> taskCaptor;

  private OrphanedTransientQueryCleaner cleaner;

  @Before
  public void setUp() {
    when(serviceContext.getTopicClient()).thenReturn(topicClient);
    when(ksqlConfig.getKsqlStreamConfigProps()).thenReturn(ImmutableMap.of("state.dir", "tmp/cat/"));
    cleaner = new OrphanedTransientQueryCleaner(queryCleanupService, ksqlConfig);
  }

  @Test
  public void shouldCleanup_allApplicationIds() {
    // Given
    when(topicClient.listTopicNames()).thenReturn(ImmutableSet.of(TOPIC1, TOPIC2, TOPIC3));

    // When
    cleaner.cleanupOrphanedInternalTopics(serviceContext, ImmutableSet.of(APP_ID_1, APP_ID_2));

    // Then
    verify(queryCleanupService, times(2)).addCleanupTask(taskCaptor.capture());
    assertThat(taskCaptor.getAllValues().get(0).getAppId(), is(APP_ID_1));
    assertThat(taskCaptor.getAllValues().get(1).getAppId(), is(APP_ID_2));
  }

  @Test
  public void shouldCleanup_someApplicationIds() {
    // Given
    when(topicClient.listTopicNames()).thenReturn(ImmutableSet.of(TOPIC1, TOPIC2));

    // When
    cleaner.cleanupOrphanedInternalTopics(serviceContext, ImmutableSet.of(APP_ID_1, APP_ID_2));

    // Then
    verify(queryCleanupService, times(1)).addCleanupTask(taskCaptor.capture());
    assertThat(taskCaptor.getAllValues().get(0).getAppId(), is(APP_ID_1));
  }

  @Test
  public void skipNonMatchingTopics() {
    // Given
    when(topicClient.listTopicNames()).thenReturn(ImmutableSet.of(TOPIC1, TOPIC2, TOPIC3));

    // When
    cleaner.cleanupOrphanedInternalTopics(serviceContext, ImmutableSet.of(APP_ID_2));

    // Then
    verify(queryCleanupService, times(1)).addCleanupTask(taskCaptor.capture());
    assertThat(taskCaptor.getAllValues().get(0).getAppId(), is(APP_ID_2));
  }

  @Test
  public void shouldSkip_exception() {
    // Given
    when(topicClient.listTopicNames())
        .thenThrow(new KafkaResponseGetFailedException("error!", new Exception()));

    // When
    cleaner.cleanupOrphanedInternalTopics(serviceContext, ImmutableSet.of());

    // Then
    verify(queryCleanupService, never()).addCleanupTask(any());
  }
}
