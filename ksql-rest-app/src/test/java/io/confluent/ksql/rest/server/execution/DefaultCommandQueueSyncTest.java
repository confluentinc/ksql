/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlEntityList;
import io.confluent.ksql.rest.server.computation.CommandQueue;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DefaultCommandQueueSyncTest {

  @Mock KsqlEntity entity1;
  @Mock KsqlEntity entity2;

  @Mock CommandStatusEntity commandStatusEntity1;
  @Mock CommandStatusEntity commandStatusEntity2;

  @Mock CommandQueue commandQueue;

  private DefaultCommandQueueSync commandQueueSync;
  private KsqlEntityList entities;

  @Before
  public void setUp() throws TimeoutException, InterruptedException {
    when(commandStatusEntity1.getCommandSequenceNumber()).thenReturn(1L);
    when(commandStatusEntity2.getCommandSequenceNumber()).thenReturn(2L);

    doNothing().when(commandQueue).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldWaitForDistributedStatements() throws Exception {
    // Given:
    givenSyncWithPredicate(clazz -> true);
    givenEntities(commandStatusEntity1);

    // When:
    commandQueueSync.waitFor(entities, CreateStreamAsSelect.class);

    // Then:
    verify(commandQueue, times(1)).ensureConsumedPast(1L, Duration.ZERO);
  }

  @Test
  public void shouldOnlyWaitForMostRecentDistributedStatements() throws Exception {
    // Given:
    givenSyncWithPredicate(clazz -> true);
    givenEntities(entity1, commandStatusEntity1, entity2, commandStatusEntity2);

    // When:
    commandQueueSync.waitFor(entities, CreateStreamAsSelect.class);

    // Then:
    verify(commandQueue, times(1)).ensureConsumedPast(2L, Duration.ZERO);
    verify(commandQueue, never()).ensureConsumedPast(1L, Duration.ZERO);
  }

  @Test
  public void shouldNotWaitForNonCommandStatusEntity() throws Exception {
    // Given:
    givenSyncWithPredicate(clazz -> true);
    givenEntities(entity1);

    // When:
    commandQueueSync.waitFor(entities, CreateStreamAsSelect.class);

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  @Test
  public void shouldNotWaitIfNotMustSync() throws Exception {
    // Given:
    givenSyncWithPredicate(clazz -> false);
    givenEntities(commandStatusEntity1);

    // When:
    commandQueueSync.waitFor(entities, CreateStreamAsSelect.class);

    // Then:
    verify(commandQueue, never()).ensureConsumedPast(anyLong(), any());
  }

  private void givenSyncWithPredicate(final Predicate<Class<? extends Statement>> mustSync) {
    commandQueueSync = new DefaultCommandQueueSync(commandQueue, mustSync, Duration.ZERO);
  }

  private void givenEntities(final KsqlEntity... entities) {
    this.entities = new KsqlEntityList(ImmutableList.copyOf(entities));
  }

}
