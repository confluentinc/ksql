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

package io.confluent.ksql.rest.util;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.engine.QueryCleanupService;
import io.confluent.ksql.logging.query.TestAppender;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.BinPackedPersistentQueryMetadataImpl;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PersistentQueryMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PersistentQueryCleanupImplTest {
  File tempFile;
  PersistentQueryCleanupImpl cleanup;

  @Mock
  KsqlConfig ksqlConfig;
  @Mock
  ServiceContext context;
  @Mock
  PersistentQueryMetadata runningQuery;
  @Mock
  BinPackedPersistentQueryMetadataImpl binPackedPersistentQueryMetadata;
  @Before
  public void setUp() {
    tempFile = new File("/tmp/cat/");
    if (!tempFile.exists()){
      if (!tempFile.mkdirs()) {
        throw new IllegalStateException(String.format(
            "Could not create temp directory: %s",
            tempFile.getAbsolutePath()
        ));
      }
    }

    cleanup = new PersistentQueryCleanupImpl("/tmp/cat/", context, ksqlConfig);
    when(binPackedPersistentQueryMetadata.getQueryId()).thenReturn(new QueryId("test"));
  }

  @Test
  public void shouldDeleteExtraStateStores() {
    // Given:
    final TestAppender appender = new TestAppender();
    final Logger logger = Logger.getRootLogger();
    logger.addAppender(appender);

    File fakeStateStore = new File(tempFile.getAbsolutePath() + "/fakeStateStore");
    if (!fakeStateStore.exists()) {
      assertTrue(fakeStateStore.mkdirs());
    }

    // When:
    cleanup.cleanupLeakedQueries(Collections.emptyList());
    awaitCleanupComplete();

    // Then:
    assertFalse(fakeStateStore.exists());
    assertTrue(tempFile.exists());
    final List<LoggingEvent> log = appender.getLog();
    final LoggingEvent firstLogEntry = log.get(0);

    assertThat((String) firstLogEntry.getMessage(), is(
      "Deleted local state store for non-existing query fakeStateStore. " +
        "This is not expected and was likely due to a race condition when the query was dropped before."));
    assertThat(firstLogEntry.getLevel(), is(Level.WARN));
  }

  @Test
  public void shouldNotDeleteSharedRuntimesWhenTheyHaveAQuery() {
    // Given:
    when(binPackedPersistentQueryMetadata.getQueryApplicationId()).thenReturn("testQueryID");
    File fakeStateStore = new File(tempFile.getAbsolutePath() + "testQueryID");
    if (!fakeStateStore.exists()) {
      assertTrue(fakeStateStore.mkdirs());
    }
    // When:
    cleanup.cleanupLeakedQueries(ImmutableList.of(binPackedPersistentQueryMetadata));
    awaitCleanupComplete();

    // Then:
    assertTrue(fakeStateStore.exists());
    assertTrue(tempFile.exists());
  }

  @Test
  public void shouldKeepStateStoresBelongingToRunningQueries() {
    // Given:
    when(runningQuery.getQueryApplicationId()).thenReturn("testQueryID");

    File fakeStateStore = new File(tempFile.getAbsolutePath() + "testQueryID");
    if (!fakeStateStore.exists()) {
      assertTrue(fakeStateStore.mkdirs());
    }
    // When:
    cleanup.cleanupLeakedQueries(ImmutableList.of(runningQuery));
    awaitCleanupComplete();

    // Then:
    assertTrue(fakeStateStore.exists());
    assertTrue(tempFile.exists());
  }

  private void awaitCleanupComplete() {
    // add a task to the end of the queue to make sure that
    // we've finished processing everything up until this point
    cleanup.getQueryCleanupService().addCleanupTask(new QueryCleanupService.QueryCleanupTask(context, "", Optional.empty(), false, "", KsqlConfig.KSQL_SERVICE_ID_DEFAULT, "") {
      @Override
      public void run() {
        // do nothing
      }
    });
    while (!cleanup.getQueryCleanupService().isEmpty()) {
      Thread.yield();
    }
  }
}
