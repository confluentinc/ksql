/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.server;

import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;

public class StandaloneExecutorTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final KsqlEngine engine = EasyMock.niceMock(KsqlEngine.class);
  private final UdfLoader udfLoader = EasyMock.niceMock(UdfLoader.class);
  private final String query = "select * from bar;";
  private StandaloneExecutor executor;

  @Before
  public void before() throws IOException {
    final String queriesFile = TestUtils.tempFile().getPath();
    executor = new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);
    try(final FileOutputStream out = new FileOutputStream(queriesFile)) {
      out.write(query.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  public void shouldCreateQueries() {
    EasyMock.expect(engine.createQueries(query + "\n", ksqlConfig))
        .andReturn(Collections.emptyList());
    EasyMock.replay(engine);

    executor.start();

    EasyMock.verify(engine);
  }

  @Test
  public void shouldExecutePersistentQueries() {
    final PersistentQueryMetadata query = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(engine.createQueries(anyString(), eq(ksqlConfig)))
        .andReturn(Collections.singletonList(query));
    query.start();
    EasyMock.expectLastCall();
    EasyMock.replay(query, engine);

    executor.start();

    EasyMock.verify(query);
  }

  @Test
  public void shouldNotExecuteNonPersistentQueries() {
    final QueryMetadata query = EasyMock.createMock(QueryMetadata.class);
    EasyMock.expect(engine.createQueries(anyString(), eq(ksqlConfig)))
        .andReturn(Collections.singletonList(query));
    EasyMock.expect(query.getStatementString()).andReturn("");
    EasyMock.replay(query, engine);

    executor.start();

    EasyMock.verify(query);
  }

}