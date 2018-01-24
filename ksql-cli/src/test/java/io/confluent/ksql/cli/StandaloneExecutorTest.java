/**
 * Copyright 2017 Confluent Inc.
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

package io.confluent.ksql.cli;

import org.easymock.EasyMock;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;

public class StandaloneExecutorTest {

  private final KsqlEngine engine = EasyMock.niceMock(KsqlEngine.class);
  private final StandaloneExecutor executor = new StandaloneExecutor(engine);
  private final String queries = "select something from somewhere";

  @Test
  public void shouldCreateQueries() throws Exception {
    EasyMock.expect(engine.createQueries(queries)).andReturn(Collections.emptyList());
    EasyMock.replay(engine);

    executor.executeStatements(queries);

    EasyMock.verify(engine);
  }

  @Test
  public void shouldExecutePersistentQueries() throws Exception {
    final PersistentQueryMetadata query = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(engine.createQueries(queries)).andReturn(Collections.singletonList(query));
    query.start();
    EasyMock.expectLastCall();
    EasyMock.replay(query, engine);

    executor.executeStatements(queries);

    EasyMock.verify(query);
  }

  @Test
  public void shouldNotExecuteNonPersistentQueries() throws Exception {
    final QueryMetadata query = EasyMock.createMock(QueryMetadata.class);
    EasyMock.expect(engine.createQueries(queries)).andReturn(Collections.singletonList(query));
    EasyMock.expect(query.getStatementString()).andReturn(queries);
    EasyMock.replay(query, engine);

    executor.executeStatements(queries);

    EasyMock.verify(query);

  }

}