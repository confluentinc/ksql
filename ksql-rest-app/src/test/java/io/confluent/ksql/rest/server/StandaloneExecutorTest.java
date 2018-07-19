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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

public class StandaloneExecutorTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final KsqlEngine engine = EasyMock.niceMock(KsqlEngine.class);
  private final UdfLoader udfLoader = EasyMock.niceMock(UdfLoader.class);
  private String queriesFile;

  @Before
  public void before() throws IOException {
    queriesFile = TestUtils.tempFile().getPath();
  }

  @Test
  public void shouldRunStatements() throws IOException {
    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);
    statementPairs.add(new Pair<>("CS", EasyMock.niceMock(CreateStream.class)));
    statementPairs.add(new Pair<>("CT", EasyMock.niceMock(CreateTable.class)));

    final CreateStreamAsSelect createStreamAsSelect1 = EasyMock.niceMock(CreateStreamAsSelect.class);
    final Query csas1Query = EasyMock.niceMock(Query.class);
    EasyMock.expect(createStreamAsSelect1.getQuery()).andReturn(csas1Query);
    statementPairs.add(new Pair<>("CSAS1", createStreamAsSelect1));

    final SetProperty setProperty = EasyMock.niceMock(SetProperty.class);
    EasyMock.expect(setProperty.getPropertyName()).andReturn("name");
    EasyMock.expect(setProperty.getPropertyValue()).andReturn("value");
    statementPairs.add(new Pair<>("SET", setProperty));

    final CreateTableAsSelect createTableAsSelect = EasyMock.niceMock(CreateTableAsSelect.class);
    final Query ctasQuery = EasyMock.niceMock(Query.class);
    EasyMock.expect(createTableAsSelect.getQuery()).andReturn(ctasQuery);
    statementPairs.add(new Pair<>("CTAS", createTableAsSelect));

    statementPairs.add(new Pair<>("UNSET", EasyMock.niceMock(UnsetProperty.class)));

    final CreateStreamAsSelect createStreamAsSelect2 = EasyMock.niceMock(CreateStreamAsSelect.class);
    final Query csas2Query = EasyMock.niceMock(Query.class);
    EasyMock.expect(createStreamAsSelect2.getQuery()).andReturn(csas2Query);
    statementPairs.add(new Pair<>("CSAS2", createStreamAsSelect2));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
    final Map<String, Object> props = new HashMap<>();
    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());
    EasyMock.expect(engine.buildMultipleQueries("CT", ksqlConfig, props))
        .andReturn(Collections.emptyList());

    final QueryMetadata csas1QueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(csas1QueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(csas1QueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(csas1Query, ksqlConfig))
        .andReturn(csas1QueryMetadata).once();

    final QueryMetadata ctasQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(ctasQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CTAS"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(ctasQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(ctasQuery, ksqlConfig))
        .andReturn(ctasQueryMetadata).once();

    final QueryMetadata csas2QueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(csas2QueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS2"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(csas2QueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(csas2Query, ksqlConfig))
        .andReturn(csas1QueryMetadata).once();

    EasyMock.replay(setProperty);
    EasyMock.replay(csas1Query);
    EasyMock.replay(createStreamAsSelect1);
    EasyMock.replay(csas1QueryMetadata);
    EasyMock.replay(ctasQuery);
    EasyMock.replay(createTableAsSelect);
    EasyMock.replay(ctasQueryMetadata);
    EasyMock.replay(csas2Query);
    EasyMock.replay(createStreamAsSelect2);
    EasyMock.replay(csas2QueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test(expected = KsqlException.class)
  public void shouldFailIfCsasIsInvalid() {
    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);
    statementPairs.add(new Pair<>("CS", EasyMock.niceMock(CreateStream.class)));
    statementPairs.add(new Pair<>("CT", EasyMock.niceMock(CreateTable.class)));

    final CreateStreamAsSelect createStreamAsSelect1 = EasyMock.niceMock(CreateStreamAsSelect.class);
    final Query csas1Query = EasyMock.niceMock(Query.class);
    EasyMock.expect(createStreamAsSelect1.getQuery()).andReturn(csas1Query);
    statementPairs.add(new Pair<>("CSAS1", createStreamAsSelect1));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
    final Map<String, Object> props = new HashMap<>();
    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());
    EasyMock.expect(engine.buildMultipleQueries("CT", ksqlConfig, props))
        .andReturn(Collections.emptyList());

    final QueryMetadata csas1QueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(csas1QueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(csas1QueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(csas1Query, ksqlConfig))
        .andReturn(csas1QueryMetadata).once();

    EasyMock.replay(csas1Query);
    EasyMock.replay(createStreamAsSelect1);
    EasyMock.replay(csas1QueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();

  }

}