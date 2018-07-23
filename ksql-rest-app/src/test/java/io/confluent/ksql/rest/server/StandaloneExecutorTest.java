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
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QualifiedName;
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
import java.util.Optional;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

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
  public void shouldRunCsStatement() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final CreateStream cs = new CreateStream(EasyMock.niceMock(QualifiedName.class), Collections.emptyList(), false, Collections.emptyMap());
    statementPairs.add(new Pair<>("CS", cs));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
    final Map<String, Object> props = new HashMap<>();
    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());


    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunCtStatement() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final CreateTable ct = new CreateTable(EasyMock.niceMock(QualifiedName.class), Collections.emptyList(), false, Collections.emptyMap());
    statementPairs.add(new Pair<>("CT", ct));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
    final Map<String, Object> props = new HashMap<>();
    EasyMock.expect(engine.buildMultipleQueries("CT", ksqlConfig, props))
        .andReturn(Collections.emptyList());

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunSetStatements() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);

    final SetProperty setProperty = new SetProperty(Optional.empty(), "name", "value");
    statementPairs.add(new Pair<>("SET", setProperty));

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertThat(standaloneExecutor.getConfigProperties().get("name"), equalTo("value"));

  }


  @Test
  public void shouldRunUnSetStatements() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);

    final SetProperty setProperty = new SetProperty(Optional.empty(), "name", "value");
    statementPairs.add(new Pair<>("SET", setProperty));

    final UnsetProperty unsetProperty = new UnsetProperty(Optional.empty(), "name");
    statementPairs.add(new Pair<>("UNSET", unsetProperty));

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertTrue(standaloneExecutor.getConfigProperties().isEmpty());

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCsasStatements() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final Query csas1Query = EasyMock.niceMock(Query.class);
    final CreateStreamAsSelect createStreamAsSelect1 = new CreateStreamAsSelect(EasyMock.niceMock(QualifiedName.class), csas1Query, false, Collections.emptyMap(), Optional.empty());
    statementPairs.add(new Pair<>("CSAS1", createStreamAsSelect1));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);

    final QueryMetadata csas1QueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(csas1QueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(csas1QueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(csas1Query, ksqlConfig))
        .andReturn(csas1QueryMetadata).once();

    EasyMock.replay(csas1Query);
    EasyMock.replay(csas1QueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(csas1Query);
    EasyMock.verify(csas1QueryMetadata);
    EasyMock.verify(engine);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunInsertIntoStatements() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final Query insertIntoQuery = EasyMock.niceMock(Query.class);
    final InsertInto insertInto = new InsertInto(EasyMock.niceMock(QualifiedName.class), insertIntoQuery, Optional.empty());
    statementPairs.add(new Pair<>("InsertInto", insertInto));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);

    final QueryMetadata insertIntoQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(engine.buildMultipleQueries(eq("InsertInto"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(insertIntoQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(insertIntoQuery, ksqlConfig))
        .andReturn(insertIntoQueryMetadata).once();

    EasyMock.replay(insertIntoQueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(insertIntoQueryMetadata);
    EasyMock.verify(engine);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCtasStatements() throws IOException {

    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final Query ctasQuery = EasyMock.niceMock(Query.class);
    final CreateTableAsSelect createTableAsSelect = new CreateTableAsSelect(EasyMock.niceMock(QualifiedName.class), ctasQuery, false, Collections.emptyMap());
    statementPairs.add(new Pair<>("CTAS", createTableAsSelect));

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(statementPairs);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);

    final QueryMetadata ctasQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(ctasQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CTAS"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(ctasQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(ctasQuery, ksqlConfig))
        .andReturn(ctasQueryMetadata).once();

    EasyMock.replay(ctasQuery);
    EasyMock.replay(ctasQueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(ctasQuery);
    EasyMock.verify(ctasQueryMetadata);
    EasyMock.verify(engine);
  }

  @Test(expected = KsqlException.class)
  @SuppressWarnings("unchecked")
  public void shouldFailIfCsasIsInvalid() {
    final StandaloneExecutor standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);

    final List<Pair<String, Statement>> statementPairs = new ArrayList<>();
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);

    final Query csas1Query = EasyMock.niceMock(Query.class);
    final CreateStreamAsSelect createStreamAsSelect1 = new CreateStreamAsSelect(EasyMock.niceMock(QualifiedName.class), csas1Query, false, Collections.emptyMap(), Optional.empty());
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
    EasyMock.replay(csas1QueryMetadata);
    EasyMock.replay(engine);
    standaloneExecutor.start();

  }

}