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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
  private StandaloneExecutor standaloneExecutor;
  private MetaStore metaStore;
  private final Map<String, Object> props = ImmutableMap.of();
  private final QualifiedName qualifiedName = QualifiedName.of("Test");

  @Before
  public void before() throws IOException {
    queriesFile = TestUtils.tempFile().getPath();
    standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);
    metaStore = EasyMock.niceMock(MetaStore.class);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
  }


  @Test
  public void shouldRunCsStatement() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject()))
        .andReturn(ImmutableList.of(new Pair<>("CS",
            new CreateStream(qualifiedName, Collections.emptyList(), false, Collections.emptyMap()))));

    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());


    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunCtStatement() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject()))
        .andReturn(ImmutableList.of(new Pair<>("CT",
            new CreateTable(qualifiedName, Collections.emptyList(), false, Collections.emptyMap()))));

    EasyMock.expect(engine.buildMultipleQueries("CT", ksqlConfig, props))
        .andReturn(Collections.emptyList());

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunSetStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("SET", new SetProperty(Optional.empty(), "name", "value"))
    ));

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertThat(standaloneExecutor.getConfigProperties().get("name"), equalTo("value"));
  }


  @Test
  public void shouldRunUnSetStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("SET", new SetProperty(Optional.empty(), "name", "value")),
        new Pair<>("UNSET", new UnsetProperty(Optional.empty(), "name"))
    ));


    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertTrue(standaloneExecutor.getConfigProperties().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCsasStatements() throws IOException {

    final Query csas1Query = EasyMock.niceMock(Query.class);
    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("CSAS1", new CreateStreamAsSelect(qualifiedName, csas1Query, false, Collections.emptyMap(), Optional.empty()))
    ));

    final QueryMetadata csas1QueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(csas1QueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(csas1QueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(csas1Query, ksqlConfig))
        .andReturn(csas1QueryMetadata).once();

    EasyMock.replay(csas1Query, csas1QueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(csas1Query, csas1QueryMetadata, engine);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunInsertIntoStatements() throws IOException {

    final Query insertIntoQuery = EasyMock.niceMock(Query.class);
    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("InsertInto", new InsertInto(qualifiedName, insertIntoQuery, Optional.empty()))
    ));

    final QueryMetadata insertIntoQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(engine.buildMultipleQueries(eq("InsertInto"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(insertIntoQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(insertIntoQuery, ksqlConfig))
        .andReturn(insertIntoQueryMetadata).once();

    EasyMock.replay(insertIntoQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(insertIntoQueryMetadata, engine);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCtasStatements() throws IOException {

    final Query ctasQuery = EasyMock.niceMock(Query.class);
    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("CTAS", new CreateTableAsSelect(qualifiedName, ctasQuery, false, Collections.emptyMap()))
    ));

    final QueryMetadata ctasQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    EasyMock.expect(ctasQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CTAS"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(ctasQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(ctasQuery, ksqlConfig))
        .andReturn(ctasQueryMetadata).once();

    EasyMock.replay(ctasQuery, ctasQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(ctasQuery, ctasQueryMetadata, engine);
  }

  @Test(expected = KsqlException.class)
  @SuppressWarnings("unchecked")
  public void shouldFailIfCsasIsInvalid() {

    final Query csas1Query = EasyMock.niceMock(Query.class);

    EasyMock.expect(engine.parseStatements(anyString(), anyObject())).andReturn(ImmutableList.of(
        new Pair<>("CSAS1", new CreateStreamAsSelect(qualifiedName, csas1Query, false, Collections.emptyMap(), Optional.empty()))
    ));
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

    EasyMock.replay(csas1Query, csas1QueryMetadata, engine);
    standaloneExecutor.start();

  }

}