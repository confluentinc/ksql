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

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.eq;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.function.UdfLoader;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser.PreparedStatement;
import io.confluent.ksql.parser.tree.CreateStream;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.CreateTable;
import io.confluent.ksql.parser.tree.CreateTableAsSelect;
import io.confluent.ksql.parser.tree.DropStream;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.serde.DataSource.DataSourceType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import io.confluent.ksql.util.QueryMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.test.TestUtils;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.easymock.EasyMock.expect;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class StandaloneExecutorTest {

  private final KsqlConfig ksqlConfig = new KsqlConfig(Collections.emptyMap());
  private final KsqlEngine engine = EasyMock.niceMock(KsqlEngine.class);
  private final UdfLoader udfLoader = EasyMock.niceMock(UdfLoader.class);
  private StandaloneExecutor standaloneExecutor;
  private final Map<String, Object> props = ImmutableMap.of();
  private final QualifiedName qualifiedName = QualifiedName.of("Test");

  private Query query;
  private QueryMetadata persistentQueryMetadata;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void before() throws IOException {
    query = EasyMock.niceMock(Query.class);
    persistentQueryMetadata = EasyMock.niceMock(PersistentQueryMetadata.class);
    final String queriesFile = TestUtils.tempFile().getPath();
    standaloneExecutor =
        new StandaloneExecutor(ksqlConfig, engine, queriesFile, udfLoader);
    final MetaStore metaStore = EasyMock.niceMock(MetaStore.class);
    EasyMock.expect(engine.getMetaStore()).andReturn(metaStore);
  }


  @Test
  public void shouldFailDropStatement() throws IOException {
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage("Ignoring statements: DROP\n"
        + "Only DDL (CREATE STREAM/TABLE, DROP STREAM/TABLE, SET, UNSET) and DML(CSAS, CTAS and INSERT INTO) statements can run in standalone mode.");

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean()))
        .andReturn(ImmutableList.of(new PreparedStatement("CS",
            new CreateStream(qualifiedName, Collections.emptyList(), false, Collections.emptyMap())),
            new PreparedStatement("DROP",
                new DropStream(qualifiedName, false, false))));

    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());


    EasyMock.replay(engine);
    standaloneExecutor.start();
  }

  @Test
  public void shouldRunCsStatement() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean()))
        .andReturn(ImmutableList.of(new PreparedStatement("CS",
            new CreateStream(qualifiedName, Collections.emptyList(), false, Collections.emptyMap()))));

    EasyMock.expect(engine.buildMultipleQueries("CS", ksqlConfig, props))
        .andReturn(Collections.emptyList());


    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunCtStatement() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean()))
        .andReturn(ImmutableList.of(new PreparedStatement("CT",
            new CreateTable(qualifiedName, Collections.emptyList(), false, Collections.emptyMap()))));

    EasyMock.expect(engine.buildMultipleQueries("CT", ksqlConfig, props))
        .andReturn(Collections.emptyList());

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);
  }

  @Test
  public void shouldRunSetStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("SET", new SetProperty(Optional.empty(), "name", "value"))
    ));

    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertThat(standaloneExecutor.getConfigProperties().get("name"), equalTo("value"));
  }


  @Test
  public void shouldRunUnSetStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("SET", new SetProperty(Optional.empty(), "name", "value")),
        new PreparedStatement("UNSET", new UnsetProperty(Optional.empty(), "name"))
    ));


    EasyMock.replay(engine);
    standaloneExecutor.start();
    EasyMock.verify(engine);

    assertTrue(standaloneExecutor.getConfigProperties().isEmpty());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCsasStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("CSAS1", new CreateStreamAsSelect(qualifiedName, query, false, Collections.emptyMap(), Optional.empty()))
    ));
    expect(persistentQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(persistentQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(query, ksqlConfig))
        .andReturn(persistentQueryMetadata).once();

    EasyMock.replay(query, persistentQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(query, persistentQueryMetadata, engine);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunInsertIntoStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("InsertInto", new InsertInto(qualifiedName, query, Optional.empty()))
    ));

    EasyMock.expect(engine.buildMultipleQueries(eq("InsertInto"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(persistentQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(query, ksqlConfig))
        .andReturn(persistentQueryMetadata).once();

    EasyMock.replay(persistentQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(persistentQueryMetadata, engine);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRunCtasStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("CTAS", new CreateTableAsSelect(qualifiedName, query, false, Collections.emptyMap()))
    ));
    expect(persistentQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CTAS"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(persistentQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(query, ksqlConfig))
        .andReturn(persistentQueryMetadata).once();

    EasyMock.replay(query, persistentQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(query, persistentQueryMetadata, engine);
  }

  @Test(expected = KsqlException.class)
  @SuppressWarnings("unchecked")
  public void shouldFailInvalidCsasStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("CSAS2", new CreateStreamAsSelect(qualifiedName, query, false, Collections.emptyMap(), Optional.empty()))
    ));
    expect(persistentQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KTABLE);
    EasyMock.expect(engine.buildMultipleQueries(eq("CSAS2"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(persistentQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(query, ksqlConfig))
        .andReturn(persistentQueryMetadata).once();

    EasyMock.replay(query, persistentQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(query, persistentQueryMetadata, engine);

  }

  @Test(expected = KsqlException.class)
  @SuppressWarnings("unchecked")
  public void shouldFailInvalidCtasStatements() throws IOException {

    EasyMock.expect(engine.parseStatements(anyString(), anyObject(), anyBoolean())).andReturn(ImmutableList.of(
        new PreparedStatement("CTAS1", new CreateTableAsSelect(qualifiedName, query, false, Collections.emptyMap()))
    ));
    expect(persistentQueryMetadata.getDataSourceType()).andReturn(DataSourceType.KSTREAM);
    EasyMock.expect(engine.buildMultipleQueries(eq("CTAS1"), anyObject(KsqlConfig.class), anyObject(Map.class)))
        .andReturn(Collections.singletonList(persistentQueryMetadata)).once();
    EasyMock.expect(engine.getQueryExecutionPlan(query, ksqlConfig))
        .andReturn(persistentQueryMetadata).once();

    EasyMock.replay(query, persistentQueryMetadata, engine);
    standaloneExecutor.start();
    EasyMock.verify(query, persistentQueryMetadata, engine);
  }

}