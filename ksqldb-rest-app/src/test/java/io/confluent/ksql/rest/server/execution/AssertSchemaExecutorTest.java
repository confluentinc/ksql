/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.server.execution;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.AssertSchema;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.AssertSchemaEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.KsqlErrorMessage;
import io.confluent.ksql.rest.server.resources.KsqlRestException;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import java.io.IOException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("OptionalGetWithoutIsPresent")
@RunWith(MockitoJUnitRunner.class)
public class AssertSchemaExecutorTest {
  @Mock
  private KsqlExecutionContext engine;
  @Mock
  private ServiceContext serviceContext;
  @Mock
  private SchemaRegistryClient srClient;
  private final KsqlConfig ksqlConfig = new KsqlConfig(ImmutableMap.of());

  @Before
  public void setUp() throws IOException, RestClientException {
    when(engine.getKsqlConfig()).thenReturn(ksqlConfig);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(srClient);

    // These are the schemas that don't exist
    when(srClient.getSchemaById(100)).thenThrow(new RestClientException("", 404, 40403));
    when(srClient.getLatestSchemaMetadata("abc")).thenThrow(new RestClientException("", 404, 40403));
    when(srClient.getAllSubjectsById(100)).thenThrow(new RestClientException("", 404, 40403));

    when(srClient.getAllSubjectsById(500)).thenReturn(ImmutableList.of("abc"));
    when(srClient.getSchemaById(222)).thenThrow(new IOException("something happened!"));
  }

  @Test
  public void shouldAssertSchemaBySubject() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("subjectName"), Optional.empty(), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.of("subjectName")));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.empty()));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(true));
  }

  @Test
  public void shouldAssertSchemaById() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.empty(), Optional.of(44), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.empty()));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.of(44)));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(true));
  }

  @Test
  public void shouldAssertSchemaBySubjectAndId() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("abc"), Optional.of(500), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.of("abc")));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.of(500)));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(true));
  }

  @Test
  public void shouldFailToAssertSchemaBySubject() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("abc"), Optional.empty(), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with subject name abc does not exist"));
  }

  @Test
  public void shouldFailToAssertSchemaById() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.empty(), Optional.of(100), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with id 100 does not exist"));
  }

  @Test
  public void shouldFailToAssertSchemaBySubjectAndId() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("def"), Optional.of(500), Optional.empty(), true);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with subject name def id 500 does not exist"));
  }

  @Test
  public void shouldAssertNotExistSchemaBySubject() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("abc"), Optional.empty(), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.of("abc")));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.empty()));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(false));
  }

  @Test
  public void shouldAssertNotExistSchemaById() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.empty(), Optional.of(100), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.empty()));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.of(100)));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(false));
  }

  @Test
  public void shouldAssertNotExistSchemaBySubjectAndId() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("def"), Optional.of(100), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final Optional<KsqlEntity> entity = AssertSchemaExecutor
        .execute(statement, mock(SessionProperties.class), engine, serviceContext).getEntity();

    // Then:
    assertThat("expected response!", entity.isPresent());
    assertThat(((AssertSchemaEntity) entity.get()).getSubject(), is(Optional.of("def")));
    assertThat(((AssertSchemaEntity) entity.get()).getId(), is(Optional.of(100)));
    assertThat(((AssertSchemaEntity) entity.get()).getExists(), is(false));
  }

  @Test
  public void shouldFailToAssertNotExistSchemaBySubject() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("subjectName"), Optional.empty(), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with subject name subjectName exists"));
  }

  @Test
  public void shouldFailToAssertNotExistSchemaById() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.empty(), Optional.of(2), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with id 2 exists"));
  }

  @Test
  public void shouldFailToAssertNotExistSchemaBySubjectAndId() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.of("abc"), Optional.of(500), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(e.getResponse().getStatus(), is(417));
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Schema with subject name abc id 500 exists"));
  }

  @Test
  public void shouldFailWhenSRClientFails() {
    // Given
    final AssertSchema assertSchema = new AssertSchema(Optional.empty(), Optional.empty(), Optional.of(222), Optional.empty(), false);
    final ConfiguredStatement<AssertSchema> statement = ConfiguredStatement
        .of(KsqlParser.PreparedStatement.of("", assertSchema),
            SessionConfig.of(ksqlConfig, ImmutableMap.of()));

    // When:
    final KsqlRestException e = assertThrows(KsqlRestException.class, () ->
        AssertSchemaExecutor.execute(statement, mock(SessionProperties.class), engine, serviceContext));

    // Then:
    assertThat(((KsqlErrorMessage) e.getResponse().getEntity()).getMessage(), is("Cannot check schema existence: something happened!"));
  }
}
