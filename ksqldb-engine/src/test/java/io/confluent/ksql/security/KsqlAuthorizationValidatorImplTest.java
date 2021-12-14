/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.security;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;

import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.ServiceContext;
import java.util.Collections;
import java.util.Optional;

import org.apache.kafka.common.acl.AclOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAuthorizationValidatorImplTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final KeyFormat KAFKA_KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(
      FormatFactory.KAFKA.name()), SerdeFeatures.of());

  private static final ValueFormat KAFKA_VALUE_FORMAT = ValueFormat.of(FormatInfo.of(
      FormatFactory.KAFKA.name()), SerdeFeatures.of());

  private static final KeyFormat AVRO_KEY_FORMAT = KeyFormat.nonWindowed(FormatInfo.of(
      FormatFactory.AVRO.name()), SerdeFeatures.of());

  private static final ValueFormat AVRO_VALUE_FORMAT = ValueFormat.of(FormatInfo.of(
      FormatFactory.AVRO.name()), SerdeFeatures.of());

  private static final String KAFKA_STREAM_TOPIC = "kafka_stream";
  private static final String AVRO_STREAM_TOPIC = "avro_stream";
  private final static String KAFKA_TOPIC = "kafka_topic";
  private final static String AVRO_TOPIC = "avro_topic";

  private final static KsqlTopic KAFKA_KSQL_TOPIC =
      new KsqlTopic(KAFKA_TOPIC, KAFKA_KEY_FORMAT, KAFKA_VALUE_FORMAT);

  private final static KsqlTopic AVRO_KSQL_TOPIC =
      new KsqlTopic(AVRO_TOPIC, AVRO_KEY_FORMAT, AVRO_VALUE_FORMAT);

  @Mock
  private KsqlAccessValidator accessValidator;
  @Mock
  private ServiceContext serviceContext;

  private KsqlAuthorizationValidator authorizationValidator;
  private KsqlEngine ksqlEngine;
  private MutableMetaStore metaStore;
  private KsqlSecurityContext securityContext;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(
        serviceContext,
        metaStore,
        new MetricCollectors()
    );

    authorizationValidator = new KsqlAuthorizationValidatorImpl(accessValidator);
    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    givenStreamWithTopic(KAFKA_STREAM_TOPIC, KAFKA_KSQL_TOPIC);
    givenStreamWithTopic(AVRO_STREAM_TOPIC, AVRO_KSQL_TOPIC);
  }

  @After
  public void closeEngine() {
    ksqlEngine.close();
  }

  private Statement givenStatement(final String sql) {
    return ksqlEngine.prepare(ksqlEngine.parse(sql).get(0)).getStatement();
  }

  @Test
  public void shouldSingleSelectWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement("SELECT * FROM " + KAFKA_STREAM_TOPIC + ";");

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenSingleSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenSingleSelectWithoutSubjectReadPermissionsDenied() {
    // Given:
    givenSubjectAccessDenied(AVRO_TOPIC + "-key", AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", AVRO_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on Schema Registry subject: [%s-key]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldJoinSelectWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", KAFKA_STREAM_TOPIC, AVRO_STREAM_TOPIC)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", KAFKA_STREAM_TOPIC, AVRO_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenJoinSelectWithoutSubjectReadPermissionsDenied() {
    // Given:
    givenSubjectAccessDenied(AVRO_TOPIC + "-value", AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", KAFKA_STREAM_TOPIC, AVRO_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on Schema Registry subject: [%s-value]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenJoinWithOneRightTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(AVRO_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", KAFKA_STREAM_TOPIC, AVRO_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenJoinWitOneLeftTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", KAFKA_STREAM_TOPIC, AVRO_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldInsertIntoWithAllPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When/then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyReadPermissionsAllowed() {
    // Given:
    givenTopicAccessDenied(AVRO_TOPIC, AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on topic(s): [%s]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyWritePermissionsAllowed() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenCreateAsSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream AS SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldCreateAsSelectExistingTopicWithWritePermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateAsSelectExistingStreamWithoutWritePermissionsDenied() {
    // Given:
    givenTopicAccessDenied(AVRO_TOPIC, AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on topic(s): [%s]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenCreateAsSelectOnExistingTopicWithoutSchemaWritePermissions() {
    // Given:
    givenSubjectAccessDenied(AVRO_TOPIC + "-value", AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", AVRO_STREAM_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on Schema Registry subject: [%s-value]", AVRO_TOPIC
    )));
  }

  @Test
  public void shouldThrowWhenCreateAsSelectOnNewTopicWithoutValueSchemaWritePermissions() {
    // Given:
    givenSubjectAccessDenied("topic-value", AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='topic', value_format='AVRO') "
            + "AS SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on Schema Registry subject: [topic-value]"
    )));
  }

  @Test
  public void shouldThrowWhenCreateAsSelectOnNewTopicWithoutKeySchemaWritePermissions() {
    // Given:
    givenSubjectAccessDenied("topic-key", AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='topic', key_format='AVRO') "
            + "AS SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlSchemaAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on Schema Registry subject: [topic-key]"
    )));
  }

  @Test
  public void shouldNotThrowWhenCreateAsSelectOnNewTopicWithoutValueSchemaInferenceFormats() {
    // Given:
    givenSubjectAccessDenied("topic-value", AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='topic', value_format='DELIMITED') "
            + "AS SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // Then/When:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldNotThrowWhenCreateAsSelectOnNewTopicWithoutKeySchemaInferenceFormats() {
    // Given:
    givenSubjectAccessDenied("topic-key", AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='topic', value_format='DELIMITED') "
            + "AS SELECT * FROM %s;", KAFKA_STREAM_TOPIC)
    );

    // Then/When:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectWithTopicAndWritePermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='%s') AS SELECT * FROM %s;",
        AVRO_TOPIC, KAFKA_STREAM_TOPIC)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldPrintTopicWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format("Print '%s';", KAFKA_TOPIC));

    // When/Then
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenPrintTopicWithoutReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format("Print '%s';", KAFKA_TOPIC));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  @Test
  public void shouldCreateSourceWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", KAFKA_TOPIC)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateSourceWithoutReadPermissionsDenied() {
    // Given:
    givenTopicAccessDenied(KAFKA_TOPIC, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", KAFKA_TOPIC)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", KAFKA_TOPIC
    )));
  }

  private void givenTopicAccessDenied(final String topicName, final AclOperation operation) {
    doThrow(new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName)))
        .when(accessValidator).checkTopicAccess(securityContext, topicName, operation);
  }

  private void givenSubjectAccessDenied(final String subjectName, final AclOperation operation) {
    doThrow(new KsqlSchemaAuthorizationException(operation, subjectName))
        .when(accessValidator).checkSubjectAccess(securityContext, subjectName, operation);
  }

  private void givenStreamWithTopic(final String streamName, final KsqlTopic sourceTopic) {
    final KsqlStream<?> streamSource = new KsqlStream<>(
        "",
        SourceName.of(streamName.toUpperCase()),
        SCHEMA,
        Optional.empty(),
        false,
        sourceTopic,
        false
    );

    metaStore.putSource(streamSource, false);
  }
}
