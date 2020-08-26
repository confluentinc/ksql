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
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOptions;
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

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";
  private final static String TOPIC_1 = "topic1";
  private final static String TOPIC_2 = "topic2";

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
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);

    authorizationValidator = new KsqlAuthorizationValidatorImpl(accessValidator);
    securityContext = new KsqlSecurityContext(Optional.empty(), serviceContext);

    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_2, TOPIC_2);
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
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenSingleSelectWithoutReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldJoinSelectWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinSelectWithoutReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldThrowWhenJoinWithOneRightTopicWithReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_2, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_2
    )));
  }

  @Test
  public void shouldThrowWhenJoinWitOneLeftTopicWithReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldInsertIntoWithAllPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When/then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyReadPermissionsAllowed() {
    // Given:
    givenAccessDenied(TOPIC_2, AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2
    )));
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyWritePermissionsAllowed() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldThrowWhenCreateAsSelectWithoutReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream AS SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldCreateAsSelectExistingTopicWithWritePermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateAsSelectExistingStreamWithoutWritePermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_2, AclOperation.WRITE);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2
    )));
  }

  @Test
  public void shouldCreateAsSelectWithTopicAndWritePermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='%s') AS SELECT * FROM %s;",
        TOPIC_2, STREAM_TOPIC_1)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldPrintTopicWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_1));

    // When/Then
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenPrintTopicWithoutReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_1));

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  @Test
  public void shouldCreateSourceWithReadPermissionsAllowed() {
    // Given:
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_1)
    );

    // When/Then:
    authorizationValidator.checkAuthorization(securityContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateSourceWithoutReadPermissionsDenied() {
    // Given:
    givenAccessDenied(TOPIC_1, AclOperation.READ);
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_1)
    );

    // When:
    final Exception e = assertThrows(
        KsqlTopicAuthorizationException.class,
        () -> authorizationValidator.checkAuthorization(securityContext, metaStore, statement)
    );

    // Then:
    assertThat(e.getMessage(), containsString(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1
    )));
  }

  private void givenAccessDenied(final String topicName, final AclOperation operation) {
    doThrow(new KsqlTopicAuthorizationException(operation, Collections.singleton(topicName)))
        .when(accessValidator).checkAccess(securityContext, topicName, operation);
  }

  private void givenStreamWithTopic(
      final String streamName,
      final String topicName
  ) {
    final KsqlTopic sourceTopic = new KsqlTopic(
        topicName,
        KeyFormat.nonWindowed(FormatInfo.of(FormatFactory.KAFKA.name())),
        ValueFormat.of(FormatInfo.of(FormatFactory.JSON.name()))
    );

    final KsqlStream<?> streamSource = new KsqlStream<>(
        "",
        SourceName.of(streamName.toUpperCase()),
        SCHEMA,
        SerdeOptions.of(),
        Optional.empty(),
        false,
        sourceTopic
    );

    metaStore.putSource(streamSource, false);
  }
}
