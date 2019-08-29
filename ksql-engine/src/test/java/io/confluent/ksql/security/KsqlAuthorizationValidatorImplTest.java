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

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.ksql.engine.KsqlEngine;
import io.confluent.ksql.engine.KsqlEngineTestUtil;
import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.exception.KsqlSchemaAuthorizationException;
import io.confluent.ksql.exception.KsqlTopicAuthorizationException;
import io.confluent.ksql.execution.ddl.commands.KsqlTopic;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import org.apache.http.HttpStatus;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlAuthorizationValidatorImplTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn("F1", SqlTypes.STRING)
      .build();

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";
  private final static String TOPIC_NAME_1 = "topic1";
  private final static String TOPIC_NAME_2 = "topic2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private SchemaRegistryClient schemaRegistryClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KsqlAuthorizationValidator authorizationValidator;
  private KsqlEngine ksqlEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);

    authorizationValidator = new KsqlAuthorizationValidatorImpl();
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);
    when(serviceContext.getSchemaRegistryClient()).thenReturn(schemaRegistryClient);

    givenTopic(TOPIC_NAME_1, TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);

    givenTopic(TOPIC_NAME_2, TOPIC_2);
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
  public void shouldAllowAnyOperationIfPermissionsAreNull() {
    // Given:
    givenTopicPermissions(TOPIC_1, null);
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldSingleSelectWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenSingleSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldJoinSelectWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenJoinSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_2.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinWithOneRightTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_2.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinWitOneLeftTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldInsertIntoWithAllPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateAsSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream AS SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectExistingTopicWithWritePermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenCreateAsSelectExistingStreamWithoutWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2.name()
    ));


    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectWithTopicAndWritePermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='%s') AS SELECT * FROM %s;",
        TOPIC_2.name(), STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenCreateAsSelectHasSinkSchemaAccessDenied()
      throws IOException, RestClientException
  {
    final String sinkSchema = TOPIC_NAME_2 + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );
    doThrow(new RestClientException("", HttpStatus.SC_FORBIDDEN, -1)).when(schemaRegistryClient)
        .getLatestSchemaMetadata(sinkSchema);

    // Then:
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Write on Schema Registry subject(s): [%s]", sinkSchema
    ));


    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldPrintTopicWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_NAME_1));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenThrowPrintTopicWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_NAME_1));

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateSourceWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_NAME_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    verifyZeroInteractions(schemaRegistryClient);
  }

  @Test
  public void shouldCreateSourceFromAvroIfSchemaAccessIsAuthorized()
      throws IOException, RestClientException
  {
    final String sourceSchema = TOPIC_NAME_1 + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='AVRO');", TOPIC_NAME_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    verify(schemaRegistryClient, times(1))
        .getLatestSchemaMetadata(sourceSchema);
  }

  @Test
  public void shouldThrowWhenCreateSourceFromAvroHasSchemaAccessDenied()
      throws IOException, RestClientException
  {
    final String sourceSchema = TOPIC_NAME_1 + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='AVRO');", TOPIC_NAME_1)
    );
    doThrow(new RestClientException("", HttpStatus.SC_FORBIDDEN, -1)).when(schemaRegistryClient)
        .getLatestSchemaMetadata(sourceSchema);

    // Then:
    expectedException.expect(KsqlSchemaAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on Schema Registry subject(s): [%s]", sourceSchema
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateSourceWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_NAME_1)
    );

    // Then:
    expectedException.expect(KsqlTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowExceptionWhenTopicClientFails() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");
    givenTopicClientError(TOPIC_1);

    // Then:
    expectedException.expect(KafkaResponseGetFailedException.class);

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  private void givenTopic(final String topicName, final TopicDescription topicDescription) {
    when(topicDescription.name()).thenReturn(topicName);
    when(kafkaTopicClient.describeTopic(topicDescription.name())).thenReturn(topicDescription);
  }

  private void givenTopicPermissions(
      final TopicDescription topicDescription,
      final Set<AclOperation> operations
  ) {
    when(topicDescription.authorizedOperations()).thenReturn(operations);
  }

  private void givenStreamWithTopic(
      final String streamName,
      final TopicDescription topicDescription
  ) {
    final KsqlTopic sourceTopic = new KsqlTopic(
        topicDescription.name(),
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KsqlStream<?> streamSource = new KsqlStream<>(
        "",
        streamName.toUpperCase(),
        SCHEMA,
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        sourceTopic
    );

    metaStore.putSource(streamSource);
  }

  private void givenTopicClientError(final TopicDescription topic) {
    when(kafkaTopicClient.describeTopic(topic.name()))
        .thenThrow(KafkaResponseGetFailedException.class);
  }
}
