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

package io.confluent.ksql.engine;

import static org.mockito.Mockito.when;

import io.confluent.ksql.exception.KafkaResponseGetFailedException;
import io.confluent.ksql.function.InternalFunctionRegistry;
import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.metastore.MutableMetaStore;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.metastore.model.KsqlStream;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.ValueFormat;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthorizationTopicAccessValidatorTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.of(SchemaBuilder
      .struct()
      .field("F1", Schema.OPTIONAL_STRING_SCHEMA)
      .build());

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private TopicAccessValidator accessValidator;
  private KsqlEngine ksqlEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    ksqlEngine = KsqlEngineTestUtil.createKsqlEngine(serviceContext, metaStore);

    accessValidator = new AuthorizationTopicAccessValidator();
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);

    givenTopic("topic1", TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);

    givenTopic("topic2", TOPIC_2);
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
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldSingleSelectWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldSingleSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_1.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
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
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldJoinSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_1.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldJoinWithOneRightTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_2.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldJoinWitOneLeftTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_1.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
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
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldInsertIntoWithOnlyReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Write Kafka topic: [%s]", TOPIC_2.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldInsertIntoWithOnlyWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_1.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream AS SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Read Kafka topic: [%s]", TOPIC_1.name()
    ));

    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
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
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldCreateAsSelectExistingStreamWithoutWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KsqlException.class);
    expectedException.expectMessage(String.format(
        "Failed to Write Kafka topic: [%s]", TOPIC_2.name()
    ));


    // When:
    accessValidator.validate(serviceContext, metaStore, statement);
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
    accessValidator.validate(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
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
    accessValidator.validate(serviceContext, metaStore, statement);
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
