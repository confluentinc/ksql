/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.integration;

import static io.confluent.ksql.GenericKey.genericKey;
import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.startsWith;

import com.google.common.collect.ImmutableMap;
import io.confluent.common.utils.IntegrationTest;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.rest.client.BasicCredentials;
import io.confluent.ksql.rest.entity.CommandStatus.Status;
import io.confluent.ksql.rest.entity.CommandStatusEntity;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.rest.entity.PushQueryId;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.SourceDescriptionEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.serde.FormatFactory;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.avro.AvroFormat;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.PageViewDataProvider;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class TerminateTransientQueryFunctionalTest {

  private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
  private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
  private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

  private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();

  private static final TestKsqlRestApp REST_APP = TestKsqlRestApp
      .builder(TEST_HARNESS::kafkaBootstrapServers)
      .withStaticServiceContext(TEST_HARNESS::getServiceContext)
      .withProperty(KsqlConfig.SCHEMA_REGISTRY_URL_PROPERTY, "http://localhost:8088")
      .build();

  @ClassRule
  public static final RuleChain CHAIN = RuleChain
      .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
      .around(TEST_HARNESS)
      .around(REST_APP);

  @BeforeClass
  public static void setUpClass() {
    TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC);

    RestIntegrationTestUtil.createStream(REST_APP, PAGE_VIEWS_PROVIDER);

    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "CREATE STREAM S AS SELECT * FROM " + PAGE_VIEW_STREAM + ";"
    );
  }

  @Test
  public void shouldTerminatePushQueryOnSingleNode() {
    // Given:
    final List<StreamedRow> results = RestIntegrationTestUtil.makeQueryRequest(
        REST_APP,
        "SELECT * FROM " + PAGE_VIEW_STREAM + " EMIT CHANGES;",
        Optional.of(BasicCredentials.of("user", "pwd"))
    );

    final StreamedRow result = results.get(0);
    final QueryId queryId = result.getHeader().get().getQueryId();

    // When:
    RestIntegrationTestUtil.makeKsqlRequest(
        REST_APP,
        "terminate" + queryId.toString() + ";"
    );

    // Then:
    assertThat(
        "Should terminate transient query using query id",
        REST_APP.getTransientQueries().isEmpty(),
        is(true)
    );

  }


}