/*
 * Copyright 2022 Confluent Inc.
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

import io.confluent.common.utils.IntegrationTest;
import io.confluent.ksql.integration.IntegrationTestHarness;
import io.confluent.ksql.integration.Retry;
import io.confluent.ksql.rest.entity.Queries;
import io.confluent.ksql.rest.entity.RunningQuery;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.rest.server.TestKsqlRestApp;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.PageViewDataProvider;
import io.confluent.ksql.util.UserDataProvider;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import kafka.zookeeper.ZooKeeperClientException;
import org.apache.kafka.streams.StreamsConfig;
import org.codehaus.plexus.util.FileUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
@RunWith(MockitoJUnitRunner.class)
public class TransientQueryResourceCleanerIntTest {
    private static final PageViewDataProvider PAGE_VIEWS_PROVIDER = new PageViewDataProvider();
    private static final String PAGE_VIEW_TOPIC = PAGE_VIEWS_PROVIDER.topicName();
    private static final String PAGE_VIEW_STREAM = PAGE_VIEWS_PROVIDER.sourceName();

    private static final UserDataProvider USER_DATA_PROVIDER = new UserDataProvider();
    private static final String USERS_TOPIC = USER_DATA_PROVIDER.topicName();
    private static final String USER_TABLE = USER_DATA_PROVIDER.sourceName();

    // Persistent Topics:
    // _confluent-ksql-default__command_topic,
    // PAGEVIEW_TOPIC,
    // USER_TOPIC
    private static final int numPersistentTopics = 3;

    // Transient Topics:
    // _confluent-ksql-default_transient_transient_PV_[0-9]\d*_[0-9]\d*-KafkaTopic_Right-Reduce-changelog
    // _confluent-ksql-default_transient_transient_PV_[0-9]\d*_[0-9]\d*-Join-repartition
    private static final int numTransientTopics = 2;

    private static final IntegrationTestHarness TEST_HARNESS = IntegrationTestHarness.build();
    private static final TestKsqlRestApp REST_APP_0 = TestKsqlRestApp
            .builder(TEST_HARNESS::kafkaBootstrapServers)
            .withStaticServiceContext(TEST_HARNESS::getServiceContext)
            .withProperty(KsqlRestConfig.LISTENERS_CONFIG, "http://localhost:8088")
            .withProperty(KsqlRestConfig.ADVERTISED_LISTENER_CONFIG, "http://localhost:8088")

            // configure initial delay for the cleanup service to be low for testing purpose
            .withProperty(KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_INITIAL_DELAY_SECONDS, 10)

            // configure time period for the cleanup service to be low for testing purpose
            .withProperty(KsqlConfig.KSQL_TRANSIENT_QUERY_CLEANUP_SERVICE_PERIOD_SECONDS, 2)
            .build();

    private static String stateDir;


    @ClassRule
    public static final RuleChain CHAIN = RuleChain
            .outerRule(Retry.of(3, ZooKeeperClientException.class, 3, TimeUnit.SECONDS))
            .around(TEST_HARNESS)
            .around(REST_APP_0);

    @Rule
    public final Timeout timeout = Timeout.seconds(120);

    private ExecutorService service;
    private Runnable backgroundTask;
    private boolean requestCompleted = false;

    @Before
    public void setUp() throws IOException {
        FileUtils.cleanDirectory(stateDir);

        service = Executors.newFixedThreadPool(1);
        final String sql = format("select * from %s pv left join %s u on pv.userid=u.userid emit changes;",
                PAGE_VIEW_STREAM, USER_TABLE);
        backgroundTask = () -> {
            RestIntegrationTestUtil.makeQueryRequest(
                    REST_APP_0,
                    sql,
                    Optional.empty());
            requestCompleted = true;
        };
    }

    @After
    public void tearDown() {
        service.shutdownNow();
    }

    @BeforeClass
    public static void setUpClass() {
        TEST_HARNESS.ensureTopics(PAGE_VIEW_TOPIC, USERS_TOPIC);

        RestIntegrationTestUtil.createStream(REST_APP_0, PAGE_VIEWS_PROVIDER);
        RestIntegrationTestUtil.createTable(REST_APP_0, USER_DATA_PROVIDER);

        stateDir = REST_APP_0.getEngine()
                .getKsqlConfig()
                .getKsqlStreamConfigProps()
                .getOrDefault(
                        StreamsConfig.STATE_DIR_CONFIG,
                        StreamsConfig.configDef()
                                .defaultValues()
                                .get(StreamsConfig.STATE_DIR_CONFIG))
                .toString();
    }

    @AfterClass
    public static void tearDownClass() {
    }

    @Test
    public void shouldCleanupLeakedTopics() throws InterruptedException {
        // Given:
        givenPushQuery();
        final String transientQueryId = getTransientQueryIds().get(0);
        Set<String> allTopics = TEST_HARNESS.getKafkaCluster().getTopics();

        // Should have the transient and persistent topics
        assertEquals(numPersistentTopics + numTransientTopics, allTopics.size());

        List<String> transientTopics = allTopics.stream()
                .filter(t -> t.contains("transient"))
                .collect(Collectors.toList());

        // Ensure that the transient topics are there
        assertEquals(numTransientTopics, transientTopics.size());
        assertTrue(transientTopics.get(0).contains(transientQueryId));
        assertTrue(transientTopics.get(1).contains(transientQueryId));

        // terminate the transient query
        RestIntegrationTestUtil.makeKsqlRequest(
                REST_APP_0,
                "terminate " + transientQueryId + ";"
        );

        // Give some time for the natural onClose() cleanup to take effect
        Thread.sleep(1000);

        Set<String> remainingTopics = TEST_HARNESS.getKafkaCluster().getTopics();

        // Transient topics should have been cleaned up; only persistent ones left
        assertEquals(numPersistentTopics, remainingTopics.size());

        // simulate "leaking" transient topics from the query we terminated
        // by recreating them
        TEST_HARNESS.ensureTopics(transientTopics.get(0), transientTopics.get(1));

        // ensure that the topics have been created ("leaked")
        assertEquals(numPersistentTopics + numTransientTopics,
                TEST_HARNESS.getKafkaCluster().getTopics().size());

        // When:
        // pause for a bit for the `TransientQueryCleanupService`
        // to clean up the leaked topics
        Thread.sleep(12000);

        // Then:
        // only the persistent topics are left and the transient topics have been cleaned up
        assertEquals(numPersistentTopics, TEST_HARNESS.getKafkaCluster().getTopics().size());
        assertEquals(0,
                TEST_HARNESS.getKafkaCluster().getTopics().stream()
                .filter(t -> t.contains("transient")).count());
    }

    @Test
    public void shouldCleanupLeakedStateDirs() throws InterruptedException, IOException {
        // Given:
        givenPushQuery();
        final String transientQueryId = getTransientQueryIds().get(0);
        File stateFolder = new File(stateDir);

        // state directory for the transient query should be present
        // it looks something like: /var/folders/yf/hc47k9x92tl3hrclf0_bblvh0000gp/T/kafka-streams/_confluent-ksql-default_transient_transient_PV_[0-9]\d*_[0-9]\d*
        assertEquals(1, Objects.requireNonNull(stateFolder.listFiles()).length);
        assertTrue(Objects.requireNonNull(stateFolder.list())[0].contains(transientQueryId));
        File leakedStateDir = new File(Objects.requireNonNull(stateFolder.listFiles())[0].toURI());

        // terminate the transient query
        RestIntegrationTestUtil.makeKsqlRequest(
                REST_APP_0,
                "terminate " + transientQueryId + ";"
        );

        // Give some time for the natural onClose() cleanup to take effect
        Thread.sleep(1000);

        // state file should be cleaned up
        assertEquals(0, Objects.requireNonNull(stateFolder.listFiles()).length);

        // simulate "leaking" state file from the query we terminated
        // by recreating the state of the killed transient query
        assertTrue(leakedStateDir.createNewFile());

        // state file has been "leaked"
        assertEquals(1, Objects.requireNonNull(stateFolder.listFiles()).length);
        assertTrue(Objects.requireNonNull(stateFolder.list())[0].contains(transientQueryId));

        // When:
        // pause for a bit for the `TransientQueryCleanupService`
        // to clean up the leaked state files
        Thread.sleep(12000);

        // Then:
        // the leaked state files have been cleaned up
        assertEquals(0, Objects.requireNonNull(stateFolder.listFiles()).length);
    }

    @Test
    public void shouldNotCleanupTopicsOfRunningQueries() throws InterruptedException {
        // Given:
        givenPushQuery();
        final String transientQueryId = getTransientQueryIds().get(0);
        Set<String> allTopics = TEST_HARNESS.getKafkaCluster().getTopics();

        // Should have the transient and persistent topics
        assertEquals(numPersistentTopics + numTransientTopics, allTopics.size());

        List<String> transientTopics = allTopics.stream()
                .filter(t -> t.contains("transient"))
                .collect(Collectors.toList());

        // Ensure that the transient topics are there
        assertEquals(numTransientTopics, transientTopics.size());
        assertTrue(transientTopics.get(0).contains(transientQueryId));
        assertTrue(transientTopics.get(1).contains(transientQueryId));

        // When:
        // pause for a bit for the `TransientQueryCleanupService`
        // to run a few times
        Thread.sleep(12000);

        // Then:
        // transient topics have not been accidentally cleaned up
        assertEquals(numPersistentTopics + numTransientTopics,
                TEST_HARNESS.getKafkaCluster().getTopics().size());
        assertEquals(numTransientTopics,
                TEST_HARNESS.getKafkaCluster().getTopics().stream()
                        .filter(t -> t.contains("transient")).count());
    }

    @Test
    public void shouldNotCleanupStateDirsOfRunningQueries() throws InterruptedException, IOException {
        // Given:
        givenPushQuery();
        final String transientQueryId = getTransientQueryIds().get(0);
        File stateFolder = new File(stateDir);

        // state directory for the transient query should be present
        // it looks something like: /var/folders/yf/hc47k9x92tl3hrclf0_bblvh0000gp/T/kafka-streams/_confluent-ksql-default_transient_transient_PV_[0-9]\d*_[0-9]\d*
        assertEquals(1, Objects.requireNonNull(stateFolder.listFiles()).length);
        assertTrue(Objects.requireNonNull(stateFolder.list())[0].contains(transientQueryId));

        // When:
        // pause for a bit for the `TransientQueryCleanupService`
        // to run a few times
        Thread.sleep(12000);

        // Then:
        // the state file of the transient query should still be there
        assertEquals(1, Objects.requireNonNull(stateFolder.listFiles()).length);
        assertTrue(Objects.requireNonNull(stateFolder.list())[0].contains(transientQueryId));
    }

    public List<RunningQuery> showQueries (){
        return ((Queries) RestIntegrationTestUtil.makeKsqlRequest(
                REST_APP_0,
                "show queries;"
        ).get(0)).getQueries();
    }

    public boolean checkForTransientQuery (){
        List<RunningQuery> queries = showQueries();
        return queries.stream()
                .anyMatch(q -> q.getId().toString().contains("transient"));
    }

    public List<String> getTransientQueryIds () {
        return showQueries().stream()
                .filter(q -> q.getId().toString().contains("transient"))
                .map(q -> q.getId().toString())
                .collect(Collectors.toList());
    }

    public void givenPushQuery() throws InterruptedException {
        service.execute(backgroundTask);

        boolean repeat = true;
        while (repeat){
            repeat = !checkForTransientQuery();
            Thread.sleep(1000L);
        }
    }
}
