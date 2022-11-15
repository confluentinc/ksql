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

package io.confluent.ksql.engine;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.query.QueryRegistry;
import io.confluent.ksql.services.DisabledKsqlClient;
import io.confluent.ksql.services.FakeKafkaTopicClient;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.services.ServiceContextFactory;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.QueryMetadata;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.Silent.class)
public class TransientQueryCleanupServiceTest {
    private static final KsqlConfig KSQL_CONFIG = new KsqlConfig(Collections.singletonMap(
            KsqlConfig.KSQL_SERVICE_ID_CONFIG, KsqlConfig.KSQL_SERVICE_ID_DEFAULT));

    private static final String internalPrefix = "_confluent-ksql-default_transient";
    private static final String APP_ID_1 = internalPrefix + "_transient_PV_11111_12345";
    private static final String APP_ID_2 = internalPrefix + "_transient_PV_22222_67890";
    private static final String APP_ID_3 = internalPrefix + "_transient_PV_33333_13579";
    private static final String APP_ID_4 = internalPrefix + "_transient_PV_44444_24680";
    private static final String APP_ID_5 = internalPrefix + "_transient_PV_55555_09876";
    private static final List<String> ALL_APP_IDS = ImmutableList.of(APP_ID_1, APP_ID_2, APP_ID_3, APP_ID_4, APP_ID_5);

    @Spy
    static ServiceContext ctx = ServiceContextFactory.create(KSQL_CONFIG, DisabledKsqlClient::instance);
    @Spy
    static QueryMetadata q1;
    @Spy
    static QueryMetadata q2;
    @Spy
    static QueryMetadata q3;
    @Spy
    static QueryMetadata q4;
    @Spy
    static QueryMetadata q5;
    @Spy
    QueryRegistry registry;
    @Spy
    TransientQueryCleanupService service = new TransientQueryCleanupService(ctx, KSQL_CONFIG);

    final String STATE_DIR = service.getStateDir();

    @Before
    public void setUp() {
        service.setQueryRegistry(registry);

        when(q1.getQueryId()).thenReturn(new QueryId(APP_ID_1));
        when(q2.getQueryId()).thenReturn(new QueryId(APP_ID_2));
        when(q3.getQueryId()).thenReturn(new QueryId(APP_ID_3));
        when(q4.getQueryId()).thenReturn(new QueryId(APP_ID_4));
        when(q5.getQueryId()).thenReturn(new QueryId(APP_ID_5));

        FakeKafkaTopicClient client = new FakeKafkaTopicClient();
        ALL_APP_IDS.forEach(id -> {
            client.createTopic(id + "-KafkaTopic_Right-Reduce-changelog", 1, (short) 1);
            client.createTopic(id + "-Join-repartition", 1, (short) 1);
        });
        when(service.getTopicClient()).thenReturn(client);
    }

    @Test
    public void shouldReturnTrueForQueriesGuaranteedToBeRunningAtSomePoint() {
        // Given:
        ALL_APP_IDS.forEach(id -> service.registerRunningQuery(id));

        // When:
        List<Boolean> results = ALL_APP_IDS.stream()
                .map(service::wasQueryGuaranteedToBeRunningAtSomePoint)
                .collect(Collectors.toList());

        // Then:
        results.forEach(result -> assertEquals(result, true));
    }

    @Test
    public void shouldReturnFalseForQueriesNotGuaranteedToBeRunningAtSomePoint() {
        // Given:
        ALL_APP_IDS.subList(0, 3).forEach(id -> service.registerRunningQuery(id));

        // When:
        List<Boolean> results = ALL_APP_IDS.stream()
                .map(service::wasQueryGuaranteedToBeRunningAtSomePoint)
                .collect(Collectors.toList());

        // Then:
        results.subList(0, 3).forEach(result -> assertEquals(result, true));
        results.subList(3, 5).forEach(result -> assertEquals(result, false));
    }

    @Test
    public void shouldOnlyReturnFalseForNonTerminatedQueries() {
        // Given:
        when(registry.getAllLiveQueries()).thenReturn(ImmutableList.of(q1, q2, q3));

        // When:
        List<Boolean> results = ALL_APP_IDS.stream()
                .map(service::isCorrespondingQueryTerminated)
                .collect(Collectors.toList());

        // Then:
        results.subList(0, 3).forEach(result -> assertEquals(result, false));
        results.subList(3, 5).forEach(result -> assertEquals(result, true));
    }

    @Test
    public void shouldOnlyReturnTrueForPresentLocalCommands() {
        // Given:
        Set<String> localCommands = new HashSet<>(ALL_APP_IDS.subList(0, 3));
        service.setLocalCommandsQueryAppIds(localCommands);

        // When:
        List<Boolean> results = ALL_APP_IDS.stream()
                .map(service::foundInLocalCommands)
                .collect(Collectors.toList());

        // Then:
        results.subList(0, 3).forEach(result -> assertEquals(result, true));
        results.subList(3, 5).forEach(result -> assertEquals(result, false));
    }

    @Test
    public void localCommandsShouldBeConsideredLeaked() {
        // Given:
        List<String> localCommandAppIds = ALL_APP_IDS.subList(0, 3);
        service.setLocalCommandsQueryAppIds(new HashSet<>(localCommandAppIds));

        // When:
        Set<String> results = new HashSet<>(service.findLeakedTopics());

        // Then:
        Set<String> expected = new HashSet<>();
        localCommandAppIds.forEach(id -> {
            expected.add(id + "-KafkaTopic_Right-Reduce-changelog");
            expected.add(id + "-Join-repartition");
        });
        assertEquals(expected, results);
    }

    @Test
    public void shouldDetectTheCorrectLeakedTopics() {
        Set<String> results = new HashSet<>(service.findLeakedTopics());
        assertEquals(results.size(), 0);

        when(registry.getAllLiveQueries()).thenReturn(ImmutableList.of(q1, q4, q5));
        results = new HashSet<>(service.findLeakedTopics());
        assertEquals(results.size(), 0);

        service.registerRunningQuery(APP_ID_2);
        Set<String> expected = new HashSet<>(ImmutableList.of(
                        APP_ID_2 + "-KafkaTopic_Right-Reduce-changelog",
                        APP_ID_2 + "-Join-repartition"));
        results = new HashSet<>(service.findLeakedTopics());
        assertEquals(expected, results);

        service.registerRunningQuery(APP_ID_1);
        results = new HashSet<>(service.findLeakedTopics());
        assertEquals(expected, results);

        service.registerRunningQuery(APP_ID_3);
        results = new HashSet<>(service.findLeakedTopics());
        expected.addAll(
                ImmutableList.of(
                        APP_ID_3 + "-KafkaTopic_Right-Reduce-changelog",
                        APP_ID_3 + "-Join-repartition"));
        assertEquals(expected, results);
    }

    @Test
    public void shouldDetectTheCorrectLeakedStateDirs() throws IOException {
        Set<String> results = new HashSet<>(service.findLeakedStateDirs());
        assertEquals(results.size(), 0);

        when(service.listAllStateFiles()).thenReturn(ALL_APP_IDS);
        assertTrue(service.findLeakedStateDirs().isEmpty());

        when(registry.getAllLiveQueries()).thenReturn(ImmutableList.of(q2, q3));
        assertTrue(service.findLeakedStateDirs().isEmpty());

        service.registerRunningQuery(APP_ID_2);
        assertTrue(service.findLeakedStateDirs().isEmpty());

        service.registerRunningQuery(APP_ID_1);
        assertEquals(service.findLeakedStateDirs(),
                ImmutableList.of(APP_ID_1));

        service.registerRunningQuery(APP_ID_4);
        assertEquals(service.findLeakedStateDirs(),
                ImmutableList.of(APP_ID_1, APP_ID_4));

        service.setLocalCommandsQueryAppIds(ImmutableSet.of(APP_ID_5));
        assertEquals(service.findLeakedStateDirs(),
                ImmutableList.of(APP_ID_1, APP_ID_4, APP_ID_5));
    }
}