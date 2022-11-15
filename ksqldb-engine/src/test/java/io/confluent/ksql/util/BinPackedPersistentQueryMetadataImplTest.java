/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.util.QueryMetadata.Listener;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BinPackedPersistentQueryMetadataImplTest {
    private static final String SQL = "sql";
    private static final String EXECUTION_PLAN = "execution plan";
    private static final QueryId QUERY_ID = new QueryId("queryId");
    private static final String APPLICATION_ID = "applicationId";

    @Mock
    private PhysicalSchema physicalSchema;
    @Mock
    private DataSource sinkDataSource;
    @Mock
    private NamedTopology topology;
    @Mock
    private QuerySchemas schemas;
    @Mock
    private Map<String, Object> overrides;
    @Mock
    private ExecutionStep<?> physicalPlan;
    @Mock
    private ProcessingLogger processingLogger;
    @Mock
    private Listener listener;
    @Mock
    private SharedKafkaStreamsRuntimeImpl sharedKafkaStreamsRuntimeImpl;
    @Mock
    private Map<String, Object> streamsProperties;
    @Mock
    private Optional<ScalablePushRegistry> scalablePushRegistry;

    private PersistentQueryMetadata query;

    @Before
    public void setUp()  {
        query = new BinPackedPersistentQueryMetadataImpl(
            KsqlConstants.PersistentQueryType.CREATE_AS,
            SQL,
            physicalSchema,
            ImmutableSet.of(),
            EXECUTION_PLAN,
            APPLICATION_ID,
            topology,
            sharedKafkaStreamsRuntimeImpl,
            schemas,
            overrides,
            QUERY_ID,
            Optional.empty(),
            physicalPlan,
            processingLogger,
            Optional.of(sinkDataSource),
            listener,
            streamsProperties,
            scalablePushRegistry,
            (runtime) -> topology);

        query.initialize();
    }

    @Test
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    public void shouldCloseKafkaStreamsOnStop() {
        // When:
        query.stop();

        // Then:
        final InOrder inOrder = inOrder(sharedKafkaStreamsRuntimeImpl);
        inOrder.verify(sharedKafkaStreamsRuntimeImpl).stop(QUERY_ID, false);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotCallCloseCallbackOnStop() {
        // When:
        query.stop();

        // Then:
        verify(listener, times(0)).onClose(query);
    }

    @Test
    public void shouldCallKafkaStreamsCloseOnStop() {
        // When:
        query.stop();

        // Then:
        verify(sharedKafkaStreamsRuntimeImpl).stop(QUERY_ID, false);
    }
}
