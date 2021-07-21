package io.confluent.ksql.util;

import io.confluent.ksql.query.KafkaStreamsBuilder;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class SharedKafkaStreamsRuntimeImplTest {

    @Mock
    private KafkaStreamsBuilder kafkaStreamsBuilder;

    @Mock
    private Map<String, Object> streamProps;

    @Mock
    private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;

    private SharedKafkaStreamsRuntimeImpl sharedKafkaStreamsRuntimeImpl;

    @Before
    public void setUp() throws Exception {
        when(kafkaStreamsBuilder.build(any())).thenReturn(kafkaStreamsNamedTopologyWrapper);
        sharedKafkaStreamsRuntimeImpl = new SharedKafkaStreamsRuntimeImpl(
            kafkaStreamsBuilder,
            5,
            streamProps
        );
    }

    @Test
    public void shouldAddQuery() {
//        sharedKafkaStreamsRuntime.addQuery();
    }
}