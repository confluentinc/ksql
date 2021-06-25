package io.confluent.ksql.util;

import io.confluent.ksql.query.KafkaStreamsBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class SharedKafkaStreamsRuntimeTest {

    @Mock
    private KafkaStreamsBuilder kafkaStreamsBuilder;

    @Mock
    private Map<String, Object> streamProps;

    private SharedKafkaStreamsRuntime sharedKafkaStreamsRuntime;

    @Before
    public void setUp() throws Exception {
        sharedKafkaStreamsRuntime = new SharedKafkaStreamsRuntime(
            kafkaStreamsBuilder,
            5,
            streamProps
        );
    }

    @Test void shouldAddQuery() {
//        sharedKafkaStreamsRuntime.addQuery();
    }
}