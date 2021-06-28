package io.confluent.ksql.internal;

import org.apache.kafka.streams.KafkaStreams;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class UtilizationMetricsListenerTest {

  List<KafkaStreams> streamsList;


  @Before
  public void setUp() {
    streamsList = new ArrayList<>();
  }

}
