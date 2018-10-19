package io.confluent.ksql.streams;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Materialized;
import org.junit.Test;

public class KsqlMaterializedFactoryTest {
  private final StreamsStatics streamsStatics = mock(StreamsStatics.class);
  private final Materialized materialized = mock(Materialized.class);
  @SuppressWarnings("unchecked")
  private final Serde<String> keySerde = mock(Serde.class);
  @SuppressWarnings("unchecked")
  private final Serde<GenericRow> valSerde = mock(Serde.class);

  @SuppressWarnings("unchecked")
  @Test
  public void shouldBuildMaterializedCorrectlyWithOptimizationsOn() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE));
    final String name = "name";
    expect(streamsStatics.materializedAs(name))
        .andReturn(materialized);
    expect(materialized.withKeySerde(keySerde))
        .andReturn(materialized);
    expect(materialized.withValueSerde(valSerde))
        .andReturn(materialized);
    replay(streamsStatics, materialized);
    final MaterializedFactory materializedFactory = new KsqlMaterializedFactory(
        ksqlConfig,
        streamsStatics);

    // When:
    final Materialized returned = materializedFactory.create(keySerde, valSerde, name);

    verify(streamsStatics, materialized);
    assertThat(returned, sameInstance(materialized));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldBuildMaterializedCorrectlyWithOptimizationsOff() {
    // Given:
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION));
    expect(streamsStatics.materializedWith(keySerde, valSerde))
        .andReturn(materialized);
    replay(streamsStatics);
    final MaterializedFactory materializedFactory = new KsqlMaterializedFactory(
        ksqlConfig,
        streamsStatics);

    // When:
    final Materialized returned = materializedFactory.create(keySerde, valSerde, "name");

    // Then:
    verify(streamsStatics);
    assertThat(returned, sameInstance(materialized));
  }
}
