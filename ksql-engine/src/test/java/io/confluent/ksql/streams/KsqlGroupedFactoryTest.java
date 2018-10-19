package io.confluent.ksql.streams;

import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isNull;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.same;
import static org.easymock.EasyMock.verify;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Collections;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.junit.Test;

public class KsqlGroupedFactoryTest {
  private final StreamsStatics streamsStatics = mock(StreamsStatics.class);
  private final Grouped grouped = mock(Grouped.class);
  @SuppressWarnings("unchecked")
  private final Serde<String> keySerde = mock(Serde.class);
  @SuppressWarnings("unchecked")
  private final Serde<GenericRow> valSerde = mock(Serde.class);

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateGroupedWithNameWhenOptimizationsOn() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE));
    final String name = "name";
    expect(streamsStatics.groupedWith(eq(name), same(keySerde), same(valSerde)))
        .andReturn(grouped);
    replay(streamsStatics);
    final KsqlGroupedFactory groupedFactory = new KsqlGroupedFactory(ksqlConfig, streamsStatics);

    final Grouped returned = groupedFactory.create(name, keySerde, valSerde);

    assertThat(returned, sameInstance(grouped));
    verify(streamsStatics);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateGroupedWithNullNameWhenOptimizationsOff() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION));
    expect(streamsStatics.groupedWith(isNull(), same(keySerde), same(valSerde)))
        .andReturn(grouped);
    replay(streamsStatics);
    final KsqlGroupedFactory groupedFactory = new KsqlGroupedFactory(ksqlConfig, streamsStatics);

    final Grouped returned = groupedFactory.create("name", keySerde, valSerde);

    assertThat(returned, sameInstance(grouped));
    verify(streamsStatics);
  }
}
