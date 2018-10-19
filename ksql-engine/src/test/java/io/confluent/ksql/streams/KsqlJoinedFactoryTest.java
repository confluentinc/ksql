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
import org.apache.kafka.streams.kstream.Joined;
import org.junit.Test;

public class KsqlJoinedFactoryTest {
  private final StreamsStatics streamsStatics = mock(StreamsStatics.class);
  private final Joined joined = mock(Joined.class);
  @SuppressWarnings("unchecked")
  private final Serde<String> keySerde = mock(Serde.class);
  @SuppressWarnings("unchecked")
  private final Serde<GenericRow> valSerde = mock(Serde.class);
  @SuppressWarnings("unchecked")
  private final Serde<GenericRow> otherValSerde = mock(Serde.class);

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateJoinedWithNameWhenOptimizationsOn() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.OPTIMIZE));
    final String name = "name";
    expect(
        streamsStatics.joinedWith(same(keySerde), same(valSerde), same(otherValSerde), eq(name)))
        .andReturn(joined);
    replay(streamsStatics);
    final KsqlJoinedFactory joinedFactory = new KsqlJoinedFactory(ksqlConfig, streamsStatics);

    final Joined returned = joinedFactory.create(keySerde, valSerde, otherValSerde, name);

    assertThat(returned, sameInstance(joined));
    verify(streamsStatics);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldCreateJoinedWithNullNameWhenOptimizationsOff() {
    final KsqlConfig ksqlConfig = new KsqlConfig(
        Collections.singletonMap(
            StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION));
    expect(
        streamsStatics.joinedWith(same(keySerde), same(valSerde), same(otherValSerde), isNull()))
        .andReturn(joined);
    replay(streamsStatics);
    final KsqlJoinedFactory joinedFactory = new KsqlJoinedFactory(ksqlConfig, streamsStatics);

    final Joined returned = joinedFactory.create(keySerde, valSerde, otherValSerde, "name");

    assertThat(returned, sameInstance(joined));
    verify(streamsStatics);
  }
}
