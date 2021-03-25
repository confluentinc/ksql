package io.confluent.ksql.rest.server.execution;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.util.KsqlHostInfo;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.streams.state.HostInfo;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;



@RunWith(MockitoJUnitRunner.class)
public class ClusterQueryStatsTest {

  final EasyRandomParameters parameters = new EasyRandomParameters()
      .randomize(ImmutableMap.class, ImmutableMap::of)
      .randomize(ImmutableList.class, ImmutableList::of);
  final EasyRandom objectMother = new EasyRandom(parameters);
  final KsqlHostInfo localHostInfo = objectMother.nextObject(KsqlHostInfo.class);
  final SourceDescription sourceDescription = objectMother.nextObject(SourceDescription.class);

  @Test
  public void testMergesRemoteAndLocalResults() {
    List<RemoteSourceDescription> remote = objectMother
        .objects(RemoteSourceDescription.class, 5)
        .collect(toList());

    ClusterQueryStats res = ClusterQueryStats.create(
        localHostInfo,
        sourceDescription,
        remote
    );
    assertEquals(sourceDescription.getStatisticsMap(), res.getStats().get(localHostInfo));
    assertEquals(sourceDescription.getErrorStatsMap(), res.getErrors().get(localHostInfo));

    remote.forEach(rsd -> {
      assertEquals(
          rsd.getSourceDescription().getStatisticsMap(),
          res.getStats().get(rsd.getKsqlHostInfo()));
      assertEquals(
          rsd.getSourceDescription().getErrorStatsMap(),
          res.getErrors().get(rsd.getKsqlHostInfo()));
    });
  }

  @Test
  public void testCanCreateWithEmptyRemoteResults() {
    ClusterQueryStats res = ClusterQueryStats.create(
        localHostInfo,
        sourceDescription,
        new ArrayList<>()
    );
    assertEquals(sourceDescription.getStatisticsMap(), res.getStats().get(localHostInfo));
    assertEquals(sourceDescription.getErrorStatsMap(), res.getErrors().get(localHostInfo));
  }
}