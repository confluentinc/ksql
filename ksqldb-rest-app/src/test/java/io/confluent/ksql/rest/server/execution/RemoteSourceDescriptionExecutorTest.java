package io.confluent.ksql.rest.server.execution;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import io.confluent.ksql.rest.entity.SourceDescription;
import io.confluent.ksql.rest.entity.SourceDescriptionList;
import io.confluent.ksql.util.Pair;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.streams.state.HostInfo;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.jeasy.random.FieldPredicates.inClass;
import static org.jeasy.random.FieldPredicates.named;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RemoteSourceDescriptionExecutorTest  {
  final EasyRandomParameters parameters = new EasyRandomParameters()
      .randomize(ImmutableMap.class, ImmutableMap::of)
      .excludeField(
          named("fields")
              .and(inClass(SourceDescription.class))
      ).excludeField(
          named("windowType") // since JDK17 does not support randomizing optionals
              .and(inClass(SourceDescription.class))
      );
  final EasyRandom objectMother = new EasyRandom(parameters);


  @Mock
  RemoteHostExecutor augmenter = mock(RemoteHostExecutor.class);

  List<HostInfo> hosts = objectMother
      .objects(HostInfo.class, 3)
      .collect(Collectors.toList());

  List<SourceDescriptionList> descriptionLists = objectMother
      .objects(SourceDescriptionList.class, 3)
      .collect(Collectors.toList());

  Map<HostInfo, SourceDescriptionList> response = Streams
      .zip(hosts.stream(), descriptionLists.stream(), AbstractMap.SimpleImmutableEntry::new)
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void itShouldReturnRemoteSourceDescriptionsGroupedByName() {
    // Given
    when(augmenter.fetchAllRemoteResults()).thenReturn(new Pair(response, ImmutableSet.of()));

    Multimap<String, SourceDescription> res = RemoteSourceDescriptionExecutor.fetchSourceDescriptions(augmenter);

    Map<String, List<SourceDescription>> queryHostCounts = descriptionLists.stream()
        .flatMap((v) -> v.getSourceDescriptions().stream())
        .collect(Collectors.groupingBy(SourceDescription::getName));

    assertThat(res.values(), everyItem(instanceOf(SourceDescription.class)));
    response.forEach((host, value) -> value.getSourceDescriptions().forEach(
        (sd) -> assertThat(res.get(sd.getName()), hasSize(queryHostCounts.get(sd.getName()).size()))
    ));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Test
  public void itShouldReturnEmptyIfNoRemoteResults() {
    // Given
    when(augmenter.fetchAllRemoteResults()).thenReturn(new Pair(ImmutableMap.of(), response.keySet()));

    Multimap<String, SourceDescription> res = RemoteSourceDescriptionExecutor.fetchSourceDescriptions(augmenter);
    response.forEach((key, value) -> value.getSourceDescriptions().forEach(
        (sd) -> assertThat(res.get(sd.getName()), hasSize(0))
    ));
  }
}