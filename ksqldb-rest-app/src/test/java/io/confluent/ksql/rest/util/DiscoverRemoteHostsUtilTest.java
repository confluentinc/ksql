/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.KsqlHostInfo;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DiscoverRemoteHostsUtilTest {

  private static final KsqlHostInfo THIS_HOST_INFO = new KsqlHostInfo("this_host", 8088);
  private static final HostInfo OTHER_HOST_INFO = new HostInfo("other_host", 8088);

  @Mock
  private PersistentQueryMetadata runningQuery;
  @Mock
  private PersistentQueryMetadata notRunningQuery;
  @Mock
  private StreamsMetadata streamsMetadata;

  @Before
  public void setUp() {
    when(runningQuery.getState()).thenReturn(State.RUNNING);
    when(runningQuery.getAllStreamsHostMetadata()).thenReturn(Collections.singleton(streamsMetadata));
    when(notRunningQuery.getState()).thenReturn(State.CREATED);
    when(streamsMetadata.hostInfo()).thenReturn(OTHER_HOST_INFO);
  }

  @Test
  public void shouldFilterQueryMetadataByState() {
    // When:
    final Set<HostInfo> info = DiscoverRemoteHostsUtil.getRemoteHosts(
        ImmutableList.of(runningQuery, notRunningQuery),
        THIS_HOST_INFO
    );

    // Then:
    assertThat(info, contains(OTHER_HOST_INFO));

    verify(notRunningQuery, never()).getAllStreamsHostMetadata();
  }

}