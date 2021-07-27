/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.rest.server;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import io.confluent.ksql.execution.streams.RoutingFilter.Host;
import io.confluent.ksql.util.KsqlHostInfo;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ActiveHostFilterTest {

  private KsqlHostInfo activeHost;
  private KsqlHostInfo standByHost;
  private ActiveHostFilter activeHostFilter;

  @Before
  public void setUp() {
    activeHost = new KsqlHostInfo("activeHost", 2345);
    HostInfo activeHostInfo = new HostInfo("activeHost", 2345);
    standByHost = new KsqlHostInfo("standby1", 1234);
    activeHostFilter = new ActiveHostFilter(activeHostInfo);
  }

  @Test
  public void shouldFilterActive() {
    // Given:

    // When:
    final Host filterActive = activeHostFilter.filter(activeHost);
    final Host filterStandby = activeHostFilter.filter(standByHost);

    // Then:
    assertThat(filterActive.isSelected(), is(true));
    assertThat(filterStandby.isSelected(), is(false));
    assertThat(filterStandby.getReasonNotSelected(), is("Host is not the active host for this partition."));
  }
}
