/*o
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.util.KsqlHostInfo;
import org.apache.kafka.streams.state.HostInfo;

/**
 * Used to filter ksql hosts based on criteria specified in implementing classes.
 * One such example is a filter that checks whether hosts are alive or dead as determined
 * by the heartbeat agent.
 */
public interface RoutingFilter {

  boolean filter(
      HostInfo activeHost,
      KsqlHostInfo hostInfo,
      String storeName,
      int partition);

}
