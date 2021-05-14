/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.api.impl;

import io.vertx.ext.dropwizard.Match;
import io.vertx.ext.dropwizard.MatchType;
import java.util.ArrayList;
import java.util.List;

public final class MonitoredEndpoints {

  private MonitoredEndpoints() {
  }

  /**
   * @return List of endpoint matches that we're going to provide metrics for
   */
  public static List<Match> getMonitoredEndpoints() {
    final List<Match> matches = new ArrayList<>();
    matches.add(new Match().setValue("/ksql"));
    matches.add(new Match().setValue("/ksql/terminate"));
    matches.add(new Match().setValue("/query"));
    matches.add(new Match().setValue("/query-stream"));
    matches.add(new Match().setValue("/inserts-stream"));
    matches.add(new Match().setValue("/close-query"));
    matches.add(new Match().setValue("/ws/query.*").setType(MatchType.REGEX).setAlias("ws-query"));
    return matches;
  }
}
