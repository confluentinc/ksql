/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.metastore;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
final class ReferentialIntegrityTableEntry {

  private final Set<String> sourceForQueries = ConcurrentHashMap.newKeySet();
  private final Set<String> sinkForQueries = ConcurrentHashMap.newKeySet();

  ReferentialIntegrityTableEntry() {
  }

  private ReferentialIntegrityTableEntry(
      final Set<String> sourceForQueries,
      final Set<String> sinkForQueries
  ) {
    this.sourceForQueries.addAll(sourceForQueries);
    this.sinkForQueries.addAll(sinkForQueries);
  }

  Set<String> getSourceForQueries() {
    return Collections.unmodifiableSet(sourceForQueries);
  }

  Set<String> getSinkForQueries() {
    return Collections.unmodifiableSet(sinkForQueries);
  }

  void addSourceForQueries(final String queryId) {
    sourceForQueries.add(queryId);
  }

  void addSinkForQueries(final String queryId) {
    sinkForQueries.add(queryId);
  }

  void removeQuery(final String queryId) {
    sourceForQueries.remove(queryId);
    sinkForQueries.remove(queryId);
  }

  public ReferentialIntegrityTableEntry copy() {
    return new ReferentialIntegrityTableEntry(sourceForQueries, sinkForQueries);
  }
}
