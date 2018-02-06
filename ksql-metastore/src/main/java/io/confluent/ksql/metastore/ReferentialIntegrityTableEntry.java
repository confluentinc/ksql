/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.metastore;

import java.util.HashSet;
import java.util.Set;

public class ReferentialIntegrityTableEntry implements Cloneable {

  private final Set<String> sourceForQueries;
  private final Set<String> sinkForQueries;

  public ReferentialIntegrityTableEntry() {
    sourceForQueries = new HashSet<>();
    sinkForQueries = new HashSet<>();
  }

  public ReferentialIntegrityTableEntry(Set<String> sourceForQueries, Set<String> sinkForQueries) {
    this.sourceForQueries = sourceForQueries;
    this.sinkForQueries = sinkForQueries;
  }

  public Set<String> getSourceForQueries() {
    return sourceForQueries;
  }

  public Set<String> getSinkForQueries() {
    return sinkForQueries;
  }

  public void removeQuery(String queryId) {
    sourceForQueries.remove(queryId);
    sinkForQueries.remove(queryId);
  }

  @Override
  public ReferentialIntegrityTableEntry clone() {
    Set<String> cloneSourceForQueries = new HashSet<>(sourceForQueries);
    Set<String> cloneSinkForQueries = new HashSet<>(sinkForQueries);
    return new ReferentialIntegrityTableEntry(cloneSourceForQueries,
                                              cloneSinkForQueries);
  }
}
