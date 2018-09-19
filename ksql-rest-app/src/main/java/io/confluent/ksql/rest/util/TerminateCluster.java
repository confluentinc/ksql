/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.parser.tree.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TerminateCluster extends Statement {

  private final List<String> keepSources;
  private final List<String> deleteSources;

  public static final String TERMINATE_CLUSTER_STATEMENT_TEXT = "TERMINATE CLUSTER;";

  public TerminateCluster(final List<String> keepSources, final List<String> deleteSources) {
    super(Optional.empty());
    Objects.requireNonNull(keepSources, "KEEP_SOURCES cannot be null.");
    Objects.requireNonNull(deleteSources, "DELETE_SOURCES cannot be null.");
    this.keepSources = keepSources;
    this.deleteSources = deleteSources;
  }


  public List<String> getKeepSources() {
    return keepSources;
  }

  public List<String> getDeleteSources() {
    return deleteSources;
  }

  @Override
  public int hashCode() {
    return Objects.hash(TERMINATE_CLUSTER_STATEMENT_TEXT, keepSources, deleteSources);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    final TerminateCluster terminateCluster = (TerminateCluster) obj;
    return keepSources.equals(terminateCluster.getKeepSources())
        && deleteSources.equals(terminateCluster.getDeleteSources());
  }

  @Override
  public String toString() {
    return TERMINATE_CLUSTER_STATEMENT_TEXT;
  }
}
