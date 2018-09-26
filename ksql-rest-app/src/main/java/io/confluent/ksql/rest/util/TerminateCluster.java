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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class TerminateCluster extends Statement {

  public static final String TERMINATE_CLUSTER_STATEMENT_TEXT = "TERMINATE CLUSTER;";

  public static final String SOURCES_LIST_PARAM_NAME = "SOURCES_LIST";
  public static final String SOURCES_LIST_TYPE_PARAM_NAME = "SOURCES_LIST_TYPE";

  public enum SourceListType { KEEP, DELETE }

  private final List<String> sourcesList;
  private final boolean isKeep;

  public TerminateCluster(final List<String> sourcesList, final boolean isKeep) {
    super(Optional.empty());
    Objects.requireNonNull(sourcesList, "SOURCES_LIST cannot be null.");
    this.sourcesList = ImmutableList.copyOf(Objects.requireNonNull(sourcesList, "sourceList"));
    this.isKeep = isKeep;
  }

  public List<String> getSourcesList() {
    return sourcesList;
  }

  public boolean isKeep() {
    return isKeep;
  }

  @Override
  public int hashCode() {
    return Objects.hash(TERMINATE_CLUSTER_STATEMENT_TEXT, sourcesList, isKeep);
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
    return sourcesList.equals(terminateCluster.getSourcesList())
        && isKeep == terminateCluster.isKeep();
  }

  @Override
  public String toString() {
    return TERMINATE_CLUSTER_STATEMENT_TEXT;
  }
}
