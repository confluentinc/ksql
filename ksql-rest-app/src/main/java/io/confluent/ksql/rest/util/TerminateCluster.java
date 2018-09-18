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

  public final static String TERMINATE_CLUSTER_STATEMENT_TEXT = "TERMINATE CLUSTER;";

  private final List<String> keepTopics;
  private final List<String> deleteTopics;

  public TerminateCluster(final List<String> keepTopics, final List<String> deleteTopics) {
    super(Optional.empty());
    this.keepTopics = keepTopics;
    this.deleteTopics = deleteTopics;
  }

  public List<String> getKeepTopics() {
    return keepTopics;
  }

  public List<String> getDeleteTopics() {
    return deleteTopics;
  }

  @Override
  public int hashCode() {
    return Objects.hash(TERMINATE_CLUSTER_STATEMENT_TEXT, keepTopics, deleteTopics);
  }

  @Override
  public boolean equals(final Object obj) {
    return false;
  }

  @Override
  public String toString() {
    return TERMINATE_CLUSTER_STATEMENT_TEXT;
  }
}
