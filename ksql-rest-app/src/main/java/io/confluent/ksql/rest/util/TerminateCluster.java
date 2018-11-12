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

  public static final String TERMINATE_CLUSTER_STATEMENT_TEXT = "TERMINATE CLUSTER;";

  public TerminateCluster(final List<String> deleteTopicList) {
    super(Optional.empty());
  }

  @Override
  public int hashCode() {
    return Objects.hash(TERMINATE_CLUSTER_STATEMENT_TEXT);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return TERMINATE_CLUSTER_STATEMENT_TEXT;
  }
}
