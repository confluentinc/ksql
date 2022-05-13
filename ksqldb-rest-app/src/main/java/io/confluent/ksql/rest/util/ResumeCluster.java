/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.parser.tree.Statement;
import java.util.Optional;

public class ResumeCluster extends Statement {

  public static final String RESUME_CLUSTER_STATEMENT_TEXT = "RESUME CLUSTER;";

  public ResumeCluster() {
    super(Optional.empty());
  }

  @Override
  public int hashCode() {
    return ResumeCluster.class.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return obj instanceof ResumeCluster;
  }

  @Override
  public String toString() {
    return RESUME_CLUSTER_STATEMENT_TEXT;
  }
}
