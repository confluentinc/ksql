/*
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

package io.confluent.ksql.execution.common.operators;

import io.confluent.ksql.planner.plan.PlanNode;
import java.util.List;

/**
 * Represents a pipelined physical operator of the physical plan.
 */
public abstract class AbstractPhysicalOperator {

  public abstract void open();

  // Scan returns QueryRow, Project returns List<List<?>>
  public abstract Object next();

  public abstract void close();

  public abstract PlanNode getLogicalNode();

  public abstract void addChild(AbstractPhysicalOperator child);

  public abstract AbstractPhysicalOperator getChild(int index);

  public abstract List<AbstractPhysicalOperator> getChildren();

}
