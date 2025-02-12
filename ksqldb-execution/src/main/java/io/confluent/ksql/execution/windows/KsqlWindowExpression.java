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

package io.confluent.ksql.execution.windows;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.Node;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Optional;

@Immutable
public abstract class KsqlWindowExpression extends Node {

  protected final Optional<WindowTimeClause> retention;
  protected final Optional<WindowTimeClause> gracePeriod;
  protected final Optional<OutputRefinement> emitStrategy;

  KsqlWindowExpression(final Optional<NodeLocation> nodeLocation,
                       final Optional<WindowTimeClause> retention,
                       final Optional<WindowTimeClause> gracePeriod,
                       final Optional<OutputRefinement> emitStrategy) {
    super(nodeLocation);
    this.retention = retention;
    this.gracePeriod = gracePeriod;
    this.emitStrategy = emitStrategy;
  }

  public Optional<WindowTimeClause> getRetention() {
    return retention;
  }

  public Optional<WindowTimeClause> getGracePeriod() {
    return gracePeriod;
  }

  public Optional<OutputRefinement> getEmitStrategy() {
    return emitStrategy;
  }

  public abstract WindowInfo getWindowInfo();

  public abstract <R, C> R accept(WindowVisitor<R, C> visitor, C context);
}
