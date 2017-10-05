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

package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class Statements extends Node {

  public List<Statement> statementList;

  public Statements(List<Statement> statementList) {
    super(Optional.empty());
    this.statementList = statementList;
  }

  protected Statements(Optional<NodeLocation> location, List<Statement> statementList) {
    super(location);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {

    return visitor.visitStatements(this, context);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .toString();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    Statements o = (Statements) obj;
    return Objects.equals(statementList, o.statementList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementList);
  }
}
