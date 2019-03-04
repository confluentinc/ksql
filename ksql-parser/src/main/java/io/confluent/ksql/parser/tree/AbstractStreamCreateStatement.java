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

package io.confluent.ksql.parser.tree;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractStreamCreateStatement extends Statement {
  public AbstractStreamCreateStatement(final Optional<NodeLocation> location) {
    super(location);
  }

  public abstract Map<String,Expression> getProperties();

  public abstract QualifiedName getName();

  public abstract List<TableElement> getElements();

  public abstract AbstractStreamCreateStatement copyWith(List<TableElement> elements, Map<String,
      Expression> properties);
}
