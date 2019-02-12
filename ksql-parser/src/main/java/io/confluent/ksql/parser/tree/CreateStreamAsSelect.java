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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.ddl.DdlConfig;
import java.util.Map;
import java.util.Optional;

@Immutable
public class CreateStreamAsSelect extends CreateAsSelect {

  public CreateStreamAsSelect(
      final QualifiedName name,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties,
      final Optional<Expression> partitionByColumn
  ) {
    this(Optional.empty(), name, query, notExists, properties, partitionByColumn);
  }

  public CreateStreamAsSelect(
      final Optional<NodeLocation> location,
      final QualifiedName name,
      final Query query,
      final boolean notExists,
      final Map<String, Expression> properties,
      final Optional<Expression> partitionByColumn) {
    super(location, name, query, notExists, properties, partitionByColumn);
  }

  private CreateStreamAsSelect(
      final CreateStreamAsSelect other,
      final Map<String, Expression> properties
  ) {
    super(other, properties);
  }

  @Override
  public Sink getSink() {
    final Map<String, Expression> sinkProperties = getPartitionByColumn()
        .map(exp -> (Map<String, Expression>)ImmutableMap.<String, Expression>builder()
            .putAll(getProperties())
            .put(DdlConfig.PARTITION_BY_PROPERTY, exp)
            .build()
        )
        .orElse(getProperties());

    return Sink.of(getName().getSuffix(), true, sinkProperties);
  }

  @Override
  public CreateAsSelect copyWith(final Map<String, Expression> properties) {
    return new CreateStreamAsSelect(this, properties);
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitCreateStreamAsSelect(this, context);
  }
}
