/*
 * Copyright 2020 Confluent Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class AlterSource extends Statement implements ExecutableDdlStatement {
  private final SourceName name;
  private final DataSourceType dataSourceType;
  private final ImmutableList<AlterOption> alterOptions;

  public AlterSource(
      final SourceName name,
      final DataSourceType dataSourceType,
      final List<AlterOption> alterOptions
  ) {
    this(Optional.empty(), name, dataSourceType, alterOptions);
  }

  public AlterSource(
      final Optional<NodeLocation> location,
      final SourceName name,
      final DataSourceType dataSourceType,
      final List<AlterOption> alterOptions
  ) {
    super(location);
    this.name = name;
    this.dataSourceType = dataSourceType;
    this.alterOptions = ImmutableList.copyOf(alterOptions);
  }

  public SourceName getName() {
    return name;
  }

  public DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public List<AlterOption> getAlterOptions() {
    return alterOptions;
  }

  @Override
  public  <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterSource(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, dataSourceType, alterOptions);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final AlterSource that = (AlterSource) o;
    return Objects.equals(name, that.name)
        && Objects.equals(dataSourceType, that.dataSourceType)
        && Objects.equals(alterOptions, that.alterOptions);
  }

  @Override
  public String toString() {
    return "AlterSource{"
        + "name=" + getName()
        + ", dataSourceType=" + getDataSourceType()
        + ", alterOptions=" + getAlterOptions()
        + "}";
  }
}
