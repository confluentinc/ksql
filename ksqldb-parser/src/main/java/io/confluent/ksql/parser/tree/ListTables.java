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

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.confluent.ksql.parser.tree.DescriptionType.BASE;
import static io.confluent.ksql.parser.tree.DescriptionType.DESCRIBE;
import static io.confluent.ksql.parser.tree.DescriptionType.EXTENDED;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ListTables extends Statement {

  private final DescriptionType descriptionType;

  public ListTables(
      final Optional<NodeLocation> location,
      final boolean showExtended,
      final boolean describe
  ) {
    super(location);
    this.descriptionType = showExtended ? EXTENDED : (describe ? DESCRIBE : BASE);
  }

  public boolean getShowExtended() {
    return descriptionType == EXTENDED;
  }

  public boolean getDescribe() {
    return descriptionType == DESCRIBE;
  }

  @Override
  public <R, C> R accept(final AstVisitor<R, C> visitor, final C context) {
    return visitor.visitListTables(this, context);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ListTables that = (ListTables) o;
    return descriptionType == that.descriptionType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(descriptionType);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("descriptionType", descriptionType)
        .toString();
  }
}
