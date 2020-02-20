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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.parser.NodeLocation;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ListQueries extends Statement {

  private final boolean showAll;
  private final boolean showExtended;

  public ListQueries(
      final Optional<NodeLocation> location,
      final boolean showAll,
      final boolean showExtended
  ) {
    super(location);
    this.showExtended = showExtended;
    this.showAll = showAll;
  }

  public boolean getShowAll() {
    return showAll;
  }

  public boolean getShowExtended() {
    return showExtended;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ListQueries that = (ListQueries) o;
    return showAll == that.showAll && showExtended == that.showExtended;
  }

  @Override
  public int hashCode() {
    return Objects.hash(showExtended, showAll);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("showAll", showAll)
        .add("showExtended", showExtended)
        .toString();
  }
}
