/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.streams.materialization.TableRow;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.query.Position;

@SuppressFBWarnings(value = "EI_EXPOSE_REP")
public final class KsMaterializedQueryResult<T extends TableRow> {

  final Iterator<T> rowIterator;
  final Optional<Position> position;

  public static <T extends TableRow> KsMaterializedQueryResult<T> rowIterator(
      final Iterator<T> iterator) {
    return new KsMaterializedQueryResult<>(iterator, Optional.empty());
  }

  public static <T extends TableRow> KsMaterializedQueryResult<T> rowIteratorWithPosition(
      final Iterator<T> iterator, final Position position) {
    return new KsMaterializedQueryResult<>(iterator, Optional.of(position));
  }

  private KsMaterializedQueryResult(final Iterator<T> rowIterator,
                                    final Optional<Position> position) {
    this.rowIterator = Objects.requireNonNull(rowIterator, "rowIterator");
    this.position = Objects.requireNonNull(position, "position");
  }

  public Iterator<T> getRowIterator() {
    return rowIterator;
  }

  public Optional<Position> getPosition() {
    return position;
  }
}
