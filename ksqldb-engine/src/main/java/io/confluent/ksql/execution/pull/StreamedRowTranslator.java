/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.execution.pull;

import com.google.common.base.Preconditions;
import io.confluent.ksql.rest.entity.KsqlHostInfoEntity;
import io.confluent.ksql.rest.entity.StreamedRow;
import io.confluent.ksql.rest.entity.StreamedRow.Header;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.ConsistencyOffsetVector;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class StreamedRowTranslator implements Iterator<PullQueryRow> {

  private final Deque<PullQueryRow> rowQueue = new ArrayDeque<>();
  private final LogicalSchema expectedSchema;
  private final Optional<ConsistencyOffsetVector> offsetVector;

  private Header header = null;

  public StreamedRowTranslator(
      final LogicalSchema expectedSchema,
      final Optional<ConsistencyOffsetVector> offsetVector
  ) {
    this.expectedSchema = expectedSchema;
    this.offsetVector = offsetVector;
  }

  @Override
  public PullQueryRow next() {
    return rowQueue.pollFirst();
  }

  @Override
  public boolean hasNext() {
    return !rowQueue.isEmpty();
  }

  public void add(final List<StreamedRow> rows) {
    if (rows == null || rows.isEmpty()) {
      return;
    }

    for (int i = 0; i < rows.size(); i++) {
      final StreamedRow row = rows.get(i);
      if (row.getHeader().isPresent()) {
        handleHeader(row);
      } else if (row.getErrorMessage().isPresent()) {
        // If we receive an error that's not a network error, we let that bubble up.
        throw new KsqlException(row.getErrorMessage().get().getMessage());
      } else if (!row.getRow().isPresent()) {
        handleNonDataRows(row, i, offsetVector);
      } else {
        final List<?> columns = row.getRow().get().getColumns();
        Preconditions.checkNotNull(header, "Expected to read header row before any other data.");
        rowQueue.add(new PullQueryRow(
            columns,
            expectedSchema,
            row.getSourceHost(),
            Optional.empty()
        ));
      }
    }
  }

  private void handleHeader(final StreamedRow row) {
    final Optional<Header> header = row.getHeader();
    if (header.isPresent()) {
      validateSchema(expectedSchema, header.get().getSchema(), row.getSourceHost());
      this.header = header.get();
    }
  }

  private static void handleNonDataRows(
      final StreamedRow row,
      final int i,
      final Optional<ConsistencyOffsetVector> offsetVector
  ) {
    if (row.getConsistencyToken().isPresent()) {
      if (offsetVector.isPresent()) {
        final String token = row.getConsistencyToken().get().getConsistencyToken();
        final ConsistencyOffsetVector received = ConsistencyOffsetVector.deserialize(token);
        offsetVector.get().merge(received);
      }
    } else if (!row.getFinalMessage().isPresent()) {
      throw new KsqlException("Missing row data on row " + i + " of chunk");
    }
  }

  private void validateSchema(
      final LogicalSchema expected,
      final LogicalSchema actual,
      final Optional<KsqlHostInfoEntity> sourceHost) {
    if (!actual.equals(expected)) {
      throw new KsqlException(
          String.format("Schemas %s from host %s differs from schema %s",
              actual, sourceHost .map(KsqlHostInfoEntity::getHost).orElse("unknown"), expected));
    }
  }




}
