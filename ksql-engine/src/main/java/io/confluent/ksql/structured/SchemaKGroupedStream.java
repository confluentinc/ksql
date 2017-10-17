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

package io.confluent.ksql.structured;

import io.confluent.ksql.function.udaf.KudafAggregator;
import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.SessionWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.List;

public class SchemaKGroupedStream {

  private final Schema schema;
  private final KGroupedStream kgroupedStream;
  private final Field keyField;
  private final List<SchemaKStream> sourceSchemaKStreams;

  SchemaKGroupedStream(final Schema schema, final KGroupedStream kgroupedStream,
                       final Field keyField,
                       final List<SchemaKStream> sourceSchemaKStreams) {
    this.schema = schema;
    this.kgroupedStream = kgroupedStream;
    this.keyField = keyField;
    this.sourceSchemaKStreams = sourceSchemaKStreams;
  }

  @SuppressWarnings("unchecked")
  public SchemaKTable aggregate(final Initializer initializer,
                                final KudafAggregator aggregator,
                                final WindowExpression windowExpression,
                                final Serde<GenericRow> topicValueSerDe,
                                final String storeName) {
    boolean isWindowed = false;
    KTable<Windowed<String>, GenericRow> aggKtable;
    if (windowExpression != null) {
      isWindowed = true;
      final Materialized<String, GenericRow, WindowStore<Bytes, byte[]>> materialized
          = Materialized.<String, GenericRow, WindowStore<Bytes, byte[]>>as(storeName)
          .withValueSerde(topicValueSerDe);
      if (windowExpression.getKsqlWindowExpression() instanceof TumblingWindowExpression) {
        TumblingWindowExpression tumblingWindowExpression =
            (TumblingWindowExpression) windowExpression.getKsqlWindowExpression();
        aggKtable =
            kgroupedStream.windowedBy(TimeWindows.of(tumblingWindowExpression.getSizeUnit().toMillis(tumblingWindowExpression.getSize())))
                .aggregate(initializer, aggregator,
                    materialized);
      } else if (windowExpression.getKsqlWindowExpression() instanceof HoppingWindowExpression) {
        HoppingWindowExpression hoppingWindowExpression =
            (HoppingWindowExpression) windowExpression.getKsqlWindowExpression();
        aggKtable =
            kgroupedStream.windowedBy(TimeWindows.of(
                hoppingWindowExpression.getSizeUnit().toMillis(hoppingWindowExpression.getSize()))
                .advanceBy(
                    hoppingWindowExpression.getAdvanceByUnit().toMillis(hoppingWindowExpression.getAdvanceBy())))
                .aggregate(initializer, aggregator, materialized);
      } else if (windowExpression.getKsqlWindowExpression() instanceof SessionWindowExpression) {
        SessionWindowExpression sessionWindowExpression =
            (SessionWindowExpression) windowExpression.getKsqlWindowExpression();
        aggKtable =
            kgroupedStream.windowedBy(SessionWindows.with(sessionWindowExpression.getSizeUnit().toMillis(sessionWindowExpression.getGap())))
                .aggregate(initializer, aggregator, aggregator.getMerger(),
                           Materialized.<String, GenericRow, SessionStore<Bytes, byte[]>>as(storeName).withValueSerde(topicValueSerDe));
      } else {
        throw new KsqlException("Could not set the window expression for aggregate.");
      }
    } else {
      aggKtable =
          kgroupedStream.aggregate(initializer, aggregator, Materialized.with(null, topicValueSerDe));
    }
    return new SchemaKTable(schema, aggKtable, keyField, sourceSchemaKStreams, isWindowed,
                            SchemaKStream.Type.AGGREGATE);
  }

}
