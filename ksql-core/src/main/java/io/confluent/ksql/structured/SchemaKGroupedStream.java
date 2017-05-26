/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.structured;

import io.confluent.ksql.parser.tree.HoppingWindowExpression;
import io.confluent.ksql.parser.tree.TumblingWindowExpression;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KSQLException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.List;

public class SchemaKGroupedStream {

  final Schema schema;
  final KGroupedStream kGroupedStream;
  final Field keyField;
  final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  final List<SchemaKStream> sourceSchemaKStreams;

  public SchemaKGroupedStream(final Schema schema, final KGroupedStream kGroupedStream,
                              final Field keyField,
                              final List<SchemaKStream> sourceSchemaKStreams) {
    this.schema = schema;
    this.kGroupedStream = kGroupedStream;
    this.keyField = keyField;
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.sourceSchemaKStreams = sourceSchemaKStreams;
  }

  public SchemaKTable aggregate(final Initializer initializer,
                                final Aggregator aggregator,
                                final WindowExpression windowExpression,
                                final Serde<GenericRow> topicValueSerDe,
                                final String storeName) {
    KTable<Windowed<String>, GenericRow> aggKtable;
    if (windowExpression != null) {
      if (windowExpression.getKsqlWindowExpression() instanceof TumblingWindowExpression) {
        TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression)
            windowExpression.getKsqlWindowExpression();
        aggKtable =
            kGroupedStream.aggregate(initializer, aggregator, TimeWindows.of(getWindowUnitInMillisecond(tumblingWindowExpression
                                                                         .getSize(),
                                                                     tumblingWindowExpression.getSizeUnit())), topicValueSerDe, storeName);
      } else if (windowExpression.getKsqlWindowExpression() instanceof HoppingWindowExpression) {
        HoppingWindowExpression hoppingWindowExpression = (HoppingWindowExpression) windowExpression.getKsqlWindowExpression();
        aggKtable =
            kGroupedStream.aggregate(initializer, aggregator, TimeWindows.of(getWindowUnitInMillisecond(hoppingWindowExpression.getSize(),
                                                                                                        hoppingWindowExpression.getSizeUnit()))
                                         .advanceBy(getWindowUnitInMillisecond(hoppingWindowExpression.getAdvanceBy(),
                                                                               hoppingWindowExpression.getAdvanceByUnit())),
                                     topicValueSerDe, storeName);
      } else {
        throw new KSQLException("Could not set the window expression for aggregate.");
      }
    } else {
      aggKtable =
          kGroupedStream.aggregate(initializer, aggregator, topicValueSerDe, storeName);
    }
    return new SchemaKTable(schema, aggKtable, keyField, sourceSchemaKStreams, true);
  }

  private long getWindowUnitInMillisecond(long value, WindowExpression.WindowUnit windowUnit) {

    switch (windowUnit) {
      case DAY:
        return value * 24 * 60 * 60 * 1000;
      case HOUR:
        return value * 60 * 60 * 1000;
      case MINUTE:
        return value * 60 * 1000;
      case SECOND:
        return value * 1000;
      case MILLISECOND:
        return value;
    }
    return -1;
  }

}
