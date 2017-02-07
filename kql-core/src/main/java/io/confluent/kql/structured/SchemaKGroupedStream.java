/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.structured;

import io.confluent.kql.physical.GenericRow;
import io.confluent.kql.util.GenericRowValueTypeEnforcer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;

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
                                final Serde<GenericRow> topicValueSerDe,
                                final String storeName) {
    KTable
        aggKtable =
        kGroupedStream.aggregate(initializer, aggregator, topicValueSerDe, storeName);
    return new SchemaKTable(schema, aggKtable, keyField, sourceSchemaKStreams);
  }
}
