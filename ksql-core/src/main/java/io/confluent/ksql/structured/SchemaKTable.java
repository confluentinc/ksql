package io.confluent.ksql.structured;

import io.confluent.ksql.physical.GenericRow;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;

public class SchemaKTable extends SchemaStream {

    final KTable kTable;

    public SchemaKTable(Schema schema, KTable kTable, Field keyField) {
        super(schema, null, keyField);
        this.kTable = kTable;
    }

    public KTable getkTable() {
        return kTable;
    }

}
