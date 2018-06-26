package io.confluent.ksql.serde.connect;

import io.confluent.ksql.GenericRow;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public interface DataTranslator {
  GenericRow toKsqlRow(final Schema connectSchema, final Object connectData);

  Struct toConnectRow(final GenericRow genericRow);
}
