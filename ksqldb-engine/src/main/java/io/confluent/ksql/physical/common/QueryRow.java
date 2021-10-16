package io.confluent.ksql.physical.common;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.Window;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.List;
import java.util.Optional;

public interface QueryRow {
  LogicalSchema schema();

  long rowTime();

  GenericKey key();

  Optional<Window> window();

  GenericRow value();

  Optional<List<Long>> getStartOffsets();

  Optional<List<Long>> getOffsets();
}
