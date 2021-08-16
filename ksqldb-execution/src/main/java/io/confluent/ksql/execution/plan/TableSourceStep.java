package io.confluent.ksql.execution.plan;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Optional;

public abstract class TableSourceStep extends SourceStep<KTableHolder<?>>  {

  public TableSourceStep(ExecutionStepPropertiesV1 properties, String topicName,
      Formats formats,
      Optional<TimestampColumn> timestampColumn,
      LogicalSchema sourceSchema, int pseudoColumnVersion) {
    super(properties, topicName, formats, timestampColumn, sourceSchema, pseudoColumnVersion);
  }

  public void MaterializationInfo() {

  }
}
