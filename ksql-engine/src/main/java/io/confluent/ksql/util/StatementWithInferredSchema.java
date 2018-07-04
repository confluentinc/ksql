package io.confluent.ksql.util;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.parser.SqlFormatter;
import io.confluent.ksql.parser.tree.AbstractStreamCreateStatement;
import io.confluent.ksql.parser.tree.Statement;

import java.util.Map;

public class StatementWithInferredSchema {
  private final Statement statement;
  private final String statementText;

  private StatementWithInferredSchema(final Statement statement, final String statementText) {
    this.statement = statement;
    this.statementText = statementText;
  }

  public Statement getStatement() {
    return statement;
  }

  public String getStatementText() {
    return statementText;
  }

  public static StatementWithInferredSchema forStatement(
      final Statement statement,
      final String statementText,
      final Map<String, Object> properties,
      final SchemaRegistryClient schemaRegistryClient) {
    if (statement instanceof AbstractStreamCreateStatement) {
      final Statement statementWithSchema
          = AvroUtil.checkAndSetAvroSchema(
              (AbstractStreamCreateStatement) statement, properties, schemaRegistryClient);
      if (statementWithSchema != statement) {
        return new StatementWithInferredSchema(
            statementWithSchema,
            SqlFormatter.formatSql(statementWithSchema)
        );
      }
    }
    return new StatementWithInferredSchema(statement, statementText);
  }
}
