package io.confluent.ksql.parser;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class VariableSubstitutorTest {
  private static final KsqlParser KSQL_PARSER = new DefaultKsqlParser();

  @Test
  public void shouldNotSubstituteWithEscapedVariables() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("env", "qa");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        Pair.of("DEFINE topicName = 'topic_$${env}';",
            "DEFINE topicName = 'topic_${env}';"),
        Pair.of("DEFINE topicName = 'topic_$${${env}}';",
            "DEFINE topicName = 'topic_${qa}';")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldSupportRecursiveVariableReplacement() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("env", "qa");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // This case only shows that ${env} is replaced when inside a escaped variable reference
        Pair.of("DEFINE topicName = 'topic_$${${env}}';",
            "DEFINE topicName = 'topic_${qa}';")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldSubstituteVariableOnDefine() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("env", "qa");
      put("env_quoted", "\"qa\"");
      put("env_backQuoted", "`qa`");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        Pair.of("DEFINE topicName = 'topic_${env}';",
            "DEFINE topicName = 'topic_qa';"),
        Pair.of("DEFINE topicName = 'topic_${env_quoted}';",
            "DEFINE topicName = 'topic_\"qa\"';"),
        Pair.of("DEFINE topicName = 'topic_${env_backQuoted}';",
            "DEFINE topicName = 'topic_`qa`';")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldSubstituteVariableOnDescribe() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("identifier", "_id_");
      put("quotedIdentifier", "\"_id_\"");
      put("backQuotedIdentifier", "`_id_`");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // DESCRIBE
        Pair.of("DESCRIBE ${identifier};", "DESCRIBE _id_;"),
        Pair.of("DESCRIBE ${quotedIdentifier};", "DESCRIBE \"_id_\";"),
        Pair.of("DESCRIBE ${backQuotedIdentifier};", "DESCRIBE `_id_`;"),

        // DESCRIBE EXTENDED
        Pair.of("DESCRIBE ${identifier} EXTENDED;", "DESCRIBE _id_ EXTENDED;"),
        Pair.of("DESCRIBE ${quotedIdentifier} EXTENDED;", "DESCRIBE \"_id_\" EXTENDED;"),
        Pair.of("DESCRIBE ${backQuotedIdentifier} EXTENDED;", "DESCRIBE `_id_` EXTENDED;"),

        // DESCRIBE FUNCTION
        Pair.of("DESCRIBE FUNCTION ${identifier};", "DESCRIBE FUNCTION _id_;"),
        Pair.of("DESCRIBE FUNCTION ${quotedIdentifier};", "DESCRIBE FUNCTION \"_id_\";"),
        Pair.of("DESCRIBE FUNCTION ${backQuotedIdentifier};", "DESCRIBE FUNCTION `_id_`;"),

        // DESCRIBE CONNECTOR
        Pair.of("DESCRIBE CONNECTOR ${identifier};", "DESCRIBE CONNECTOR _id_;"),
        Pair.of("DESCRIBE CONNECTOR ${quotedIdentifier};", "DESCRIBE CONNECTOR \"_id_\";"),
        Pair.of("DESCRIBE CONNECTOR ${backQuotedIdentifier};", "DESCRIBE CONNECTOR `_id_`;")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldThrowOnDescribeWhenInvalidVariables() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      // Attempts to use a keyword
      put("identifier", "EXTENDED _id_");
      put("quotedIdentifier", "EXTENDED \"_id_\"");
      put("backQuotedIdentifier", "EXTENDED `_id_`");
      put("singleQuoteIdentifier", "EXTENDED '_id_'");

      // Attempts to use a keyword inside quotes
      put("quotedIdentifier2", "\"EXTENDED _id_\"");
      put("backQuotedIdentifier2", "`EXTENDED _id_`");
      put("singleQuoteIdentifier2", "'EXTENDED _id_'");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // DESCRIBE
        Pair.of("DESCRIBE ${identifier};", "Illegal argument"),
        Pair.of("DESCRIBE ${quotedIdentifier};", "Illegal argument"),
        Pair.of("DESCRIBE ${backQuotedIdentifier};", "Illegal argument"),
        Pair.of("DESCRIBE ${singleQuoteIdentifier};", "Illegal argument"),
        Pair.of("DESCRIBE ${quotedIdentifier2};", "Illegal argument"),
        Pair.of("DESCRIBE ${backQuotedIdentifier2};", "Illegal argument"),
        Pair.of("DESCRIBE ${singleQuoteIdentifier2};", "Illegal argument")
    );

    assertThrowOnInvalidVariables(statements, variablesMap);
  }

  @Test
  public void shouldSubstituteVariableOnInsert() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("identifier", "_id_");
      put("num", "1");
      put("dec", "0.32");
      put("bool", "false");
      put("str", "'john'");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // INSERT VALUES
        Pair.of("INSERT INTO ${identifier} VALUES (${num}, ${bool}, ${dec}, ${str});",
            "INSERT INTO _id_ VALUES (1, false, 0.32, 'john');")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldSubstituteVariableOnCreate() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("identifier", "_id_");
      put("quotedIdentifier", "\"_id_\"");
      put("backQuotedIdentifier", "`_id_`");
      put("topicName", "'name1'");
      put("replicas", "3");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // CREATE STREAM
        Pair.of("CREATE STREAM ${identifier} WITH (kafka_topic=${topicName}, replicas=${replicas});",
            "CREATE STREAM _id_ WITH (kafka_topic='name1', replicas=3);"),

        Pair.of("CREATE STREAM ${quotedIdentifier} WITH (kafka_topic=${topicName}, replicas=${replicas});",
            "CREATE STREAM \"_id_\" WITH (kafka_topic='name1', replicas=3);"),

        Pair.of("CREATE STREAM ${backQuotedIdentifier} WITH (kafka_topic=${topicName}, replicas=${replicas});",
            "CREATE STREAM `_id_` WITH (kafka_topic='name1', replicas=3);"),

        Pair.of("CREATE STREAM ${backQuotedIdentifier} WITH (kafka_topic=${topicName}, replicas=${replicas});",
            "CREATE STREAM `_id_` WITH (kafka_topic='name1', replicas=3);")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldSanitizeStatements() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("escapeQuote", "'t1', value_format='AVRO'");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // CREATE
        Pair.of("CREATE STREAM s1 WITH (kafka_topic=${escapeQuote});",
            "CREATE STREAM s1 WITH (kafka_topic='t1'', value_format=''AVRO');")
    );

    // When/Then
    assertReplacedStatements(statements, variablesMap);
  }

  @Test
  public void shouldThrowOnSQLInjection() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("injectSchema", "s1 (id, name)");
      put("injectValues", "1, 5");
      put("injectWhere", "s1 WHERE id = 1");
      put("injectExpression", "5 and id != 5");
    }}.build();

    final List<Pair<String, String>> statements = Arrays.asList(
        // INSERT
        Pair.of("INSERT INTO ${injectSchema} VALUES (1);",
            "Identifier names cannot start"),
        Pair.of("INSERT INTO s1 VALUES (${injectValues});",
            "Illegal argument"),

        // SELECT
        Pair.of("SELECT * FROM ${injectWhere};",
            "Identifier names cannot start"),
        Pair.of("SELECT * FROM s1 WHERE id = ${injectExpression};",
            "Illegal argument"),

        // CREATE
        Pair.of("CREATE STREAM ${injectSchema} WITH (kafka_topic='t1');",
            "Identifier names cannot start")
    );

    // When/Then
    assertThrowOnInvalidVariables(statements, variablesMap);
  }

  @Test
  public void shouldSubstituteVariablesInString() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("event", "birthday");
    }}.build();

    // When
    final String substituted = VariableSubstitutor.substitute("Happy ${event} to you!", variablesMap);

    // Then
    assertThat(substituted, equalTo("Happy birthday to you!"));
  }

  @Test
  public void shouldSubstituteVariablesInAssert() {
    // Given
    final Map<String, String> variablesMap = new ImmutableMap.Builder<String, String>() {{
      put("name", "foo");
    }}.build();

    // When
    final String substitutedString = VariableSubstitutor.substitute("ASSERT TOPIC '${name}';", variablesMap);
    final String substitutedIdentifier = VariableSubstitutor.substitute("ASSERT TOPIC ${name};", variablesMap);

    // Then
    assertThat(substitutedString, equalTo("ASSERT TOPIC 'foo';"));
    assertThat(substitutedIdentifier, equalTo("ASSERT TOPIC foo;"));
  }

  private void assertReplacedStatements(
      final List<Pair<String, String>> statements,
      final Map<String, String> variablesMap
  ) {
    for (Pair<String, String> stmt : statements) {
      KsqlParser.ParsedStatement sqlStatement = KSQL_PARSER.parse(stmt.getLeft()).get(0);
      String sqlReplaced = stmt.getRight();

      // When
      final String sqlResult = VariableSubstitutor.substitute(sqlStatement, variablesMap);

      // Then
      assertThat("Should replace: " + sqlStatement.getUnMaskedStatementText(), sqlResult, equalTo(sqlReplaced));
    }
  }

  private void assertThrowOnInvalidVariables(
      final List<Pair<String, String>> statements,
      final Map<String, String> variablesMap
  ) {
    for (Pair<String, String> stmt : statements) {
      KsqlParser.ParsedStatement sqlStatement = KSQL_PARSER.parse(stmt.getLeft()).get(0);
      String sqlError = stmt.getRight();

      // When
      final Exception e = assertThrows(
          Exception.class,
          () -> VariableSubstitutor.substitute(sqlStatement, variablesMap)
      );

      // Then
      assertThat("Should fail replace: " + sqlStatement.getUnMaskedStatementText(),
          e.getMessage(), containsString(sqlError));
    }
  }
}
