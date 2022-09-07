package io.confluent.ksql.test;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.DefaultKsqlParser;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.test.QueryTranslationTest.QttTestFile;
import io.confluent.ksql.test.loader.JsonTestLoader;
import io.confluent.ksql.test.model.SourceNode;
import io.confluent.ksql.test.model.TestHeader;
import io.confluent.ksql.test.tools.Record;
import io.confluent.ksql.test.tools.TestCase;
import io.confluent.ksql.util.BytesUtils;
import io.confluent.ksql.util.BytesUtils.Encoding;
import io.confluent.ksql.util.KsqlConstants;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Windowed;

public class Translate {

  private static KsqlParser KSQL_PARSER = new DefaultKsqlParser();

  public static void main(final String[] args) throws IOException {
    translateFile2("query-validation-tests/bin14");
  }

  private static void translateFile2(final String path) throws IOException {
    final JsonTestLoader loader = JsonTestLoader.of(Paths.get(path), QttTestFile.class);
    final Map<String, String> translated = new HashMap<>();
    loader.load().forEach(rawTest -> {
      final TestCase test = (TestCase) rawTest;
      if (test.getPostConditions().getSourceNode().getTopics().isPresent()) {
        System.out.println("Cannot translate post-condition topic: " + test.getOriginalFileName().toString() + " " + test.getName());
        return;
      }

      if (test.getDeclaredTopics().size() > 0) {
        System.out.println("Cannot declare topics without statements: " + test.getOriginalFileName().toString() + " " + test.getName());
        return;
      }

      String text = "--@test: " + test.getName() + "\n";
      if (test.expectedExceptionNode.isPresent()) {
        text += "--@expected.error: " + test.expectedExceptionNode.get().getType() + "\n";
        text += "--@expected.message: " + test.expectedExceptionNode.get().getMessage() + "\n";
      }
      text += propertiesToSet(test.properties());
      text += String.join("\n", test.statements());

      if (!test.expectedExceptionNode.isPresent()) {
        final StatementRunner runner = new StatementRunner();
        runner.runStatements(test.statements().stream().map(statement -> KSQL_PARSER.parse(statement).get(0)).collect(Collectors.toList()));
        final MetaStore metaStore = runner.getMetaStore();
        runner.close();

        final Map<String, SourceName> topicToDataSource = new HashMap<>();
        for (final Entry e : metaStore.getAllDataSources().entrySet()) {
          topicToDataSource.put(((DataSource) e.getValue()).getKafkaTopicName(), (SourceName) e.getKey());
        }

        text += "\n" + recordsToCommands(test.getInputRecords(), metaStore, topicToDataSource, "INSERT INTO");
        text += "\n" + recordsToCommands(test.getOutputRecords(), metaStore, topicToDataSource, "ASSERT VALUES");

        text += "\n";

        for (SourceNode sourceNode : test.getPostConditions().getSourceNode().getSources()) {
          if (!sourceNode.getSchema().isPresent()) {
            System.out.println("ASSERT SOURCE with no schema provided: " + test.getOriginalFileName().toString() + " " + test.getName());
            continue;
          }
          String assertStatement = "ASSERT " + sourceNode.getType() + " " + sourceNode.getName() + " (" + sourceNode.getSchema().get() + ") WITH (KAFKA_TOPIC='" + metaStore.getSource(SourceName.of(sourceNode.getName())).getKafkaTopicName() + "');\n";
          if (sourceNode.getKeyFormat().isPresent() || sourceNode.getValueFormat().isPresent()) {
            System.out.println("Don't forget to add in the key and value format! " + test.getOriginalFileName().toString() + " " + test.getName());
          }
          text += assertStatement;
        }
      }

      text += "\n";

      System.out.println(test.getName());

      final String newFileName = test.getTestLocation().getTestPath().toString().replace(".json", ".sql").replace(path, "sql-tests/query-validation-tests/new");
      if (translated.containsKey(newFileName)) {
        translated.put(newFileName, translated.get(newFileName) + text);
      } else {
        translated.put(newFileName, text);
      }
    });

    for (final Entry<String, String> e : translated.entrySet()) {
      final File myObj = new File(e.getKey());
      if (myObj.createNewFile()) {
        FileWriter myWriter = new FileWriter(e.getKey());
        myWriter.write(e.getValue());
        myWriter.close();
      }
    }
  }

  private static String propertiesToSet(final Map<String, Object> properties) {
    return properties.entrySet()
        .stream()
        .map(entry -> String.format("SET '%s' = '%s';", entry.getKey(), entry.getValue().toString()))
        .collect(Collectors.joining("\n"));
  }

  private static String recordsToCommands(
      final List<Record> records,
      final MetaStore metaStore,
      final Map<String, SourceName> topicToDataSource,
      final String command
  ) {
    return records.stream().map(record-> {
      final SourceName sourceName = topicToDataSource.get(record.getTopicName());
      LogicalSchema schema = metaStore.getSource(sourceName).getSchema();
      final List<String> columns = new ArrayList<>();
      final List<String> values = new ArrayList<>();

      if (record.key() != null) {
        if (schema.key().size() > 1) {
          final Object extractedKey;
          if (record.key() instanceof Windowed) {
            extractedKey = ((Windowed<?>) record.key()).key();
          } else {
            extractedKey = record.key();
          }
          if (extractedKey instanceof Map) {
            for (final Object columnName : ((Map) extractedKey).keySet()) {
              columns.add((String) columnName);
              values.add(objectToString(
                  ((Map<?, ?>) extractedKey).get(columnName),
                  schema.findColumn(ColumnName.of((String) columnName)).isPresent()
                      ? schema.findColumn(ColumnName.of((String) columnName)).get().type()
                      : schema.findColumn(ColumnName.of(((String) columnName).toUpperCase())).get().type()
              ));
            }
          } else {
            columns.addAll(schema.key().stream().map(col -> col.name().text()).collect(Collectors.toList()));
            final String[] rawValues = ((String) extractedKey).split(",");
            for (int i = 0; i < schema.key().size(); i++) {
              values.add(objectToString(rawValues[i], schema.key().get(i).type()));
            }
          }
        } else if (schema.key().size() == 1) {
          columns.add(schema.key().get(0).name().text());
          values.add(objectToString(record.key(), schema.key().get(0).type()));
        } else {
          throw new IllegalStateException("no key columns");
        }
      }

      if (record.value() != null) {
        if (record.value() instanceof Map) {
          for (final Object columnName : ((Map) record.value()).keySet()) {
            columns.add((String) columnName);
            values.add(objectToString(
                ((Map<?, ?>) record.value()).get(columnName),
                schema.findColumn(ColumnName.of((String) columnName)).isPresent()
                    ? schema.findColumn(ColumnName.of((String) columnName)).get().type()
                    : schema.findColumn(ColumnName.of(((String) columnName).toUpperCase())).get().type()
            ));
          }
        } else {
          columns.addAll(schema.value().stream().map(col -> col.name().text()).collect(Collectors.toList()));
          if (record.value() instanceof String) {
            final String[] rawValues = ((String) record.value()).split(",");
            for (int i = 0; i < schema.value().size(); i++) {
              if (i >= rawValues.length) {
                values.add("NULL");
              } else {
                values.add(objectToString(rawValues[i], schema.value().get(i).type()));
              }
            }
          } else {
            values.add(record.value().toString());
          }
        }
      }

      if (record.timestamp().isPresent()) {
        columns.add(SystemColumns.ROWTIME_NAME.text());
        values.add(record.timestamp().get().toString());
      }

      if (record.getWindow() != null) {
        columns.add(SystemColumns.WINDOWSTART_NAME.text());
        columns.add(SystemColumns.WINDOWEND_NAME.text());
        values.add(Long.toString(record.getWindow().start));
        values.add(Long.toString(record.getWindow().end));
      }

      if (record.headers().isPresent()) {
        for (int i = 0; i < schema.headers().size(); i++) {
          columns.add(schema.headers().get(i).name().toString());
          if (schema.headers().get(i).headerKey().isPresent()) {
            final String headerKey = schema.headers().get(i).headerKey().get();
            final List<TestHeader> headers = record.headers().get();
            final Optional<TestHeader> header = headers.stream().filter(h -> h.key().equals(headerKey)).findFirst();
            values.add(header.isPresent() ? BytesUtils.encode(header.get().value(), Encoding.BASE64) : "NULL");
          } else {
            values.add(
                objectToString(
                    record.headers().get().stream().map(testHeader ->
                        ImmutableMap.of("KEY", testHeader.key(), "VALUE", testHeader.value())).collect(Collectors.toList()),
                    SystemColumns.HEADERS_TYPE)
            );
          }
        }
      }

      return String.format(
          "%s %s (%s) VALUES (%s);",
          command,
          sourceName,
          String.join(", ", columns),
          String.join(", ", values)
      );
    }).collect(Collectors.joining("\n"));
  }

  private static String objectToString(final Object obj, final SqlType type) {
    if (obj == null) {
      return "NULL";
    }

    if (obj instanceof Windowed) {
      return objectToString(((Windowed<?>) obj).key(), type);
    }
    switch (type.baseType()) {
      case BOOLEAN:
      case INTEGER:
      case DOUBLE:
      case BIGINT:
      case DECIMAL:
        return obj.toString();
      case BYTES:
        if (obj instanceof byte[]) {
          return "'" + BytesUtils.encode((byte[]) obj, Encoding.BASE64) + "'";
        } else {
          return "'" + obj.toString() + "'";
        }
      case STRING:
        return "'" + obj + "'";
      case ARRAY:
        final List<String> listElements = new ArrayList<>();
        for (final Object element : (List) obj) {
          listElements.add(objectToString(element, ((SqlArray) type).getItemType()));
        }
        return "ARRAY[" + String.join(", ", listElements) + "]";
      case STRUCT:
        final List<String> structElements = new ArrayList<>();
        for (final Entry<String, Object> e : ((Map<String, Object>) obj).entrySet()) {
          final SqlType entryType = ((SqlStruct) type).field(e.getKey()).isPresent()
              ? ((SqlStruct) type).field(e.getKey()).get().type()
              : ((SqlStruct) type).field(e.getKey().toUpperCase()).get().type();
          structElements.add(e.getKey() + ":=" + objectToString(e.getValue(), entryType));
        }
        return "STRUCT(" + String.join(", ", structElements) + ")";
      case MAP:
        final List<String> mapElements = new ArrayList<>();
        for (final Entry e : ((Map<Object, Object>) obj).entrySet()) {
          mapElements.add(objectToString(e.getKey(), ((SqlMap) type).getKeyType())
              + ":="
              + objectToString(e.getValue(), ((SqlMap) type).getValueType())
          );
        }
        return "MAP(" + String.join(", ", mapElements) + ")";
      case DATE:
        return "'" + LocalDate.ofEpochDay(Long.valueOf(obj.toString())).toString() + "'";
      case TIME:
        return "'" + LocalTime.ofSecondOfDay(Long.valueOf(obj.toString()) / 1000).toString() + "'";
      case TIMESTAMP:
        return "'" + DateTimeFormatter
            .ofPattern(KsqlConstants.DATE_TIME_PATTERN)
            .withZone(ZoneId.of("Z"))
            .format(Instant.ofEpochMilli(Long.valueOf(obj.toString()))) + "'";
      default:
        throw new IllegalStateException("type is " + type.baseType());
    }
  }
}
