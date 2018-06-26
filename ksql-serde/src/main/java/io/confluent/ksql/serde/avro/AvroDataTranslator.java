package io.confluent.ksql.serde.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.serde.connect.ConnectDataTranslator;
import io.confluent.ksql.serde.connect.DataTranslator;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class AvroDataTranslator implements DataTranslator {
  private final DataTranslator innerTranslator;
  private final Schema ksqlSchema;
  private final Schema avroCompatibleSchema;

  public AvroDataTranslator(final Schema ksqlSchema) {
    this.ksqlSchema = ksqlSchema;
    this.avroCompatibleSchema = buildAvroCompatibleSchema(
        ksqlSchema,
        new TypeNameGenerator());
    this.innerTranslator = new ConnectDataTranslator(avroCompatibleSchema);
  }

  @Override
  public GenericRow toKsqlRow(Schema connectSchema, Object connectObject) {
    final GenericRow avroCompatibleRow = innerTranslator.toKsqlRow(connectSchema, connectObject);
    final List<Object> columns = new LinkedList<>();
    for (int i = 0; i < avroCompatibleRow.getColumns().size(); i++) {
      columns.add(
          replaceSchema(
              ksqlSchema.fields().get(i).schema(),
              avroCompatibleRow.getColumns().get(i)));
    }
    return new GenericRow(columns);
  }

  @Override
  public Struct toConnectRow(final GenericRow genericRow) {
    final List<Object> columns = new LinkedList<>();
    for (int i = 0; i < genericRow.getColumns().size(); i++) {
      columns.add(
          replaceSchema(
              avroCompatibleSchema.fields().get(i).schema(),
              genericRow.getColumns().get(i)));
    }
    return innerTranslator.toConnectRow(new GenericRow(columns));
  }

  private static class TypeNameGenerator {
    private static final String DELIMITER = "_";
    private static final String DEFAULT_SCHEMA_NAME_BASE = "KSQLDefaultSchemaName";

    static final String MAP_KEY_NAME = "MapKey";
    static final String MAP_VALUE_NAME = "MapValue";

    private Iterable<String> names;

    TypeNameGenerator() {
      this(ImmutableList.of(DEFAULT_SCHEMA_NAME_BASE));
    }

    private TypeNameGenerator(Iterable<String> names) {
      this.names = names;
    }

    TypeNameGenerator with(String name) {
      return new TypeNameGenerator(Iterables.concat(names, ImmutableList.of(name)));
    }

    public String name() {
      return String.join(DELIMITER, names);
    }
  }

  private String avroCompatibleFieldName(final Field field) {
    // Currently the only incompatible field names expected are fully qualified
    // column identifiers. Once quoted identifier support is introduced we will
    // need to implement something more generic here.
    return field.name().replace(".", "_");
  }

  private Schema buildAvroCompatibleSchema(final Schema schema,
                                           final TypeNameGenerator typeNameGenerator) {
    final SchemaBuilder schemaBuilder;
    switch (schema.type()) {
      default:
        return schema;
      case STRUCT:
        schemaBuilder = SchemaBuilder.struct();
        if (schema.name() == null) {
          schemaBuilder.name(typeNameGenerator.name());
        }
        for (final Field f : schema.fields()) {
          schemaBuilder.field(
              avroCompatibleFieldName(f),
              buildAvroCompatibleSchema(f.schema(), typeNameGenerator.with(f.name())));
        }
        break;
      case ARRAY:
        schemaBuilder = SchemaBuilder.array(
            buildAvroCompatibleSchema(schema.valueSchema(), typeNameGenerator));
        break;
      case MAP:
        schemaBuilder = SchemaBuilder.map(
            buildAvroCompatibleSchema(schema.keySchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_KEY_NAME)),
            buildAvroCompatibleSchema(schema.valueSchema(),
                typeNameGenerator.with(TypeNameGenerator.MAP_VALUE_NAME)));
        break;
    }
    if (schema.isOptional()) {
      schemaBuilder.optional();
    }
    return schemaBuilder.build();
  }

  @SuppressWarnings("unchecked")
  private Object replaceSchema(Schema schema, Object object) {
    if (object == null) {
      return null;
    }
    switch (schema.type()) {
      case ARRAY:
        return ((List) object).stream()
            .map(e -> replaceSchema(schema.valueSchema(), e))
            .collect(Collectors.toList());
      case MAP:
        return ((Map<Object, Object>) object).entrySet().stream()
            .collect(
                Collectors.toMap(
                    e -> replaceSchema(schema.keySchema(), e.getKey()),
                    e -> replaceSchema(schema.valueSchema(), e.getValue())
                )
            );

      case STRUCT:
        final Struct struct = new Struct(schema);
        schema.fields().forEach(
            f -> struct.put(
                f.name(),
                replaceSchema(f.schema(), ((Struct) object).get(f.name())))
        );
        return struct;
      default:
        return object;
    }
  }
}
