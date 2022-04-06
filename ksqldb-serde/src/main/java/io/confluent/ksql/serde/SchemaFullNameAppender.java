/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.serde;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Collections;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Utility class to append a full schema name (name & namespace) to a Connect schema.
 */
public final class SchemaFullNameAppender {

  private SchemaFullNameAppender() {
  }

  /**
   * Appends the {@code schemaFullName} to a Connect {@code schema}. The schema full name is
   * a fully qualified name that may contain a schema namespace and a schema name; all separated
   * by `.`.
   * </p>
   * For example, {@code schemaFullName} may be `com.confluent.MySchemaName`.
   * </p>
   * @param schema The Connect schema to append the full schema name
   * @param schemaFullName The full name to be used in the Connect schema
   * @return A new Connect schema with the fully qualified schema name
   */
  public static Schema appendSchemaFullName(
      final Schema schema,
      final String schemaFullName
  ) {
    return buildCompatibleSchemaFullName(
        schema,
        new Context(Collections.singleton(schemaFullName), true)
    );
  }

  private static final class Context {
    private static final String DELIMITER = "_";

    static final String MAP_KEY_NAME = "MapKey";
    static final String MAP_VALUE_NAME = "MapValue";

    private final Iterable<String> names;
    private boolean root;

    private Context(
        final Iterable<String> names,
        final boolean root
    ) {
      this.names = requireNonNull(names, "names");
      this.root = root;
    }

    Context with(final String name) {
      return new Context(Iterables.concat(names, ImmutableList.of(name)), root);
    }

    public String name() {
      return String.join(DELIMITER, names);
    }
  }

  private static String compatibleFieldName(final Field field) {
    // Currently the only incompatible field names expected are fully qualified
    // column identifiers. Once quoted identifier support is introduced we will
    // need to implement something more generic here.
    return field.name().replace(".", "_");
  }

  private static Schema buildCompatibleSchemaFullName(
      final Schema schema,
      final Context context
  ) {
    final boolean notRoot = !context.root;
    context.root = false;

    final SchemaBuilder schemaBuilder;
    switch (schema.type()) {
      default:
        if (notRoot || !schema.isOptional()) {
          return schema;
        }

        schemaBuilder = new SchemaBuilder(schema.type())
            .name(schema.name());
        break;

      case STRUCT:
        schemaBuilder = buildCompatibleStruct(schema, context);
        break;

      case ARRAY:
        schemaBuilder = buildCompatibleArray(schema, context);
        break;

      case MAP:
        schemaBuilder = buildCompatibleMap(schema, context);
        break;
    }

    if (schema.parameters() != null) {
      schemaBuilder.parameters(schema.parameters());
    }

    if (schema.isOptional() && notRoot) {
      schemaBuilder.optional();
    }

    return schemaBuilder.build();
  }

  private static SchemaBuilder buildCompatibleMap(
      final Schema schema, final Context context
  ) {
    final Schema keySchema =
        buildCompatibleSchemaFullName(schema.keySchema(), context.with(Context.MAP_KEY_NAME));

    final Schema valueSchema =
        buildCompatibleSchemaFullName(schema.valueSchema(), context.with(Context.MAP_VALUE_NAME));

    final SchemaBuilder schemaBuilder = SchemaBuilder.map(
        keySchema,
        valueSchema
    );

    schemaBuilder.name(context.name());
    return schemaBuilder;
  }

  private static SchemaBuilder buildCompatibleArray(
      final Schema schema,
      final Context context
  ) {
    final Schema valueSchema = buildCompatibleSchemaFullName(schema.valueSchema(), context);

    return SchemaBuilder.array(valueSchema);
  }

  private static SchemaBuilder buildCompatibleStruct(
      final Schema schema,
      final Context context
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    if (schema.name() == null) {
      schemaBuilder.name(context.name());
    } else {
      schemaBuilder.name(schema.name());
    }

    for (final Field f : schema.fields()) {
      final String fieldName = compatibleFieldName(f);
      final Schema fieldSchema = buildCompatibleSchemaFullName(f.schema(), context.with(f.name()));

      schemaBuilder.field(fieldName, fieldSchema);
    }

    return schemaBuilder;
  }
}
