/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.serde.connect;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.util.DecimalUtil;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

public abstract class ConnectFormat implements Format {

  private final Function<Schema, Schema> toKsqlTransformer;

  public ConnectFormat() {
    this(new ConnectSchemaTranslator()::toKsqlSchema);
  }

  @VisibleForTesting
  ConnectFormat(final Function<Schema, Schema> toKsqlTransformer) {
    this.toKsqlTransformer = Objects.requireNonNull(toKsqlTransformer, "toKsqlTransformer");
  }

  @Override
  public boolean supportsSchemaInference() {
    return true;
  }

  @Override
  public List<SimpleColumn> toColumns(final ParsedSchema schema) {
    final Schema connectSchema = toKsqlTransformer.apply(toConnectSchema(schema));

    return connectSchema.fields().stream()
        .map(ConnectFormat::toColumn)
        .collect(Collectors.toList());
  }

  public ParsedSchema toParsedSchema(final List<SimpleColumn> columns) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    columns.forEach(col -> schemaBuilder.field(
        col.ref().name(),
        SchemaConverters.sqlToConnectConverter().toConnectSchema(col.type()))
    );

    final Schema connectSchema = ensureNamed(schemaBuilder.build());
    return fromConnectSchema(connectSchema);
  }

  protected abstract Schema toConnectSchema(ParsedSchema schema);

  protected abstract ParsedSchema fromConnectSchema(Schema schema);

  private static SimpleColumn toColumn(final Field field) {
    final ColumnName name = ColumnName.of(field.name());
    final SqlType type = SchemaConverters.connectToSqlConverter().toSqlType(field.schema());
    return new ConnectColumn(name, type);
  }

  private static Schema ensureNamed(final Schema schema) {
    final SchemaBuilder builder;
    switch (schema.type()) {
      case BYTES:
        DecimalUtil.requireDecimal(schema);
        builder = DecimalUtil.builder(schema);
        break;
      case ARRAY:
        builder = SchemaBuilder.array(ensureNamed(schema.valueSchema()));
        break;
      case MAP:
        builder = SchemaBuilder.map(
            Schema.STRING_SCHEMA,
            ensureNamed(schema.valueSchema())
        );
        break;
      case STRUCT:
        builder = SchemaBuilder.struct();
        if (schema.name() == null) {
          builder.name("TestSchema" + UUID.randomUUID().toString().replace("-", ""));
        } else {
          builder.name(schema.name());
        }
        for (final Field field : schema.fields()) {
          builder.field(field.name(), ensureNamed(field.schema()));
        }
        break;
      default:
        builder = SchemaBuilder.type(schema.type());
    }
    if (schema.isOptional()) {
      builder.optional();
    }
    return builder.build();
  }

  private static final class ConnectColumn implements SimpleColumn {

    private final ColumnName name;
    private final SqlType type;

    private ConnectColumn(
        final ColumnName name,
        final SqlType type
    ) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
    }

    @Override
    public ColumnName ref() {
      return name;
    }

    @Override
    public SqlType type() {
      return type;
    }
  }
}
