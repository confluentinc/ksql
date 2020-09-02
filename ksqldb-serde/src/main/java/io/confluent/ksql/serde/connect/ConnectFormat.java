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
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.serde.SerdeOptions;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * Base class for formats that internally leverage Connect's data model, i.e. it's {@link Schema}
 * type.
 *
 * <p>Subclasses need only worry about conversion between the `ParsedSchema` returned from the
 * Schema Registry and Connect's {@link Schema} type. This class handles the conversion between
 * Connect's {@link Schema} type and KSQL's {@link SimpleColumn}.
 */
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

  public ParsedSchema toParsedSchema(
      final List<? extends SimpleColumn> columns,
      final SerdeOptions serdeOptions,
      final FormatInfo formatInfo
  ) {
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct();
    columns.forEach(col -> schemaBuilder.field(
        col.name().text(),
        SchemaConverters.sqlToConnectConverter().toConnectSchema(col.type()))
    );

    final PersistenceSchema persistenceSchema =
        buildValuePhysical(schemaBuilder.build(), serdeOptions);

    return fromConnectSchema(persistenceSchema.serializedSchema(), formatInfo);
  }

  protected abstract Schema toConnectSchema(ParsedSchema schema);

  protected abstract ParsedSchema fromConnectSchema(Schema schema, FormatInfo formatInfo);

  private static PersistenceSchema buildValuePhysical(
      final Schema valueConnectSchema,
      final SerdeOptions serdeOptions
  ) {
    final boolean singleField = valueConnectSchema.fields().size() == 1;

    final boolean unwrapSingle = serdeOptions.valueWrapping()
        .map(option -> option == SerdeOption.UNWRAP_SINGLE_VALUES)
        .orElse(false);

    if (unwrapSingle && !singleField) {
      throw new KsqlException("'" + CommonCreateConfigs.WRAP_SINGLE_VALUE + "' "
          + "is only valid for single-field value schemas");
    }

    return PersistenceSchema.from((ConnectSchema) valueConnectSchema, unwrapSingle);
  }

  private static SimpleColumn toColumn(final Field field) {
    final ColumnName name = ColumnName.of(field.name());
    final SqlType type = SchemaConverters.connectToSqlConverter().toSqlType(field.schema());
    return new ConnectColumn(name, type);
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
    public ColumnName name() {
      return name;
    }

    @Override
    public SqlType type() {
      return type;
    }
  }
}
