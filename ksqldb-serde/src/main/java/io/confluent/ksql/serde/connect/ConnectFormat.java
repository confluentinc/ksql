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
import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.Format;
import io.confluent.ksql.serde.FormatInfo;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.jetbrains.annotations.NotNull;

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
    Schema connectSchema = toConnectSchema(schema);

    if (connectSchema.type() != Type.STRUCT) {
      if (!supportsFeature(SerdeFeature.UNWRAP_SINGLES)) {
        throw new KsqlException("Schema returned from schema registry is anonymous type, "
            + "but format " + name() + " does not support anonymous types. "
            + "schema: " + schema);
      }

      connectSchema = SerdeUtils.wrapSingle(connectSchema);
    }

    final Schema rowSchema = toKsqlTransformer.apply(connectSchema);

    return rowSchema.fields().stream()
        .map(ConnectFormat::toColumn)
        .collect(Collectors.toList());
  }

  public ParsedSchema toParsedSchema(
      final PersistenceSchema schema,
      final FormatInfo formatInfo
  ) {
    SerdeUtils.throwOnUnsupportedFeatures(schema.features(), supportedFeatures());

    final ConnectSchema outerSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    final ConnectSchema innerSchema = SerdeUtils
        .applySinglesUnwrapping(outerSchema, schema.features());

    return fromConnectSchema(innerSchema, formatInfo);
  }

  @Override
  public Serde<Struct> getSerde(
      final PersistenceSchema schema,
      final Map<String, String> formatProps,
      final KsqlConfig config,
      final Supplier<SchemaRegistryClient> srFactory
  ) {
    SerdeUtils.throwOnUnsupportedFeatures(schema.features(), supportedFeatures());

    final ConnectSchema outerSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    final ConnectSchema innerSchema = SerdeUtils
        .applySinglesUnwrapping(outerSchema, schema.features());

    final Class<?> targetType = SchemaConverters.connectToJavaTypeConverter()
        .toJavaType(innerSchema);

    if (schema.features().enabled(SerdeFeature.UNWRAP_SINGLES)) {
      return handleUnwrapping(innerSchema, outerSchema, formatProps, config, srFactory, targetType);
    }

    if (!targetType.equals(Struct.class)) {
      throw new IllegalArgumentException("Expected STRUCT, got " + targetType);
    }

    return getConnectSerde(innerSchema, formatProps, config, srFactory, Struct.class);
  }

  @NotNull
  private <T> Serde<Struct> handleUnwrapping(
      final ConnectSchema innerSchema,
      final ConnectSchema outerSchema,
      final Map<String, String> formatProps,
      final KsqlConfig config,
      final Supplier<SchemaRegistryClient> srFactory,
      final Class<T> targetType
  ) {
    final Serde<T> innerSerde =
        getConnectSerde(innerSchema, formatProps, config, srFactory, targetType);

    return Serdes.serdeFrom(
        SerdeUtils.unwrappedSerializer(outerSchema, innerSerde.serializer(), targetType),
        SerdeUtils.unwrappedDeserializer(outerSchema, innerSerde.deserializer(), targetType)
    );
  }

  protected abstract <T> Serde<T> getConnectSerde(
      ConnectSchema connectSchema,
      Map<String, String> formatProps,
      KsqlConfig config,
      Supplier<SchemaRegistryClient> srFactory,
      Class<T> targetType
  );

  protected abstract Schema toConnectSchema(ParsedSchema schema);

  protected abstract ParsedSchema fromConnectSchema(Schema schema, FormatInfo formatInfo);

  private static SimpleColumn toColumn(final Field field) {
    final ColumnName name = ColumnName.of(field.name());
    final SqlType type = SchemaConverters.connectToSqlConverter().toSqlType(field.schema());
    return new ConnectColumn(name, type);
  }

  @Immutable
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
