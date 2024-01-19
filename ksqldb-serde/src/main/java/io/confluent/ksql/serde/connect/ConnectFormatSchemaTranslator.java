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

import static java.util.Objects.requireNonNull;

import com.google.errorprone.annotations.Immutable;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.properties.with.CommonCreateConfigs;
import io.confluent.ksql.schema.ksql.PersistenceSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SimpleColumn;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.serde.SchemaTranslator;
import io.confluent.ksql.serde.SerdeFeature;
import io.confluent.ksql.serde.SerdeFeatures;
import io.confluent.ksql.serde.SerdeUtils;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;

class ConnectFormatSchemaTranslator implements SchemaTranslator {

  private final ConnectFormat format;
  private final ConnectSchemaTranslator connectSrTranslator;
  private final ConnectKsqlSchemaTranslator connectKsqlTranslator;

  ConnectFormatSchemaTranslator(
      final ConnectFormat format,
      final Map<String, String> formatProps,
      final ConnectKsqlSchemaTranslator connectKsqlSchemaTranslator
  ) {
    this.format = requireNonNull(format, "format");
    this.connectSrTranslator = requireNonNull(format.getConnectSchemaTranslator(formatProps));
    this.connectKsqlTranslator = requireNonNull(connectKsqlSchemaTranslator);
  }

  @Override
  public String name() {
    return connectSrTranslator.name();
  }

  @Override
  public void configure(final Map<String, ?> configs) {
    connectSrTranslator.configure(configs);
  }

  @Override
  public List<SimpleColumn> toColumns(
      final ParsedSchema schema,
      final SerdeFeatures serdeFeatures,
      final boolean isKey) {
    SerdeUtils.throwOnUnsupportedFeatures(serdeFeatures, format.supportedFeatures());

    Schema connectSchema = connectSrTranslator.toConnectSchema(schema);

    if (serdeFeatures.enabled(SerdeFeature.UNWRAP_SINGLES)) {
      connectSchema = SerdeUtils.wrapSingle(connectSchema, isKey);
    }

    if (connectSchema.type() != Type.STRUCT) {
      if (isKey) {
        throw new IllegalStateException("Key schemas are always unwrapped.");
      }

      throw new KsqlException("Schema returned from schema registry is anonymous type. "
          + "To use this schema with ksqlDB, set '" + CommonCreateConfigs.WRAP_SINGLE_VALUE
          + "=false' in the WITH clause properties.");
    }

    final Schema rowSchema = connectKsqlTranslator.toKsqlSchema(connectSchema);

    return rowSchema.fields().stream()
        .map(ConnectFormatSchemaTranslator::toColumn)
        .collect(Collectors.toList());
  }

  @Override
  public ParsedSchema toParsedSchema(final PersistenceSchema schema) {
    SerdeUtils.throwOnUnsupportedFeatures(schema.features(), format.supportedFeatures());

    final ConnectSchema outerSchema = ConnectSchemas.columnsToConnectSchema(schema.columns());
    final ConnectSchema innerSchema = SerdeUtils
        .applySinglesUnwrapping(outerSchema, schema.features());

    return connectSrTranslator.fromConnectSchema(innerSchema);
  }

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
      this.name = requireNonNull(name, "name");
      this.type = requireNonNull(type, "type");
    }

    @Override
    public ColumnName name() {
      return name;
    }

    @Override
    public SqlType type() {
      return type;
    }

    @Override
    public String toString() {
      return toString(FormatOptions.none());
    }

    public String toString(final FormatOptions formatOptions) {
      final String fmtType = type.toString(formatOptions);

      return name.toString(formatOptions) + " " + fmtType;
    }
  }
}
