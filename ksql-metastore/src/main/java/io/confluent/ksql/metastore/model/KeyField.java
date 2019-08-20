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

package io.confluent.ksql.metastore.model;

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * Pojo that holds the details of a source's key field.
 *
 * <p>In KSQL versions 5.2.x and earlier the key field of sources stored in the metastore was not
 * always accurate. The inaccurate key field is used in downstream queries.
 *
 * <p>To maintain backwards compatibility for persistent queries started on these earlier it is
 * unfortunately necessary to track both the corrected and legacy key fields of sources, and to use
 * these when building queries, resolving to the correct key depending on the version of KSQL the
 * query was started on.
 *
 * <p>Downstream queries started on earlier versions of KSQL should use the legacy key field. This
 * ensures the logical and topology remain backwards compatible
 *
 * <p>Downstream queries started on later versions of KSQL should use the corrected key field. This
 * allows these later queries to benefit from the improved logical.
 *
 * <p>This Pojo holds both the legacy and latest key field details. The legacy field is a complete
 * {@link Field}, where as the latest is just the key field name, which can be looked up in the
 * associated schema.
 *
 * @see <a href="https://github.com/confluentinc/ksql/issues/2636">Github issue 2636</a>
 */
@Immutable
public final class KeyField {

  private static final KeyField NONE = KeyField.of(Optional.empty(), Optional.empty());

  private final Optional<String> keyField;
  private final Optional<LegacyField> legacyKeyField;

  public static KeyField none() {
    return NONE;
  }

  public static KeyField of(
      final String keyField,
      final Field legacyKeyField
  ) {
    final LegacyField legacy = LegacyField.of(legacyKeyField.fullName(), legacyKeyField.type());
    return new KeyField(Optional.of(keyField), Optional.of(legacy));
  }

  public static KeyField of(
      final String keyField,
      final LegacyField legacyKeyField
  ) {
    return new KeyField(Optional.of(keyField), Optional.of(legacyKeyField));
  }

  public static KeyField of(
      final Optional<String> keyField,
      final Optional<LegacyField> legacyKeyField
  ) {
    return new KeyField(keyField, legacyKeyField);
  }

  private KeyField(final Optional<String> keyField, final Optional<LegacyField> legacyKeyField) {
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.legacyKeyField = Objects.requireNonNull(legacyKeyField, "legacyKeyField");
  }

  /**
   * Validate the new key field, if set, is contained within the supplied {@code schema}.
   *
   * @param schema the associated schema that the key should be present in.
   * @return self, to allow fluid syntax.
   * @throws IllegalArgumentException if the key is not within the supplied schema.
   */
  public KeyField validateKeyExistsIn(final LogicalSchema schema) {
    resolveLatest(schema);
    return this;
  }

  public Optional<String> name() {
    return keyField;
  }

  public Optional<LegacyField> legacy() {
    return legacyKeyField;
  }

  /**
   * Resolve this {@code KeyField} to the specific key {@code Field} to use.
   *
   * <p>The method inspects the supplied {@code ksqlConfig} to determine if the new or legacy
   * key field should be returned.
   *
   * <p>The new key field is obtained from the supplied {@code schema} using the instance's
   * {@code keyField} field as the field name.
   *
   * <p>The legacy key field is obtained from the instance's {@code legacyKeyField}.
   *
   * @param schema the schema to use when resolving new key fields.
   * @param ksqlConfig the config to use to determine if new or legacy key fields are required.
   * @return the resolved key field, or {@link Optional#empty()} if no key field is set.
   * @throws IllegalArgumentException if new key field is required but not available in the schema.
   */
  public Optional<Field> resolve(final LogicalSchema schema, final KsqlConfig ksqlConfig) {
    if (shouldUseLegacy(ksqlConfig)) {
      return legacyKeyField
          .map(f -> Field.of(f.name, f.type));
    }

    return resolveLatest(schema);
  }

  /**
   * Resolve this {@code KeyField} to the specific key field name to use.
   *
   * <p>The method inspects the supplied {@code ksqlConfig} to determine if the new or legacy
   * key field name should be returned.
   *
   * <p>The new key field name is obtained from the {@code keyField} field of the instance.
   *
   * <p>The legacy key field name is obtained from the name of the {@code legacyKeyField} field of
   * the instance.
   *
   * @param ksqlConfig the config to use to determine if new or legacy key fields are required.
   * @return the resolved key field name, or {@link Optional#empty()} if no key field is set.
   */
  public Optional<String> resolveName(final KsqlConfig ksqlConfig) {
    if (shouldUseLegacy(ksqlConfig)) {
      return legacyKeyField.map(LegacyField::name);
    }

    return keyField;
  }

  /**
   * Resolve the _latest_ keyfield in the supplied {@code schema}.
   *
   * <p>Note: this method ignores the legacy key field.
   *
   * @param schema the schema to find the key field in.
   * @return the key field, if one is present, or else {@code empty}.
   * @throws IllegalArgumentException is the key field is not in the supplied schema.
   */
  public Optional<Field> resolveLatest(final LogicalSchema schema) {
    return keyField
        .map(fieldName -> schema.findValueField(fieldName)
            .orElseThrow(() -> new IllegalArgumentException(
                "Invalid key field, not found in schema: " + fieldName)));
  }

  /**
   * Build a new instance with the supplied new key field name. Legacy remains the same.
   *
   * @param newName the new name.
   * @return the new instance.
   */
  public KeyField withName(final Optional<String> newName) {
    return KeyField.of(newName, legacyKeyField);
  }

  public KeyField withName(final String newName) {
    return withName(Optional.of(newName));
  }

  /**
   * Build a new instance with the supplied legacy key field. Latest remains the same.
   *
   * @param legacy the new legacy field.
   * @return the new instance.
   */
  public KeyField withLegacy(final Optional<LegacyField> legacy) {
    return KeyField.of(keyField, legacy);
  }

  public KeyField withLegacy(final LegacyField legacy) {
    return withLegacy(Optional.of(legacy));
  }

  /**
   * Build a new instance with the supplied {@code alias} applied to both new and legacy fields.
   *
   * @param alias the field alias to apply.
   * @return the new instance.
   */
  public KeyField withAlias(final String alias) {
    return KeyField.of(
        keyField.map(fieldName -> SchemaUtil.buildAliasedFieldName(alias, fieldName)),
        legacyKeyField.map(field -> field.withSource(alias))
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final KeyField that = (KeyField) o;
    return Objects.equals(keyField, that.keyField)
        && Objects.equals(legacyKeyField, that.legacyKeyField);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyField, legacyKeyField);
  }

  @Override
  public String toString() {
    return "KeyField{"
        + "keyField='" + keyField + '\''
        + ", legacyKeyField=" + legacyKeyField
        + '}';
  }

  private static boolean shouldUseLegacy(final KsqlConfig ksqlConfig) {
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD);
  }

  @Immutable
  public static final class LegacyField {

    public final String name;
    public final SqlType type;

    /**
     * This is used to replicate legacy logic to ensure unnecessary repartition steps are maintained
     * going forward to ensure compatibility for existing queries.
     */
    private final boolean notInSchema;

    public static LegacyField of(final String name, final SqlType type) {
      return new LegacyField(name, type, false);
    }

    public static LegacyField notInSchema(final String name, final SqlType type) {
      return new LegacyField(name, type, true);
    }

    private LegacyField(final String name, final SqlType type, final boolean notInSchema) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.notInSchema = notInSchema;
    }

    public String name() {
      return name;
    }

    public SqlType type() {
      return type;
    }

    public boolean isNotInSchema() {
      return notInSchema;
    }

    public LegacyField withSource(final String source) {
      return new LegacyField(
          SchemaUtil.buildAliasedFieldName(source, name),
          type,
          notInSchema
      );
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final LegacyField that = (LegacyField) o;
      return notInSchema == that.notInSchema
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, type, notInSchema);
    }

    @Override
    public String toString() {
      return "LegacyField{"
          + "name='" + name + '\''
          + ", type=" + type
          + ", notInSchema=" + notInSchema
          + '}';
    }
  }
}
