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
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

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
  private final Optional<Field> legacyKeyField;

  public static KeyField none() {
    return NONE;
  }

  public static KeyField of(final String keyField, final Field legacyKeyField) {
    return new KeyField(Optional.of(keyField), Optional.of(legacyKeyField));
  }

  public static KeyField of(final Optional<String> keyField, final Optional<Field> legacyKeyField) {
    return new KeyField(keyField, legacyKeyField);
  }

  private KeyField(final Optional<String> keyField, final Optional<Field> legacyKeyField) {
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
  public KeyField validateKeyExistsIn(final Schema schema) {
    resolveKey(schema);
    return this;
  }

  public Optional<String> name() {
    return keyField;
  }

  public Optional<Field> legacy() {
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
  public Optional<Field> resolve(final Schema schema, final KsqlConfig ksqlConfig) {
    if (shouldUseLegacy(ksqlConfig)) {
      return legacyKeyField;
    }

    return resolveKey(schema);
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
      return legacyKeyField.map(Field::name);
    }

    return keyField;
  }

  /**
   * Build a new instance with the supplied new key field name. Legacy remains the same.
   *
   * @param newName the new name.
   * @return the new instance.
   */
  public KeyField withName(final String newName) {
    return KeyField.of(Optional.of(newName), legacyKeyField);
  }

  /**
   * Build a new instance with the supplied legacy key field. Latest remains the same.
   *
   * @param legacy the new legacy field.
   * @return the new instance.
   */
  public KeyField withLegacy(final Optional<Field> legacy) {
    return KeyField.of(keyField, legacy);
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
        legacyKeyField.map(field -> SchemaUtil.buildAliasedField(alias, field))
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

  private Optional<Field> resolveKey(final Schema schema) {
    return keyField
        .map(fieldName -> SchemaUtil
            .getFieldByName(schema, fieldName)
            .orElseThrow(() -> new IllegalArgumentException(
                "Invalid key field, not found in schema: " + fieldName)));
  }

  private static boolean shouldUseLegacy(final KsqlConfig ksqlConfig) {
    return ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD);
  }
}
