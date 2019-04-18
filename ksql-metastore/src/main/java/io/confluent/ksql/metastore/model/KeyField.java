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
 * {@link Field}, where as the latest is just the key field name, which can be lookup up in the
 * associated schema.
 *
 * @see <a href="https://github.com/confluentinc/ksql/issues/2636">Github issue 2636</a>
 */
@Immutable
public final class KeyField {

  private final Optional<String> keyField;
  private final Optional<Field> legacyKeyField;

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
   * Validate the new key field is contained within the supplied {@code schema}.
   *
   * @param schema the associated schema that the key should be present in.
   * @return self, to allow fluid syntax.
   * @throws IllegalArgumentException if the key is not within the supplied schema.
   */
  public KeyField validateKeyExistsIn(final Schema schema) {
    if (!keyField
        .filter(name -> !name.equalsIgnoreCase(SchemaUtil.ROWKEY_NAME))
        .isPresent()) {
      return this;
    }

    resolveKey(schema);
    return this;
  }

  public Optional<String> name() {
    return keyField;
  }

  public Optional<Field> legacy() {
    return legacyKeyField;
  }

  public Optional<Field> resolve(final Schema schema, final KsqlConfig ksqlConfig) {
    if (ksqlConfig.getBoolean(KsqlConfig.KSQL_USE_LEGACY_KEY_FIELD)) {
      return legacyKeyField;
    }

    return resolveKey(schema);
  }

  public KeyField withName(final String newName) {
    return KeyField.of(Optional.of(newName), legacyKeyField);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KeyField keyField1 = (KeyField) o;
    return Objects.equals(keyField, keyField1.keyField)
        && Objects.equals(legacyKeyField, keyField1.legacyKeyField);
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
            .orElseThrow(() -> new IllegalArgumentException("Invalid key field: " + fieldName)));
  }
}
