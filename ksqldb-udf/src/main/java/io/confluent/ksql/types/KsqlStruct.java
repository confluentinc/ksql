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

package io.confluent.ksql.types;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlStruct.Field;
import io.confluent.ksql.schema.utils.DataException;
import io.confluent.ksql.testing.EffectivelyImmutable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Instance of {@link io.confluent.ksql.schema.ksql.types.SqlStruct}.
 *
 * <p>Note: this is not yet a released / used feature of ksqlDB.
 */
@Immutable
public final class KsqlStruct {

  @EffectivelyImmutable
  private final ImmutableList<Optional<?>> values;

  public static Builder builder(final SqlStruct schema) {
    return new Builder(schema);
  }

  private KsqlStruct(final List<Optional<?>> values) {
    this.values = ImmutableList.copyOf(Objects.requireNonNull(values, "values"));
  }

  /**
   * Get a field value by index - efficient
   *
   * <p>This is the most efficient way to retrieve the value. The field index can be obtained from
   * {@link SqlStruct.Field#index()}.
   *
   * @param index the index of the field within the instance and its schema.
   * @return the value.
   */
  public Optional<?> get(final int index) {
    validateIndex(index, values);
    return values.get(index);
  }

  /**
   * Get the accessor for this instance, for richer access patterns.
   *
   * @param schema the instances schema.
   * @return the accessor.
   */
  public Accessor accessor(final SqlStruct schema) {
    return new Accessor(schema);
  }

  /**
   * Convert this immutable struct to a mutable builder with all the values set.
   *
   * @param schema the instances schema.
   * @return the builder.
   */
  public Builder asBuilder(final SqlStruct schema) {
    final Builder builder = builder(schema);

    final ListIterator<Optional<?>> it = values.listIterator();
    while (it.hasNext()) {
      final int idx = it.nextIndex();
      builder.set(idx, it.next());
    }

    return builder;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlStruct that = (KsqlStruct) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public String toString() {
    return "Struct("
        + values.stream()
        .map(v -> v.orElse(null))
        .map(Objects::toString)
        .collect(Collectors.joining(","))
        + ')';
  }

  private static int fieldIndex(final String name, final SqlStruct schema) {
    final ListIterator<Field> it = schema.fields().listIterator();
    while (it.hasNext()) {
      final int idx = it.nextIndex();
      if (it.next().name().equals(name)) {
        return idx;
      }
    }

    throw new DataException("Unknown field: " + name);
  }

  private static void validateIndex(final int index, final List<?> values) {
    if (index < 0 || values.size() <= index) {
      throw new DataException("Invalid field index: " + index);
    }
  }

  public static final class Builder {

    private final SqlStruct schema;
    private final List<Optional<?>> values;

    private Builder(final SqlStruct schema) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.values = new ArrayList<>(schema.fields().size());
      schema.fields().forEach(f -> values.add(Optional.empty()));
    }

    /**
     * Set a field by index - efficient
     *
     * <p>The field index can be obtained from {@link SqlStruct.Field#index()}.
     *
     * @param fieldIndex the index of the field (it the schema) to set.
     * @param value the (nullable) value to set.
     * @return the builder.
     */
    public Builder set(final int fieldIndex, final Object value) {
      return set(fieldIndex, Optional.ofNullable(value));
    }

    /**
     * Set a field by index - efficient
     *
     * <p>The field index can be obtained from {@link SqlStruct.Field#index()}.
     *
     * @param fieldIndex the index of the field (it the schema) to set.
     * @param value the value to set.
     * @return the builder.
     */
    public Builder set(final int fieldIndex, final Optional<?> value) {
      validateIndex(fieldIndex, values);
      values.set(fieldIndex, value);
      return this;
    }

    /**
     * Set a field by name - inefficient
     *
     * <p>This is inefficient. It is much more efficient to access it by index. The critical
     * processing path should bake the indexes that need setting into the code, rather than setting
     * by name. See {@link #set(int, Object)}.
     *
     * @param fieldName the name of the field to set.
     * @param value the (nullable) value to set.
     * @return the builder.
     */
    public Builder set(final String fieldName, final Object value) {
      return set(fieldName, Optional.ofNullable(value));
    }

    /**
     * Set a field by name - inefficient
     *
     * <p>This is inefficient. It is much more efficient to access it by index. The critical
     * processing path should bake the indexes that need setting into the code, rather than setting
     * by name. See {@link #set(int, Optional)}.
     *
     * @param fieldName the name of the field to set.
     * @param value the value to set.
     * @return the builder.
     */
    public Builder set(final String fieldName, final Optional<?> value) {
      return set(fieldIndex(fieldName, schema), value);
    }

    public KsqlStruct build() {
      return new KsqlStruct(values);
    }
  }

  /**
   * Combine an instance with its schema to allow richer access patterns.
   */
  public final class Accessor {

    private final SqlStruct schema;

    private Accessor(final SqlStruct schema) {
      this.schema = Objects.requireNonNull(schema, "schema");

      final int schemaFields = schema.fields().size();
      final int values = KsqlStruct.this.values.size();
      Preconditions.checkArgument(
          schemaFields == values,
          "schema/value size mismatch: schema:" + schemaFields + ", values:" + values
      );
    }

    /**
     * Access a field value by name - inefficient
     *
     * <p>This is inefficient. It is much more efficient to access it by index. The critical
     * processing path should bake the indexes that need setting into the code, rather than setting
     * by name. See {@link KsqlStruct#get(int)}.
     *
     * @param fieldName the name of the field to access.
     * @return the value of the field.
     */
    public Optional<?> get(final String fieldName) {
      final int index = fieldIndex(fieldName, schema);
      return values.get(index);
    }

    /**
     * Visit each field and its value in the struct, in the order defined by the schema - efficient.
     *
     * @param consumer the consumer that will be called with each field and its value.
     */
    public void forEach(final BiConsumer<? super Field, ? super Optional<?>> consumer) {
      final Iterator<Field> fieldIt = schema.fields().iterator();
      final Iterator<Optional<?>> valueIt = values.iterator();

      while (fieldIt.hasNext()) {
        final Field field = fieldIt.next();
        final Optional<?> value = valueIt.next();
        consumer.accept(field, value);
      }
    }
  }
}
