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

package io.confluent.ksql.api.client;

import io.vertx.core.json.JsonArray;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * A representation of an array of values.
 */
public class KsqlArray {

  private final JsonArray delegate;

  /**
   * Creates an empty instance.
   */
  public KsqlArray() {
    delegate = new JsonArray();
  }

  /**
   * Creates an instance with the specified values.
   *
   * @param list the values
   */
  public KsqlArray(final List<?> list) {
    delegate = new JsonArray(list);
  }

  KsqlArray(final JsonArray jsonArray) {
    delegate = Objects.requireNonNull(jsonArray);
  }

  /**
   * Returns the size (number of values) of the array.
   *
   * @return the size
   */
  public int size() {
    return delegate.size();
  }

  /**
   * Returns whether the array is empty.
   *
   * @return whether the array is empty
   */
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  /**
   * Returns values of the array as a list.
   *
   * @return list of values
   */
  public List<?> getList() {
    return delegate.getList();
  }

  /**
   * Returns an iterator over values of the array.
   *
   * @return the iterator
   */
  public Iterator<Object> iterator() {
    return delegate.iterator();
  }

  /**
   * Returns values of the array as a stream.
   *
   * @return the stream
   */
  public java.util.stream.Stream<Object> stream() {
    return delegate.stream();
  }

  /**
   * Returns the value at a specified index as an {@code Object}.
   *
   * @param pos the index
   * @return the value
   */
  public Object getValue(final int pos) {
    return delegate.getValue(pos);
  }

  /**
   * Returns the value at a specified index as a string.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a string
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public String getString(final int pos) {
    return delegate.getString(pos);
  }

  /**
   * Returns the value at a specified index as an integer.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Integer getInteger(final int pos) {
    return delegate.getInteger(pos);
  }

  /**
   * Returns the value at a specified index as a long.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Long getLong(final int pos) {
    return delegate.getLong(pos);
  }

  /**
   * Returns the value at a specified index as a double.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Double getDouble(final int pos) {
    return delegate.getDouble(pos);
  }

  /**
   * Returns the value at a specified index as a boolean.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a boolean
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Boolean getBoolean(final int pos) {
    return delegate.getBoolean(pos);
  }

  /**
   * Returns the value at a specified index as a {@code BigDecimal}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public BigDecimal getDecimal(final int pos) {
    return new BigDecimal(getValue(pos).toString());
  }

  /**
   * Returns the value at a specified index as a byte array.
   *
   * @param pos the index
   * @return the value
   * @throws IndexOutOfBoundsException if the index is invalid
   * @throws IllegalArgumentException if the array value is not a Base64 encoded string
   */
  public byte[] getBytes(final int pos) {
    return delegate.getBinary(pos);
  }

  /**
   * Returns the value at a specified index as a {@code KsqlArray}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a list
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public KsqlArray getKsqlArray(final int pos) {
    return new KsqlArray(delegate.getJsonArray(pos));
  }

  /**
   * Returns the value at a specified index as a {@link KsqlObject}.
   *
   * @param pos the index
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a map
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public KsqlObject getKsqlObject(final int pos) {
    return new KsqlObject(delegate.getJsonObject(pos));
  }

  /**
   * Removes the value at a specified index from the array.
   *
   * @param pos the index
   * @return the removed value
   * @throws IndexOutOfBoundsException if the index is invalid
   */
  public Object remove(final int pos) {
    return delegate.remove(pos);
  }

  /**
   * Removes the first occurrence of the specified value from the array, if present.
   *
   * @param value the value to remove
   * @return whether the value was removed
   */
  public boolean remove(final Object value) {
    return delegate.remove(value);
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final String value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Integer value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Long value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Double value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Boolean value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final BigDecimal value) {
    // Vert.x JsonArray does not accept BigDecimal values. Instead we store the value as a string
    // so as to not lose precision.
    delegate.add(value.toString());
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final byte[] value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final KsqlArray value) {
    delegate.add(KsqlArray.toJsonArray(value));
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final KsqlObject value) {
    delegate.add(KsqlObject.toJsonObject(value));
    return this;
  }

  /**
   * Appends the specified value to the end of the array.
   *
   * @param value the value to append
   * @return a reference to this
   */
  public KsqlArray add(final Object value) {
    delegate.add(value);
    return this;
  }

  /**
   * Appends a null value to the end of the array.
   *
   * @return a reference to this
   */
  public KsqlArray addNull() {
    delegate.addNull();
    return this;
  }

  /**
   * Appends the values in the specified {@code KsqlArray} to the end of this instance.
   *
   * @param array the values to append
   * @return a reference to this
   */
  public KsqlArray addAll(final KsqlArray array) {
    delegate.addAll(toJsonArray(array));
    return this;
  }

  /**
   * Returns a copy of this.
   *
   * @return the copy
   */
  public KsqlArray copy() {
    return new KsqlArray(delegate.copy());
  }

  /**
   * Returns a JSON string representing the values in the array.
   *
   * @return the JSON string
   */
  public String toJsonString() {
    return delegate.toString();
  }

  /**
   * Returns a JSON string representing the values in the array. Same as {@link #toJsonString()}.
   *
   * @return the JSON string
   */
  @Override
  public String toString() {
    return toJsonString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlArray that = (KsqlArray) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  static JsonArray toJsonArray(final KsqlArray ksqlArray) {
    return new JsonArray(ksqlArray.getList());
  }
}
