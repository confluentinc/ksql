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

import io.vertx.core.json.JsonObject;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * A representation of a map of string keys to values. Useful for representing a JSON object.
 */
public class KsqlObject {

  private final JsonObject delegate;

  /**
   * Creates an empty instance.
   */
  public KsqlObject() {
    delegate = new JsonObject();
  }

  /**
   * Creates an instance with the specified entries.
   *
   * @param map the entries
   */
  public KsqlObject(final Map<String, Object> map) {
    delegate = new JsonObject(map);
  }

  KsqlObject(final JsonObject jsonObject) {
    delegate = Objects.requireNonNull(jsonObject);
  }

  /**
   * Returns whether the map contains the specified key.
   *
   * @param key the key
   * @return whether the map contains the key
   */
  public boolean containsKey(final String key) {
    return delegate.containsKey(key);
  }

  /**
   * Returns the keys of the map.
   *
   * @return the keys
   */
  public Set<String> fieldNames() {
    return delegate.fieldNames();
  }

  /**
   * Returns the size (number of entries) of the map.
   *
   * @return the size
   */
  public int size() {
    return delegate.size();
  }

  /**
   * Returns whether the map is empty.
   *
   * @return whether the map is empty
   */
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  /**
   * Returns the entries of the map as a {@code Map}.
   *
   * @return the entries
   */
  public Map<String, Object> getMap() {
    return delegate.getMap();
  }

  /**
   * Returns an iterator over the entries of the map.
   *
   * @return the iterator
   */
  public Iterator<Entry<String,Object>> iterator() {
    return delegate.iterator();
  }

  /**
   * Returns entries of the map as a stream.
   *
   * @return the stream
   */
  public java.util.stream.Stream<Map.Entry<String,Object>> stream() {
    return delegate.stream();
  }

  /**
   * Returns the value associated with the specified key as an {@code Object}. Returns null if the
   * key is not present.
   *
   * @param key the key
   * @return the value
   */
  public Object getValue(final String key) {
    return delegate.getValue(key);
  }

  /**
   * Returns the value associated with the specified key as a string. Returns null if the key is not
   * present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a string
   */
  public String getString(final String key) {
    return delegate.getString(key);
  }

  /**
   * Returns the value associated with the specified key as an integer. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Integer getInteger(final String key) {
    return delegate.getInteger(key);
  }

  /**
   * Returns the value associated with the specified key as a long. Returns null if the key is not
   * present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Long getLong(final String key) {
    return delegate.getLong(key);
  }

  /**
   * Returns the value associated with the specified key as a double. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public Double getDouble(final String key) {
    return delegate.getDouble(key);
  }

  /**
   * Returns the value associated with the specified key as a boolean. Returns null if the key is
   * not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a boolean
   */
  public Boolean getBoolean(final String key) {
    return delegate.getBoolean(key);
  }

  /**
   * Returns the value associated with the specified key as a {@code BigDecimal}. Returns null if
   * the key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code Number}
   */
  public BigDecimal getDecimal(final String key) {
    return new BigDecimal(getValue(key).toString());
  }

  /**
   * Returns the value associated with the specified key as a byte array. Returns null if
   * the key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value is not a {@code String}
   * @throws IllegalArgumentException if the column value is not a base64 encoded string
   */
  public byte[] getBytes(final String key) {
    return delegate.getBinary(key);
  }

  /**
   * Returns the value associated with the specified key as a {@link KsqlArray}. Returns null if the
   * key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a list
   */
  public KsqlArray getKsqlArray(final String key) {
    return new KsqlArray(delegate.getJsonArray(key));
  }

  /**
   * Returns the value associated with the specified key as a {@code KsqlObject}. Returns null if
   * the key is not present.
   *
   * @param key the key
   * @return the value
   * @throws ClassCastException if the value cannot be converted to a map
   */
  public KsqlObject getKsqlObject(final String key) {
    return new KsqlObject(delegate.getJsonObject(key));
  }

  /**
   * Removes the value associated with a specified key.
   *
   * @param key the key
   * @return the removed value, or null if the key was not present
   */
  public Object remove(final String key) {
    return delegate.remove(key);
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Integer value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Long value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final String value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Double value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Boolean value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final BigDecimal value) {
    // Vert.x JsonObject does not accept BigDecimal values. Instead we store the value as a string
    // so as to not lose precision.
    delegate.put(key, value.toString());
    return this;
  }

  public KsqlObject put(final String key, final byte[] value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final KsqlArray value) {
    delegate.put(key, KsqlArray.toJsonArray(value));
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final KsqlObject value) {
    delegate.put(key, KsqlObject.toJsonObject(value));
    return this;
  }

  /**
   * Adds an entry for the specified key and value to the map.
   *
   * @param key the key
   * @param value the value
   * @return a reference to this
   */
  public KsqlObject put(final String key, final Object value) {
    delegate.put(key, value);
    return this;
  }

  /**
   * Adds an entry for the specified key with null value to the map.
   *
   * @param key the key
   * @return a reference to this
   */
  public KsqlObject putNull(final String key) {
    delegate.putNull(key);
    return this;
  }

  /**
   * Adds entries from the specified {@code KsqlObject} into this instance.
   *
   * @param other the entries to add
   * @return a reference to this
   */
  public KsqlObject mergeIn(final KsqlObject other) {
    delegate.mergeIn(toJsonObject(other));
    return this;
  }

  /**
   * Returns a copy of this.
   *
   * @return the copy
   */
  public KsqlObject copy() {
    return new KsqlObject(delegate.copy());
  }

  /**
   * Returns a JSON string representing the entries in the map.
   *
   * @return the JSON string
   */
  public String toJsonString() {
    return delegate.toString();
  }

  /**
   * Returns a JSON string representing the entries in the map. Same as {@link #toJsonString()}.
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
    final KsqlObject that = (KsqlObject) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(delegate);
  }

  public static KsqlObject fromArray(final List<String> keys, final KsqlArray values) {
    if (values == null || keys == null) {
      return null;
    }
    if (keys.size() != values.size()) {
      throw new IllegalArgumentException("Size of keys and values must match.");
    }

    final KsqlObject ret = new KsqlObject();
    for (int i = 0; i < keys.size(); i++) {
      ret.put(keys.get(i), values.getValue(i));
    }
    return ret;
  }

  static JsonObject toJsonObject(final KsqlObject ksqlObject) {
    return new JsonObject(ksqlObject.getMap());
  }
}
