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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

public class KsqlObject {

  private final JsonObject delegate;

  public KsqlObject() {
    delegate = new JsonObject();
  }

  public KsqlObject(final Map<String, Object> map) {
    delegate = new JsonObject(map);
  }

  KsqlObject(final JsonObject jsonObject) {
    delegate = Objects.requireNonNull(jsonObject);
  }

  public boolean containsKey(final String key) {
    return delegate.containsKey(key);
  }

  public Set<String> fieldNames() {
    return delegate.fieldNames();
  }

  public int size() {
    return delegate.size();
  }

  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  public Map<String, Object> getMap() {
    return delegate.getMap();
  }

  public Iterator<Entry<String,Object>> iterator() {
    return delegate.iterator();
  }

  public java.util.stream.Stream<Map.Entry<String,Object>> stream() {
    return delegate.stream();
  }

  public Object getValue(final String key) {
    return delegate.getValue(key);
  }

  public String getString(final String key) {
    return delegate.getString(key);
  }

  public Integer getInteger(final String key) {
    return delegate.getInteger(key);
  }

  public Long getLong(final String key) {
    return delegate.getLong(key);
  }

  public Double getDouble(final String key) {
    return delegate.getDouble(key);
  }

  public Boolean getBoolean(final String key) {
    return delegate.getBoolean(key);
  }

  public KsqlArray getKsqlArray(final String key) {
    return new KsqlArray(delegate.getJsonArray(key));
  }

  public KsqlObject getKsqlObject(final String key) {
    return new KsqlObject(delegate.getJsonObject(key));
  }

  public Object remove(final String key) {
    return delegate.remove(key);
  }

  public KsqlObject put(final String key, final Integer value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final Long value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final String value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final Double value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final Boolean value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final KsqlArray value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final KsqlObject value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject put(final String key, final Object value) {
    delegate.put(key, value);
    return this;
  }

  public KsqlObject putNull(final String key) {
    delegate.putNull(key);
    return this;
  }

  public KsqlObject mergeIn(final KsqlObject other) {
    delegate.mergeIn(toJsonObject(other));
    return this;
  }

  public KsqlObject copy() {
    return new KsqlObject(delegate.copy());
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

  private static JsonObject toJsonObject(final KsqlObject ksqlObject) {
    return new JsonObject(ksqlObject.getMap());
  }
}
